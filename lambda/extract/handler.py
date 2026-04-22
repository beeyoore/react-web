import json
import logging
import os
import re
import unicodedata
import urllib.parse
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
bedrock = boto3.client(
    "bedrock-runtime",
    config=Config(
        read_timeout=int(os.environ.get("BEDROCK_READ_TIMEOUT", "240")),
        connect_timeout=int(os.environ.get("BEDROCK_CONNECT_TIMEOUT", "10")),
        retries={"max_attempts": int(os.environ.get("BEDROCK_MAX_ATTEMPTS", "3"))},
    ),
)
dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "eu-central-1"))
lambda_client = boto3.client("lambda", region_name=os.environ.get("AWS_REGION", "eu-central-1"))

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_SCHEMA_CACHE_BY_PATH: dict[str, dict] = {}

DOCUMENT_TYPE_DECRETO_RICOSTRUZIONE = "Decreto di ricostruzione della carriera"
CATEGORIA_PERSONALE_VALUES = {"docente", "ATA"}

ALTERNATIVE_SERVICE_DOCUMENT_TYPES = {
    "certificato_servizio",
    "dichiarazione_servizi",
    "autocertificazione_servizi",
}

# Tipi di documento che, se appena estratti, possono sbloccare l'ingestion servizi.
# Ogni volta che uno di questi viene scritto, vale la pena rivalutare le precondizioni.
SERVIZI_TRIGGER_DOCUMENT_TYPES = ALTERNATIVE_SERVICE_DOCUMENT_TYPES | {"decreto_ricostruzione"}

COMMON_REQUIRED_DOCUMENT_TYPES = {
    "decreto_ricostruzione",
    "istanza_ricostruzione",
    "contratto_tempo_indeterminato",
    "titolo_studio",
}

DOCENTE_REQUIRED_DOCUMENT_TYPES = COMMON_REQUIRED_DOCUMENT_TYPES | {
    "decreto_superamento_prova",
}

ATA_REQUIRED_DOCUMENT_TYPES = COMMON_REQUIRED_DOCUMENT_TYPES | {
    "presa_atto_conferma_ruolo",
}

# Controlli preliminari: ogni voce diventa un item PRELIMINARE#{id} in DynamoDB.
# categorie=None  → valido per tutte le categorie di personale
# categorie=set   → valido solo per le categorie indicate
# tipo_documento=None → controllo speciale gestito separatamente (documento servizi)
PRELIMINARY_CONTROLS = [
    {
        "id": "verifica_decreto_ricostruzione",
        "nome": "Verifica presenza decreto ricostruzione carriera",
        "tipo_documento": "decreto_ricostruzione",
        "categorie": None,
    },
    {
        "id": "verifica_istanza_ricostruzione",
        "nome": "Verifica presenza istanza ricostruzione di carriera",
        "tipo_documento": "istanza_ricostruzione",
        "categorie": None,
    },
    {
        "id": "verifica_contratto_tempo_indeterminato",
        "nome": "Verifica presenza contratto a tempo indeterminato",
        "tipo_documento": "contratto_tempo_indeterminato",
        "categorie": None,
    },
    {
        "id": "verifica_titolo_studio",
        "nome": "Verifica presenza titolo di studio",
        "tipo_documento": "titolo_studio",
        "categorie": None,
    },
    {
        "id": "verifica_decreto_superamento_prova",
        "nome": "Verifica presenza decreto superamento prova",
        "tipo_documento": "decreto_superamento_prova",
        "categorie": {"docente"},
    },
    {
        "id": "verifica_presa_atto_conferma_ruolo",
        "nome": "Verifica presenza presa atto conferma ruolo",
        "tipo_documento": "presa_atto_conferma_ruolo",
        "categorie": {"ATA"},
    },
    {
        "id": "verifica_documento_servizi",
        "nome": "Verifica presenza documento servizi",
        "tipo_documento": None,
        "categorie": None,
    },
]

ATA_QUALIFICA_HINTS = {
    "ata",
    "assistente amministrativo",
    "assistente tecnico",
    "collaboratore scolastico",
    "collaboratore dei servizi",
    "dsga",
    "direttore dei servizi generali e amministrativi",
    "guardarobiere",
    "cuoco",
    "infermiere",
    "addetto azienda agraria",
}

DOCENTE_QUALIFICA_HINTS = {
    "docente",
    "docenti",
    "personale docente",
    "insegnante",
    "insegnanti",
    "professore",
    "professori",
    "professoressa",
    "professoresse",
    "maestro",
    "maestri",
    "maestra",
    "maestre",
    "educatore",
    "educatori",
    "educatrice",
    "educatrici",
}

DOCENTE_QUALIFICA_STEMS = {
    "docent",
    "insegnant",
    "professor",
    "maestr",
    "educator",
}

SERVICE_MERGE_LOGIC_VERSION = "service-merge-date-first-v2"

ABSENCE_ROW_HINTS = {
    "assenz",
    "permess",
    "conged",
    "malatti",
    "covid",
    "parental",
    "matern",
    "patern",
    "aspettat",
    "infort",
}

SERVICE_ROW_HINTS = {
    "giorni effettivi",
    "orario settimanale",
    "in qualita",
    "tipo posto",
    "tipo nomina",
    "autorita nomina",
    "situazione prev",
    "qual retributiva",
    "periodo retribuito",
    "tipo servizio",
    "tipo attivita",
    "partec scrutini",
    "scrutini e esami",
    "sede servizio",
    "classe di concorso",
}

CATEGORY_CLASSIFICATION_SYSTEM_PROMPT = """
Sei un classificatore prudente della categoria del personale in un decreto di ricostruzione carriera.

Ricevi il clean JSON del decreto gia classificato come "Decreto di ricostruzione della carriera".

Devi stabilire solo se la pratica riguarda:
- docente
- ATA

Regole:
- usa come fonte principale qualifica, profilo professionale, ruolo, area, diciture del personale e dati professionali presenti nel decreto
- classifica ATA quando il decreto parla di personale ATA, assistente amministrativo, assistente tecnico, collaboratore scolastico, DSGA, direttore SGA, cuoco, guardarobiere, infermiere o profili equivalenti ATA
- classifica docente quando il decreto parla di personale docente, insegnante, professore, maestro, educatore o servizio di insegnamento
- non estrarre altri campi del decreto
- se non e sufficientemente chiaro, restituisci categoria_personale null e confidence LOW
- restituisci SOLO JSON puro con questa struttura:
{
  "categoria_personale": "docente|ATA|null",
  "confidence": "HIGH|MEDIUM|LOW",
  "evidence": "breve motivazione con la dicitura letta"
}
""".strip()


EXTRACTION_SYSTEM_PROMPT = """
Sei un estrattore prudente di entita da documenti amministrativi scolastici italiani.

Ricevi:
- il tipo documento classificato
- la sezione di schema corretta per quel documento
- il clean del documento

Devi restituire SOLO un JSON che rispetti esattamente la struttura del campo `campi` della sezione di schema fornita.

Regole obbligatorie:
- estrai solo i campi previsti dalla sezione di schema ricevuta
- non inventare chiavi aggiuntive
- usa descrizioni, alias_campo_checklist, default_se_assente, istruzioni_estrazione ed eventuali regole_estrazione o esempi_estrazione presenti nello schema per capire come valorizzare i campi
- se un campo non e presente in modo sufficientemente affidabile, usa null, salvo che lo schema definisca un default esplicito
- per nomi propri, cognomi, luoghi e altri dati anagrafici testuali, copia il testo in modo letterale dalla fonte piu esplicita; non correggere, non abbreviare e non cambiare nemmeno una lettera
- se il documento contiene una riga anagrafica esplicita del tipo `COGNOME NOME nato/a a ... il ...`, usa quella come fonte principale per `nome_cognome`, `luogo_nascita` e `data_nascita`
- se `formsByPage`, `pagesText` e `text` mostrano lo stesso nominativo, mantieni esattamente quella grafia; una consonante doppia mancante o aggiunta e un errore
- per i campi ripetuti restituisci un array di oggetti coerente con `item_struttura`
- per i campi con `per_servizio: true`, mantieni l'allineamento logico richiesto dallo schema; se le istruzioni del campo descrivono piu periodi distinti, restituisci un elemento per ciascun periodo riconoscibile nello stesso ordine di comparsa
- per le assenze annidate dentro un servizio, associa ogni assenza al servizio corretto quando condivide la stessa riga, sezione, blocco o un periodo compatibile; lascia `assenze` vuoto solo se l'assenza non e agganciabile in modo affidabile a nessun servizio specifico
- per i boolean usa solo true o false
- per le date rispetta il formato richiesto dal campo nello schema
- se il campo richiede `Date (DD/MM/YYYY)`, mantieni l'ordine italiano giorno/mese/anno
- non reinterpretare una data numerica in un formato diverso da quello richiesto dallo schema
- se le istruzioni di schema dichiarano che due campi data sono equivalenti, quando uno dei due e certo e l'altro manca, valorizza entrambi con lo stesso valore
- se uno schema nidificato rappresenta durate o anzianita in `anni`, `mesi`, `giorni`, estrai separatamente i tre numeri per ciascuna riga e colonna richiesta senza copiare testo libero nel posto dei numeri
- quando il documento contiene una tabella o matrice con etichette di riga e intestazioni di colonna, usa come regola primaria l'intersezione esatta riga/colonna; non copiare mai i valori di una riga o colonna dentro un'altra
- se una cella tabellare contiene esplicitamente `anni 0 mesi 0 giorni 0` o valori equivalenti, restituisci i numeri `0`, `0`, `0`; zero esplicito non significa campo assente
- per tabelle presenti in `tablesByPage`, usa `tablesByPage` come fonte primaria per capire la geometria di righe e colonne; usa `formsByPage`, `pagesText` e `text` solo come supporto o verifica
- restituisci solo JSON puro, senza testo aggiuntivo
""".strip()


SERVICE_SPECIALIZED_EXTRACTION_SYSTEM_PROMPT = """
Sei un estrattore specializzato di servizi scolastici italiani.

Ricevi:
- lo schema anagrafico del documento servizi
- lo schema dei campi del singolo servizio
- lo schema dei campi della singola assenza
- il clean del documento

Devi restituire SOLO un JSON con questa struttura:
{
  "anagrafica": { ... },
  "servizi": [
    {
      "indice": 0,
      "<campi_servizio>": "..."
    }
  ],
  "assenze": [
    {
      "indice": 0,
      "<campi_assenza>": "..."
    }
  ]
}

Regole obbligatorie:
- Estrai un elemento in `servizi` per ogni riga o blocco di servizio. Mai aggregare servizi diversi.
- Estrai un elemento in `assenze` per ogni riga di assenza o permesso presente nel documento.
- In questo step NON devi ancora assegnare le assenze ai servizi.
- Usa esattamente i nomi campo forniti negli schemi ricevuti.
- Per l'anagrafica, usa come fonte prioritaria la riga nominativa esplicita del certificato, tipicamente vicina a formule come `si certifica che`, `nato/a a`, `nata a`, `il`.
- `nome_cognome` deve essere copiato in modo letterale dalla fonte anagrafica piu esplicita: non normalizzare, non correggere, non riassumere e non perdere lettere.
- Presta attenzione ai cognomi con consonanti doppie: una grafia come `FERRETTI` non puo diventare `FERRETI`.
- Se il nominativo compare anche in `formsByPage`, `pagesText` o `text`, usa la grafia coerente con la maggioranza delle occorrenze esplicite; se una sola fonte isola diverge ma le altre concordano, segui la grafia concordante.
- Non usare intestazioni di istituto, firme, sigle o nomi di sedi per ricostruire l'anagrafica della persona.
- Per i certificati di servizio, mappa normalmente:
  - `Dec. Giuridica` -> `data_inizio`
  - `Dec. Econom.` -> `periodo_retribuito_inizio`
  - `Fine servizio` -> `data_fine` e, se il documento non espone un diverso termine retribuito, anche `periodo_retribuito_fine`
  - `Data inizio` / `Data fine` / `Giorni` / `Motivo assenza` -> campi assenza corrispondenti
- Se un servizio riporta esplicitamente `Classe di concorso`, copia il valore nel campo `classe_concorso`; se non e presente in modo affidabile usa null.
- Nei certificati di servizio, l'elenco dei servizi puo comparire in piu blocchi separati: servizi iniziali, poi assenze/permessi, poi altri servizi nelle pagine successive. Non assumere mai che la lista dei servizi termini prima della sezione assenze.
- Se dopo una sezione `Assenze` o `Permessi` ricompaiono pattern da servizio come due date di periodo, anno scolastico, `Periodo retribuito`, `Tipo servizio`, `Partec. scrutini e esami`, devi estrarre anche quei servizi successivi.
- Non fondere mai due servizi distinti solo perche hanno stessa qualifica, stessa scuola o stesso anno scolastico. Ogni coppia distinta di periodo di servizio (`data_inizio`/`data_fine` oppure `periodo_retribuito_inizio`/`periodo_retribuito_fine`) corrisponde a un servizio separato.
- Se a fondo pagina compare l'inizio di un nuovo servizio e i dettagli continuano nella pagina successiva, devi comunque riconoscere quel nuovo servizio come elemento distinto.
- Per l'estrazione dei servizi usa come fonte primaria `tablesByPage` quando contiene righe o blocchi di servizio; usa `formsByPage` e `pagesText` come supporto per completare o verificare i dettagli mancanti.
- In `tablesByPage`, considera come servizio ogni riga o blocco che mostra un periodo distinto con due date e dettagli coerenti come `In qualita`, `Tipo servizio`, `Periodo retribuito`, `Partec. scrutini e esami`, `Sede servizio`, `Tipo nomina`, `Classe di concorso`.
- Se una riga tabellare mostra chiaramente un nuovo periodo di servizio con dettagli di servizio nello stesso blocco, devi creare un nuovo elemento in `servizi` anche se subito dopo compare un altro servizio nella stessa pagina o nella pagina successiva.
- Se una tabella contiene servizi, poi una sezione assenze, poi di nuovo servizi, devi riprendere l'estrazione dei servizi dal blocco tabellare successivo; la sezione assenze non chiude definitivamente la lista dei servizi.
- Se due righe tabellari consecutive hanno periodi diversi, anche se sono nello stesso anno o nella stessa scuola, sono due servizi distinti.
- Usa `tablesByPage`, `formsByPage` e `pagesText` anche per contare i blocchi di servizio: se nel documento riconosci N periodi di servizio distinti, l'array `servizi` deve contenere N elementi nello stesso ordine di apparizione.
- Prima di restituire il JSON, fai un controllo finale di completezza dei servizi: verifica di non esserti fermato all'ultimo servizio prima delle assenze, di non aver saltato servizi successivi presenti dopo la sezione assenze e di avere un elemento `servizi` per ogni periodo distinto leggibile in `tablesByPage`.
- Per i campi data dei servizi e delle assenze, leggi sempre le etichette di colonna o di form prima di valorizzare i campi:
  - `data_inizio` e `periodo_fruizione_inizio` corrispondono sempre all'inizio del periodo
  - `data_fine` e `periodo_fruizione_fine` corrispondono sempre alla fine del periodo
  - non invertire mai inizio e fine
- Se in una riga di assenza o di servizio trovi due date, usa la prima solo se il documento la presenta davvero come inizio e la seconda solo se il documento la presenta davvero come fine.
- Se la colonna o il layout sono chiari (`Data inizio`, `Dal`, `Dec. Giuridica`, `Inizio`) non devi reinterpretare l'ordine in modo creativo.
- Se l'output che stai per produrre ha una data di inizio successiva alla data di fine, considera il dato sospetto, rileggi la riga, le intestazioni e le celle vicine, e correggi il mapping prima di rispondere.
- Prima di restituire il JSON, fai un controllo finale di coerenza:
  - per ogni servizio con inizio e fine entrambe presenti, `data_inizio` non puo essere successiva a `data_fine`
  - per ogni assenza con inizio e fine entrambe presenti, `periodo_fruizione_inizio` non puo essere successiva a `periodo_fruizione_fine`
- Se non riesci a determinare in modo affidabile quale delle due date sia l'inizio e quale la fine, usa null invece di invertirle o indovinarle.
- Se il documento contiene una tabella servizi separata da una tabella assenze, estrai entrambe le liste in modo indipendente e fedele.
- Le tabelle di assenze possono continuare nella pagina successiva senza ripetere il titolo o l'intestazione: considera le righe della pagina successiva come parte della stessa lista finche il formato delle colonne resta coerente.
- Non fermarti al cambio pagina: se una tabella di assenze prosegue nella pagina seguente, devi estrarre anche tutte le righe successive.
- Non associare mai un'assenza al servizio piu vicino: l'assegnazione verra fatta dopo in modo deterministico.
- Quando lo schema prevede una durata in giorni (`durata_assenza_giorni` o equivalente), usa un intero solo se rappresenta davvero un numero di giorni.
- Se il documento espone un valore esplicito in giorni, usa quel numero.
- Se il documento non espone i giorni ma il periodo inizio/fine e certo, calcola i giorni di calendario del periodo includendo sia il giorno iniziale sia il giorno finale.
- Se la durata e scritta in mesi o anni (per esempio `11 mesi e 11 giorni`), non copiare mai il primo numero nel campo durata in giorni; usa i giorni esatti solo se ricavabili in modo affidabile dal periodo, altrimenti usa null.
- Mantieni `tipologia_assenza` pulita e fedele alla causale/codice dell'assenza; non aggiungere frammenti di durata se non fanno parte della causale stessa.
- Rispetta il formato data richiesto dallo schema dei campi.
- Se un valore non e leggibile in modo affidabile, usa null.
- Restituisci solo JSON puro, senza markdown e senza testo aggiuntivo.
""".strip()


def _service_debug_enabled() -> bool:
    return os.environ.get("SERVICE_EXTRACTION_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def _truncate_debug_string(value: str, max_chars: int = 20000) -> str:
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + f" ...[truncated {len(value) - max_chars} chars]"


def _log_service_debug_snapshot(stage: str, **payload) -> None:
    if not _service_debug_enabled():
        return

    try:
        serialized = json.dumps(payload, ensure_ascii=False, default=str)
    except TypeError:
        serialized = json.dumps({"payload_repr": repr(payload)}, ensure_ascii=False)

    logger.info(
        "service_debug stage=%s payload=%s",
        stage,
        _truncate_debug_string(serialized),
    )


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def _schema_candidates(schema_filename: str = "rts_schema.json") -> list[Path]:
    env_var = "RTS_SCHEMA_ATA_PATH" if schema_filename == "rts_schema_ata.json" else "RTS_SCHEMA_PATH"
    env_path = os.environ.get(env_var)
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))

    base_dir = Path(__file__).parent
    candidates.extend(
        [
            base_dir / "docs" / "query_tool" / schema_filename,
            base_dir / schema_filename,
        ]
    )
    return candidates


def load_schema(schema_filename: str = "rts_schema.json") -> dict:
    for path in _schema_candidates(schema_filename):
        cache_key = str(path.resolve()) if path.exists() else str(path)
        cached_schema = _SCHEMA_CACHE_BY_PATH.get(cache_key)
        if cached_schema is not None:
            return cached_schema

        if path.exists():
            with open(path, encoding="utf-8") as f:
                schema = json.load(f)
                _SCHEMA_CACHE_BY_PATH[cache_key] = schema
                logger.info("entity_schema_loaded path=%s", path)
                return schema

    raise FileNotFoundError(f"Impossibile trovare {schema_filename}")


def schema_filename_for_categoria(categoria_personale: str | None) -> str:
    return "rts_schema_ata.json" if categoria_personale == "ATA" else "rts_schema.json"


def load_schema_for_category(categoria_personale: str) -> dict:
    return load_schema(schema_filename_for_categoria(categoria_personale))


# ---------------------------------------------------------------------------
# S3 / document helpers
# ---------------------------------------------------------------------------

def parse_classified_key(key: str, classified_prefix: str) -> dict | None:
    parts = key.split("/")

    if len(parts) == 2 and parts[0] == classified_prefix and parts[1].lower().endswith(".classification.json"):
        filename = parts[1]
        base_name = filename[: -len(".classification.json")]
        return {"filename": filename, "base_name": base_name, "id_pratica": None}

    if (
        len(parts) == 5
        and parts[0] == "pratiche"
        and parts[2] == "output"
        and parts[3] == classified_prefix
        and parts[4].lower().endswith(".classification.json")
    ):
        filename = parts[4]
        base_name = filename[: -len(".classification.json")]
        return {"filename": filename, "base_name": base_name, "id_pratica": parts[1]}

    if (
        len(parts) == 4
        and parts[0] == "output"
        and parts[1] == classified_prefix
        and parts[3].lower().endswith(".classification.json")
    ):
        filename = parts[3]
        base_name = filename[: -len(".classification.json")]
        return {"filename": filename, "base_name": base_name, "id_pratica": parts[2]}

    if (
        len(parts) == 5
        and parts[0] == "output"
        and parts[1] == classified_prefix
        and parts[4].lower().endswith(".classification.json")
    ):
        filename = parts[4]
        base_name = filename[: -len(".classification.json")]
        return {
            "filename": filename,
            "base_name": base_name,
            "id_pratica": parts[3],
            "tipo_flusso": parts[2],
        }

    return None


def load_json_object(bucket: str, key: str) -> dict | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return json.loads(body)


def _classified_prefixes_for_pratica(
    classified_prefix: str,
    id_pratica: str,
    current_key: str | None = None,
) -> list[str]:
    prefixes = [
        f"output/{classified_prefix}/{id_pratica}/",
        f"pratiche/{id_pratica}/output/{classified_prefix}/",
    ]

    if current_key and "/" in current_key:
        prefixes.insert(0, current_key.rsplit("/", 1)[0] + "/")

    deduplicated_prefixes = []
    seen = set()
    for prefix in prefixes:
        if prefix in seen:
            continue
        seen.add(prefix)
        deduplicated_prefixes.append(prefix)
    return deduplicated_prefixes


def list_classified_keys_for_pratica(
    bucket: str,
    classified_prefix: str,
    id_pratica: str,
    current_key: str | None = None,
) -> list[str]:
    keys: list[str] = []
    seen = set()
    paginator = s3.get_paginator("list_objects_v2")

    for prefix in _classified_prefixes_for_pratica(classified_prefix, id_pratica, current_key):
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if not key or not key.lower().endswith(".classification.json") or key in seen:
                    continue
                seen.add(key)
                keys.append(key)

    return keys


def trigger_extraction_for_classified_keys(
    *,
    function_name: str | None,
    bucket: str,
    classified_keys: list[str],
    current_key: str,
) -> int:
    if not function_name:
        logger.info("pending_classified_extraction_trigger_skipped reason=function_name_missing")
        return 0

    triggered = 0
    for key in classified_keys:
        if key == current_key:
            continue

        payload = {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": urllib.parse.quote_plus(key)},
                    },
                }
            ]
        }

        try:
            lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="Event",
                Payload=json.dumps(payload).encode(),
            )
            triggered += 1
        except ClientError as exc:
            logger.error(
                "pending_classified_extraction_trigger_failed key=%s function_name=%s error=%s",
                key,
                function_name,
                exc,
            )

    logger.info(
        "pending_classified_extraction_trigger_done bucket=%s current_key=%s candidates=%s triggered=%s",
        bucket,
        current_key,
        len(classified_keys),
        triggered,
    )
    return triggered


# ---------------------------------------------------------------------------
# Bedrock helpers
# ---------------------------------------------------------------------------

def get_text_block(response_body: dict) -> str:
    content = response_body.get("output", {}).get("message", {}).get("content", [])
    texts = [block["text"] for block in content if "text" in block]
    return "".join(texts).strip()


def _strip_code_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped

    lines = stripped.splitlines()
    if lines:
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _extract_json_candidate(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text


def _remove_trailing_commas(text: str) -> str:
    return re.sub(r",(?=\s*[}\]])", "", text)


def _insert_missing_commas(text: str) -> str:
    repaired = text
    repaired = re.sub(r'([}\]])(\s*)(?=[{\"])', r'\1,\2', repaired)
    repaired = re.sub(
        r'("(?:[^"\\]|\\.)*"|\btrue\b|\bfalse\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)\s+(?="[^"]+"\s*:)',
        r'\1, ',
        repaired,
    )
    return repaired


def _close_unbalanced_json(text: str) -> str:
    stack = []
    in_string = False
    escaped = False

    for char in text:
        if in_string:
            if escaped:
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == '{':
            stack.append('}')
        elif char == '[':
            stack.append(']')
        elif char in {'}', ']'} and stack and char == stack[-1]:
            stack.pop()

    repaired = text
    if in_string:
        repaired += '"'
    if stack:
        repaired += "".join(reversed(stack))
    return repaired


def _repair_json_candidate(text: str) -> str:
    repaired = _remove_trailing_commas(text)
    repaired = _insert_missing_commas(repaired)
    repaired = _close_unbalanced_json(repaired)
    repaired = _remove_trailing_commas(repaired)
    return repaired


def _bedrock_meta(response_body: dict) -> dict:
    usage = response_body.get("usage") or {}
    return {
        "stopReason": response_body.get("stopReason"),
        "inputTokens": usage.get("inputTokens"),
        "outputTokens": usage.get("outputTokens"),
    }


def _log_bedrock_response_meta(context: str, response_body: dict, raw_text: str) -> None:
    meta = _bedrock_meta(response_body)
    logger.info(
        "%s stop_reason=%s input_tokens=%s output_tokens=%s raw_chars=%s",
        context,
        meta.get("stopReason"),
        meta.get("inputTokens"),
        meta.get("outputTokens"),
        len(raw_text),
    )


def parse_model_json(raw_text: str) -> dict:
    text = _extract_json_candidate(_strip_code_fences(raw_text))

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        repaired = _repair_json_candidate(text)
        if repaired != text:
            try:
                parsed = json.loads(repaired)
                logger.warning(
                    "entity_model_json_repaired error=%s original_tail=%s repaired_tail=%s",
                    exc,
                    text[max(0, len(text) - 400):],
                    repaired[max(0, len(repaired) - 400):],
                )
                return parsed
            except json.JSONDecodeError:
                pass
        raise


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def get_table():
    return dynamodb.Table(os.environ.get("DYNAMODB_TABLE", "RTS_Pratiche"))


def get_controlli_table():
    return dynamodb.Table(os.environ.get("CONTROLLI_TABLE", "Controlli"))


def load_pratica_metadata(table, id_pratica: str) -> dict:
    response = table.query(
        KeyConditionExpression=Key("PK").eq(f"PRATICA#{id_pratica}") & Key("SK").eq("METADATA")
    )
    items = response.get("Items", [])
    return items[0] if items else {}


def query_document_items(table, id_pratica: str) -> list[dict]:
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(f"PRATICA#{id_pratica}") & Key("SK").begins_with("DOCUMENTO#")
    }
    items = []

    while True:
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key

    return items


def find_existing_document_item(table, id_pratica: str, source_classified_key: str) -> dict | None:
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(f"PRATICA#{id_pratica}") & Key("SK").begins_with("DOCUMENTO#")
    }

    while True:
        response = table.query(**kwargs)
        for item in response.get("Items", []):
            if item.get("sourceClassifiedKey") == source_classified_key:
                return item
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return None
        kwargs["ExclusiveStartKey"] = last_key


def ensure_metadata_item(table, id_pratica: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    table.update_item(
        Key={"PK": f"PRATICA#{id_pratica}", "SK": "METADATA"},
        UpdateExpression=(
            "SET id_pratica = :id_pratica, "
            "stato_pratica = if_not_exists(stato_pratica, :stato_pratica), "
            "data_apertura = if_not_exists(data_apertura, :data_apertura), "
            "fonte_documento_servizi = if_not_exists(fonte_documento_servizi, :fonte_documento_servizi), "
            "flag_servizio_militare = if_not_exists(flag_servizio_militare, :flag_servizio_militare), "
            "flag_mancato_superamento_prova = if_not_exists(flag_mancato_superamento_prova, :flag_mancato_superamento_prova), "
            "flag_risposta_osservazione = if_not_exists(flag_risposta_osservazione, :flag_risposta_osservazione), "
            "flag_completezza = if_not_exists(flag_completezza, :flag_completezza)"
        ),
        ExpressionAttributeValues={
            ":id_pratica": id_pratica,
            ":stato_pratica": os.environ.get("DEFAULT_STATO_PRATICA", "in_lavorazione"),
            ":data_apertura": now_iso,
            ":fonte_documento_servizi": os.environ.get("DEFAULT_FONTE_DOCUMENTO_SERVIZI", "certificato_servizio"),
            ":flag_servizio_militare": False,
            ":flag_mancato_superamento_prova": False,
            ":flag_risposta_osservazione": False,
            ":flag_completezza": False,
        },
    )


def evaluate_preliminary_controls(
    present_document_types: set[str],
    categoria_personale: str | None,
) -> list[dict]:
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    results = []
    for ctrl in PRELIMINARY_CONTROLS:
        categorie = ctrl["categorie"]
        # Skip controls for other categories when category is known
        if categorie is not None and categoria_personale and categoria_personale not in categorie:
            continue
        # Special case: documento servizi is any of ALTERNATIVE_SERVICE_DOCUMENT_TYPES
        if ctrl["tipo_documento"] is None:
            presente = bool(present_document_types & ALTERNATIVE_SERVICE_DOCUMENT_TYPES)
        else:
            presente = ctrl["tipo_documento"] in present_document_types
        # Category-specific controls stay pending until category is known
        if categorie is not None and not categoria_personale:
            esito = "in_attesa_categoria"
        else:
            esito = "superato" if presente else "non_superato"
        results.append({
            "id": ctrl["id"],
            "nome": ctrl["nome"],
            "esito": esito,
            "aggiornato_at": now_iso,
        })
    return results


def write_preliminary_controls(id_pratica: str, controls: list[dict]) -> None:
    controlli_table = get_controlli_table()
    for ctrl in controls:
        if ctrl["esito"] == "superato":
            # Documento confermato presente: scrivi sempre superato
            controlli_table.update_item(
                Key={"PK": f"PRATICA#{id_pratica}", "SK": f"PRELIMINARE#{ctrl['id']}"},
                UpdateExpression=(
                    "SET nome = :nome, "
                    "esito = :esito, "
                    "convalidato = if_not_exists(convalidato, :convalidato), "
                    "aggiornato_at = :aggiornato_at"
                ),
                ExpressionAttributeValues={
                    ":nome": ctrl["nome"],
                    ":esito": ctrl["esito"],
                    ":convalidato": False,
                    ":aggiornato_at": ctrl["aggiornato_at"],
                },
            )
        else:
            # Documento non ancora visto: inizializza a non_avviato se non esiste.
            # Non sovrascrivere mai un superato già presente.
            controlli_table.update_item(
                Key={"PK": f"PRATICA#{id_pratica}", "SK": f"PRELIMINARE#{ctrl['id']}"},
                UpdateExpression=(
                    "SET nome = :nome, "
                    "esito = if_not_exists(esito, :non_avviato), "
                    "convalidato = if_not_exists(convalidato, :convalidato), "
                    "aggiornato_at = if_not_exists(aggiornato_at, :aggiornato_at)"
                ),
                ExpressionAttributeValues={
                    ":nome": ctrl["nome"],
                    ":non_avviato": "non_avviato",
                    ":convalidato": False,
                    ":aggiornato_at": ctrl["aggiornato_at"],
                },
            )
        logger.info(
            "preliminary_control_written id_pratica=%s control=%s esito=%s",
            id_pratica, ctrl["id"], ctrl["esito"],
        )


def _finalize_pending_controls(id_pratica: str) -> None:
    """
    Transizione non_avviato → non_superato per tutti i controlli preliminari
    che non sono stati risolti, chiamata solo quando tutti i documenti attesi
    sono stati processati.
    """
    controlli_table = get_controlli_table()
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Leggi i controlli esistenti per questa pratica
    resp = controlli_table.query(
        KeyConditionExpression=Key("PK").eq(f"PRATICA#{id_pratica}"),
    )
    existing = {item["SK"]: item for item in resp.get("Items", [])}

    for ctrl in PRELIMINARY_CONTROLS:
        sk = f"PRELIMINARE#{ctrl['id']}"
        item = existing.get(sk)
        if item and item.get("esito") == "non_avviato":
            controlli_table.update_item(
                Key={"PK": f"PRATICA#{id_pratica}", "SK": sk},
                UpdateExpression="SET esito = :esito, aggiornato_at = :aggiornato_at",
                ConditionExpression="esito = :non_avviato",
                ExpressionAttributeValues={
                    ":esito": "non_superato",
                    ":non_avviato": "non_avviato",
                    ":aggiornato_at": now_iso,
                },
            )
            logger.info(
                "preliminary_control_finalized id_pratica=%s control=%s non_avviato→non_superato",
                id_pratica, ctrl["id"],
            )


# ---------------------------------------------------------------------------
# Categoria personale inference
# ---------------------------------------------------------------------------

def normalize_text_for_match(value: str | None) -> str:
    if not isinstance(value, str):
        return ""

    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.replace(chr(8217), "'").replace("`", "'")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9']+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def infer_categoria_personale_from_qualifica(qualifica: str | None) -> str | None:
    normalized_qualifica = normalize_text_for_match(qualifica)
    if not normalized_qualifica:
        return None

    if any(hint in normalized_qualifica for hint in ATA_QUALIFICA_HINTS):
        return "ATA"
    if any(hint in normalized_qualifica for hint in DOCENTE_QUALIFICA_HINTS):
        return "docente"
    if any(stem in normalized_qualifica for stem in DOCENTE_QUALIFICA_STEMS):
        return "docente"
    return None


def normalize_categoria_personale(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if value in CATEGORIA_PERSONALE_VALUES:
        return value

    normalized_value = normalize_text_for_match(value)
    if normalized_value in {"ata", "personale ata"}:
        return "ATA"
    if normalized_value in {"docente", "docenti", "personale docente"}:
        return "docente"
    return infer_categoria_personale_from_qualifica(value)


def derive_categoria_personale(pratica_metadata: dict, document_items: list[dict]) -> str | None:
    decreto_items = [
        item for item in document_items
        if item.get("tipo_documento") == "decreto_ricostruzione"
    ]
    decreto_items.sort(key=lambda item: item.get("extractedAt") or "")

    for decreto_item in reversed(decreto_items):
        qualifica = ((decreto_item.get("dati_professionali") or {}).get("qualifica_funzionale"))
        categoria_personale = infer_categoria_personale_from_qualifica(qualifica)
        if categoria_personale:
            return categoria_personale

    existing_value = (pratica_metadata or {}).get("categoria_personale")
    if existing_value in {"docente", "ATA"}:
        return existing_value
    return None


def _is_decreto_ricostruzione_document(document_type: str | None) -> bool:
    return document_type == DOCUMENT_TYPE_DECRETO_RICOSTRUZIONE


def _build_categoria_classification_view(clean_document: dict, classified_document: dict) -> dict:
    return {
        "documentType": classified_document.get("documentType"),
        "sourceCleanKey": classified_document.get("sourceCleanKey"),
        "sourceFile": clean_document.get("sourceFile"),
        "textPreview": clean_document.get("textPreview"),
        "pagesText": clean_document.get("pagesText", []),
        "formsByPage": clean_document.get("formsByPage", []),
        "tablesByPage": clean_document.get("tablesByPage", []),
        "text": clean_document.get("text", ""),
    }


def classify_categoria_personale_with_model(
    model_id: str,
    clean_document: dict,
    classified_document: dict,
) -> dict:
    user_prompt = (
        "Documento clean da classificare per categoria personale:\n"
        f"{json.dumps(_build_categoria_classification_view(clean_document, classified_document), ensure_ascii=False)}"
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"text": user_prompt}],
            }
        ],
        "system": [{"text": CATEGORY_CLASSIFICATION_SYSTEM_PROMPT}],
        "inferenceConfig": {
            "maxTokens": 1000,
            "temperature": 0,
            "topP": 0.9,
        },
    }

    response = bedrock.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )

    response_body = json.loads(response["body"].read())
    raw_text = get_text_block(response_body)
    _log_bedrock_response_meta("categoria_personale_bedrock_response", response_body, raw_text)
    parsed = parse_model_json(raw_text)

    categoria = normalize_categoria_personale(parsed.get("categoria_personale"))
    confidence = str(parsed.get("confidence") or "LOW").upper()
    if confidence not in {"HIGH", "MEDIUM", "LOW"}:
        confidence = "LOW"

    result = {
        "categoria_personale": categoria,
        "confidence": confidence,
        "evidence": str(parsed.get("evidence") or "")[:1000],
    }
    logger.info(
        "categoria_personale_classified categoria=%s confidence=%s evidence=%s",
        result["categoria_personale"],
        result["confidence"],
        result["evidence"],
    )
    return result


def save_categoria_personale_metadata(table, id_pratica: str, categoria_result: dict) -> None:
    categoria = normalize_categoria_personale(categoria_result.get("categoria_personale"))
    if not categoria:
        return

    ensure_metadata_item(table, id_pratica)
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    table.update_item(
        Key={"PK": f"PRATICA#{id_pratica}", "SK": "METADATA"},
        UpdateExpression=(
            "SET categoria_personale = :categoria_personale, "
            "categoria_personale_source = :source, "
            "categoria_personale_confidence = :confidence, "
            "categoria_personale_evidence = :evidence, "
            "categoria_personale_updated_at = :updated_at"
        ),
        ExpressionAttributeValues={
            ":categoria_personale": categoria,
            ":source": "decreto_ricostruzione_category_classifier",
            ":confidence": categoria_result.get("confidence") or "LOW",
            ":evidence": categoria_result.get("evidence") or "categoria determinata dal decreto",
            ":updated_at": now_iso,
        },
    )


# ---------------------------------------------------------------------------
# Metadata state
# ---------------------------------------------------------------------------

def build_metadata_state(
    pratica_metadata: dict, document_items: list[dict]
) -> tuple[dict, list[dict]]:
    present_document_types = {
        item.get("tipo_documento")
        for item in document_items
        if item.get("tipo_documento")
    }

    service_document_types = sorted(present_document_types & ALTERNATIVE_SERVICE_DOCUMENT_TYPES)
    categoria_personale = derive_categoria_personale(pratica_metadata, document_items)

    controls = evaluate_preliminary_controls(present_document_types, categoria_personale)
    flag_completezza = bool(controls) and all(c["esito"] == "superato" for c in controls)

    metadata_state = {
        "flag_servizio_militare": "foglio_congedo_illimitato" in present_document_types,
        "flag_mancato_superamento_prova": "decreto_mancato_superamento_prova" in present_document_types,
        "flag_risposta_osservazione": "osservazione_precedente" in present_document_types,
        "flag_completezza": flag_completezza,
    }

    if categoria_personale:
        metadata_state["categoria_personale"] = categoria_personale

    existing_service_source = (pratica_metadata or {}).get("fonte_documento_servizi")
    if len(service_document_types) == 1:
        metadata_state["fonte_documento_servizi"] = service_document_types[0]
    elif existing_service_source in ALTERNATIVE_SERVICE_DOCUMENT_TYPES:
        metadata_state["fonte_documento_servizi"] = existing_service_source

    return metadata_state, controls


def refresh_metadata_from_documents(table, id_pratica: str, pratica_metadata: dict | None = None) -> dict:
    current_metadata = pratica_metadata or load_pratica_metadata(table, id_pratica)
    document_items = query_document_items(table, id_pratica)
    metadata_state, controls = build_metadata_state(current_metadata, document_items)

    update_parts = []
    expression_attribute_values = {}
    for field_name, field_value in metadata_state.items():
        token = f":{field_name}"
        update_parts.append(f"{field_name} = {token}")
        expression_attribute_values[token] = field_value

    if update_parts:
        table.update_item(
            Key={"PK": f"PRATICA#{id_pratica}", "SK": "METADATA"},
            UpdateExpression="SET " + ", ".join(update_parts),
            ExpressionAttributeValues=expression_attribute_values,
        )

    write_preliminary_controls(id_pratica, controls)

    # Finalizzazione: se tutti i documenti attesi sono stati processati,
    # i controlli rimasti a non_avviato diventano non_superato (documento assente definitivo).
    documenti_attesi = int((current_metadata or {}).get("documenti_attesi", 0))
    if documenti_attesi > 0 and len(document_items) >= documenti_attesi:
        _finalize_pending_controls(id_pratica)

    logger.info(
        "metadata_refreshed id_pratica=%s metadata_state=%s controls=%s present_document_types=%s",
        id_pratica,
        json.dumps(metadata_state, ensure_ascii=False),
        json.dumps([{"id": c["id"], "esito": c["esito"]} for c in controls], ensure_ascii=False),
        sorted(item.get("tipo_documento") for item in document_items if item.get("tipo_documento")),
    )
    return metadata_state


# ---------------------------------------------------------------------------
# Schema resolution
# ---------------------------------------------------------------------------

def resolve_schema_section(document_type: str, schema: dict, pratica_metadata: dict | None = None) -> tuple[str | None, dict | None]:
    matches = []
    for section_name, section_def in schema.get("entita", {}).items():
        if not isinstance(section_def, dict):
            continue
        section_matches = section_def.get("classificatore_match", [])
        if document_type in section_matches:
            matches.append((section_name, section_def))

    if not matches:
        return None, None

    if len(matches) == 1:
        return matches[0]

    fonte_documento_servizi = (pratica_metadata or {}).get("fonte_documento_servizi") or os.environ.get("DEFAULT_FONTE_DOCUMENTO_SERVIZI")
    if fonte_documento_servizi:
        target_prefix = f"DOCUMENTO#{fonte_documento_servizi}"
        for section_name, section_def in matches:
            if section_def.get("sk_prefix") == target_prefix:
                logger.info(
                    "resolve_schema_section_disambiguated document_type=%s fonte_documento_servizi=%s schema_section=%s",
                    document_type,
                    fonte_documento_servizi,
                    section_name,
                )
                return section_name, section_def

    logger.warning(
        "resolve_schema_section_ambiguous document_type=%s candidate_sections=%s pratica_metadata=%s",
        document_type,
        [section_name for section_name, _ in matches],
        json.dumps(pratica_metadata or {}, ensure_ascii=False),
    )
    return None, None


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def build_extraction_view(clean_document: dict, classified_document: dict) -> dict:
    return {
        "documentType": classified_document.get("documentType"),
        "sourceCleanKey": classified_document.get("sourceCleanKey"),
        "sourceFile": clean_document.get("sourceFile"),
        "sourceRawConsolidatedKey": clean_document.get("sourceRawConsolidatedKey"),
        "metadata": clean_document.get("metadata", {}),
        "summary": clean_document.get("summary", {}),
        "textPreview": clean_document.get("textPreview"),
        "pagesText": clean_document.get("pagesText", []),
        "formsByPage": clean_document.get("formsByPage", []),
        "tablesByPage": clean_document.get("tablesByPage", []),
        "text": clean_document.get("text", ""),
    }


def extract_entities_with_model(model_id: str, schema_section_name: str, schema_section: dict, extraction_view: dict) -> dict:
    user_prompt = (
        f"Tipo documento classificato: {extraction_view['documentType']}\n"
        f"Sezione schema selezionata: {schema_section_name}\n\n"
        "Schema da rispettare (estrai solo questi campi):\n"
        f"{json.dumps(schema_section.get('campi', {}), ensure_ascii=False, indent=2)}\n\n"
        "Documento clean da cui estrarre: \n"
        f"{json.dumps(extraction_view, ensure_ascii=False)}"
    )

    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"text": user_prompt}],
            }
        ],
        "system": [{"text": EXTRACTION_SYSTEM_PROMPT}],
        "inferenceConfig": {
            "maxTokens": 6000,
            "temperature": 0,
            "topP": 0.9,
        },
    }

    response = bedrock.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )

    response_body = json.loads(response["body"].read())
    raw_text = get_text_block(response_body)
    _log_bedrock_response_meta("entity_extractor_bedrock_response", response_body, raw_text)
    return parse_model_json(raw_text)


def _get_schema_section_tipo_documento(schema_section: dict) -> str | None:
    sk_prefix = schema_section.get("sk_prefix") or ""
    return sk_prefix.split("#", 1)[1] if "#" in sk_prefix else None


def _is_service_document_section(schema_section: dict) -> bool:
    return _get_schema_section_tipo_documento(schema_section) in ALTERNATIVE_SERVICE_DOCUMENT_TYPES


def extract_service_components_with_model(
    model_id: str,
    schema_section_name: str,
    schema_section: dict,
    extraction_view: dict,
) -> dict:
    campi = schema_section.get("campi", {})
    anagrafica_schema = campi.get("anagrafica", {})
    servizi_node = campi.get("servizi") or {}
    servizi_schema = servizi_node.get("item_struttura", {})
    assenze_schema = ((servizi_schema.get("assenze") or {}).get("item_struttura") or {})
    service_fields_schema = {k: v for k, v in servizi_schema.items() if k != "assenze"}

    user_payload = {
        "schemaSection": schema_section_name,
        "tipoDocumentoTecnico": _get_schema_section_tipo_documento(schema_section),
        "anagraficaSchema": anagrafica_schema,
        "serviceFieldsSchema": service_fields_schema,
        "absenceFieldsSchema": assenze_schema,
        "document": {
            "sourceFile": extraction_view.get("sourceFile"),
            "textPreview": extraction_view.get("textPreview"),
            "pagesText": extraction_view.get("pagesText", []),
            "formsByPage": extraction_view.get("formsByPage", []),
            "tablesByPage": extraction_view.get("tablesByPage", []),
            "text": extraction_view.get("text", ""),
        },
    }

    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"text": json.dumps(user_payload, ensure_ascii=False)}],
            }
        ],
        "system": [{"text": SERVICE_SPECIALIZED_EXTRACTION_SYSTEM_PROMPT}],
        "inferenceConfig": {
            "maxTokens": 6000,
            "temperature": 0,
            "topP": 0.9,
        },
    }

    response = bedrock.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )

    response_body = json.loads(response["body"].read())
    raw_text = get_text_block(response_body)
    _log_bedrock_response_meta("entity_extractor_service_bedrock_response", response_body, raw_text)
    parsed = parse_model_json(raw_text)
    _log_service_debug_snapshot(
        "specialized_llm_raw",
        schemaSection=schema_section_name,
        sourceFile=extraction_view.get("sourceFile"),
        anagrafica=parsed.get("anagrafica"),
        servizi=parsed.get("servizi"),
        assenze=parsed.get("assenze"),
    )
    return parsed


def _normalize_list_against_item_schema(item_schema: dict, raw_items: Any) -> list[dict]:
    if not item_schema:
        return []
    return normalize_against_schema({"item_struttura": item_schema}, raw_items) or []


def _normalize_cell_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _iter_tables_in_page_order(tables_by_page: list[dict]):
    for page_entry in sorted(tables_by_page or [], key=lambda entry: entry.get("page", 0)):
        page_number = page_entry.get("page", 0)
        for table in sorted(page_entry.get("items") or [], key=lambda item: item.get("tableIndex", 0)):
            yield page_number, table.get("tableIndex", 0), table.get("rows") or []


def _iter_forms_in_page_order(forms_by_page: list[dict]):
    for page_entry in sorted(forms_by_page or [], key=lambda entry: entry.get("page", 0)):
        page_number = page_entry.get("page", 0)
        for item_index, item in enumerate(page_entry.get("items") or []):
            key_text = ((item.get("key") or {}).get("text") or "").strip()
            value_text = ((item.get("value") or {}).get("text") or "").strip()
            yield page_number, item_index, key_text, value_text


def _extract_dates_from_text(text_value: str | None) -> list[str]:
    if not text_value:
        return []

    candidates = re.findall(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b\d{4}[/-]\d{2}[/-]\d{2}\b", text_value)
    extracted = []
    seen = set()
    for candidate in candidates:
        normalized = _try_parse_date_string(candidate, italian_format=False)
        if normalized and normalized not in seen:
            seen.add(normalized)
            extracted.append(normalized)
    return extracted


def _extract_school_year_from_text(text_value: str | None) -> str | None:
    if not text_value:
        return None

    match = re.search(r"\b(\d{4})\s*[-/]\s*(\d{4})\b", text_value)
    if not match:
        return None

    year_start, year_end = map(int, match.groups())
    if year_end != year_start + 1:
        return None
    return f"{year_start:04d}/{year_end:04d}"


def _infer_school_year_from_dates(start_value: str | None, end_value: str | None) -> str | None:
    start_date = _parse_date_to_date(start_value)
    end_date = _parse_date_to_date(end_value)
    reference = start_date or end_date
    if not reference:
        return None

    school_year_start = reference.year if reference.month >= 9 else reference.year - 1
    return f"{school_year_start:04d}/{school_year_start + 1:04d}"


def _parse_scrutini_value(text_value: str | None) -> bool | None:
    normalized = normalize_text_for_match(text_value)
    if not normalized:
        return None
    if re.search(r"\bsi\b", normalized):
        return True
    if re.search(r"\bno\b", normalized):
        return False
    return None


def _extract_labeled_fragment(text_value: str | None, label_pattern: str) -> str | None:
    if not text_value:
        return None

    stop_pattern = (
        r"(?:"
        r"in\s*qualit[aà]\s*di|"
        r"classe\s*di\s*concorso|"
        r"tipo\s*posto|"
        r"tipo\s*nomina|"
        r"autorit[aà]\s*nomina|"
        r"situazione\s*prev|"
        r"qual\.?\s*retributiva|"
        r"periodo\s*retribuito|"
        r"tipo\s*servizio|"
        r"tipo\s*attivit[aà]|"
        r"qual\.?\s*servizio|"
        r"partec\.?\s*scrutini(?:\s*e\s*esami)?|"
        r"sede\s*servizio|"
        r"sede\s*titolarit[aà]|"
        r"data\s*protocollo|"
        r"num\.?\s*protocollo|"
        r"giorni\s*effettivi|"
        r"anno\s*scolastico"
        r")"
    )
    match = re.search(
        rf"{label_pattern}\s*:?\s*(.+?)(?=\s*{stop_pattern}\s*:|\s*$)",
        text_value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    fragment = _normalize_cell_text(match.group(1))
    return fragment or None


def _build_service_updates_from_form(key_text: str, value_text: str) -> dict:
    normalized_key = normalize_text_for_match(key_text)
    clean_value = _normalize_cell_text(value_text)
    updates = {}

    if not normalized_key or not clean_value:
        return updates

    if "periodo retribuito" in normalized_key:
        dates = _extract_dates_from_text(clean_value)
        if len(dates) >= 2:
            updates["periodo_retribuito_inizio"] = dates[0]
            updates["periodo_retribuito_fine"] = dates[1]
            updates["data_inizio"] = dates[0]
            updates["data_fine"] = dates[1]
            school_year = _infer_school_year_from_dates(dates[0], dates[1])
            if school_year:
                updates["anno_scolastico"] = school_year
        return updates

    if "in qualita" in normalized_key:
        updates["qualifica_professionale"] = clean_value
        return updates

    if "tipo servizio" in normalized_key:
        updates["tipo_servizio"] = clean_value
        return updates

    if "classe di concorso" in normalized_key:
        updates["classe_concorso"] = clean_value
        return updates

    if "partec scrutini e esami" in normalized_key or "partec scrutini" in normalized_key:
        parsed_value = _parse_scrutini_value(clean_value)
        if parsed_value is not None:
            updates["partecipazione_scrutini"] = parsed_value
        return updates

    if "anno scolastico" in normalized_key:
        school_year = _extract_school_year_from_text(clean_value)
        if school_year:
            updates["anno_scolastico"] = school_year
        return updates

    return updates


def _service_candidate_has_identity(candidate: dict) -> bool:
    return bool(
        candidate.get("periodo_retribuito_inizio")
        and candidate.get("periodo_retribuito_fine")
    )


def _service_candidate_should_split(current: dict, updates: dict) -> bool:
    if not current or not updates or not _service_candidate_has_identity(current):
        return False

    for key, value in updates.items():
        current_value = current.get(key)
        if current_value in (None, "", []) or value in (None, "", []):
            continue
        if current_value != value:
            return True
    return False


def _merge_service_candidate_updates(candidate: dict, updates: dict) -> None:
    for key, value in updates.items():
        if value in (None, "", []):
            continue

        current_value = candidate.get(key)
        if current_value in (None, "", []):
            candidate[key] = value
            continue

        if current_value == value:
            continue

        if isinstance(current_value, str) and isinstance(value, str) and len(value) > len(current_value):
            candidate[key] = value


def _finalize_service_candidate(candidate: dict) -> dict | None:
    if not _service_candidate_has_identity(candidate):
        return None

    finalized = dict(candidate)
    finalized.setdefault("data_inizio", finalized.get("periodo_retribuito_inizio"))
    finalized.setdefault("data_fine", finalized.get("periodo_retribuito_fine"))

    if not finalized.get("anno_scolastico"):
        finalized["anno_scolastico"] = _infer_school_year_from_dates(
            finalized.get("data_inizio"),
            finalized.get("data_fine"),
        )

    if not (finalized.get("qualifica_professionale") or finalized.get("tipo_servizio")):
        return None

    return finalized


def _extract_services_from_forms(forms_by_page: list[dict]) -> list[dict]:
    extracted_services = []
    current_candidate = {}

    for page_number, item_index, key_text, value_text in _iter_forms_in_page_order(forms_by_page):
        updates = _build_service_updates_from_form(key_text, value_text)
        if not updates:
            continue

        if _service_candidate_should_split(current_candidate, updates):
            finalized = _finalize_service_candidate(current_candidate)
            if finalized:
                extracted_services.append(finalized)
            current_candidate = {}

        _merge_service_candidate_updates(current_candidate, updates)

        logger.debug(
            "service_form_scan page=%s item=%s key=%s updates=%s",
            page_number,
            item_index,
            key_text,
            json.dumps(updates, ensure_ascii=False),
        )

    finalized = _finalize_service_candidate(current_candidate)
    if finalized:
        extracted_services.append(finalized)

    return extracted_services


def _looks_like_service_row(row: list[Any]) -> bool:
    if not isinstance(row, list):
        return False
    if len(_row_dates(row)) < 2:
        return False
    if _looks_like_absence_row(row):
        return False

    normalized_row = normalize_text_for_match(_row_joined_text(row))
    if not normalized_row:
        return False

    explicit_service_tokens = {
        "periodo retribuito",
        "in qualita",
        "tipo servizio",
        "partec scrutini",
        "sede servizio",
    }
    return _normalized_text_contains_hint(normalized_row, SERVICE_ROW_HINTS) or any(
        token in normalized_row for token in explicit_service_tokens
    )


def _build_service_item_from_table_row(row: list[Any]) -> dict | None:
    if not _looks_like_service_row(row):
        return None

    dates = _row_dates(row)
    if len(dates) < 2:
        return None

    row_text = _row_joined_text(row)
    periodo_retribuito_text = _extract_labeled_fragment(row_text, r"periodo\s*retribuito")
    periodo_retribuito_dates = _extract_dates_from_text(periodo_retribuito_text or "")

    data_inizio = dates[0].isoformat()
    data_fine = dates[1].isoformat()
    periodo_retribuito_inizio = (
        periodo_retribuito_dates[0] if len(periodo_retribuito_dates) >= 2 else data_inizio
    )
    periodo_retribuito_fine = (
        periodo_retribuito_dates[1] if len(periodo_retribuito_dates) >= 2 else data_fine
    )

    item = {
        "data_inizio": data_inizio,
        "data_fine": data_fine,
        "periodo_retribuito_inizio": periodo_retribuito_inizio,
        "periodo_retribuito_fine": periodo_retribuito_fine,
    }

    qualifica = _extract_labeled_fragment(row_text, r"in\s*qualit[aà]\s*di")
    if qualifica:
        item["qualifica_professionale"] = qualifica

    tipo_servizio = _extract_labeled_fragment(row_text, r"tipo\s*servizio")
    if tipo_servizio:
        item["tipo_servizio"] = tipo_servizio

    classe_concorso = _extract_labeled_fragment(row_text, r"classe\s*di\s*concorso")
    if classe_concorso:
        item["classe_concorso"] = classe_concorso

    scrutini_text = _extract_labeled_fragment(row_text, r"partec\.?\s*scrutini(?:\s*e\s*esami)?")
    scrutini_value = _parse_scrutini_value(scrutini_text or row_text)
    if scrutini_value is not None:
        item["partecipazione_scrutini"] = scrutini_value

    anno_scolastico = _extract_school_year_from_text(row_text)
    if not anno_scolastico:
        anno_scolastico = _infer_school_year_from_dates(item["data_inizio"], item["data_fine"])
    if anno_scolastico:
        item["anno_scolastico"] = anno_scolastico

    return _finalize_service_candidate(item)


def _extract_services_from_tables(tables_by_page: list[dict]) -> list[dict]:
    extracted_services = []

    for page_number, table_index, rows in _iter_tables_in_page_order(tables_by_page):
        detected_rows = 0
        for row in rows:
            service_item = _build_service_item_from_table_row(row)
            if not service_item:
                continue
            extracted_services.append(service_item)
            detected_rows += 1

        if detected_rows:
            logger.info(
                "service_table_service_scan page=%s table=%s detected_rows=%s",
                page_number,
                table_index,
                detected_rows,
            )

    return extracted_services


def _extract_services_with_support(tables_by_page: list[dict], forms_by_page: list[dict]) -> list[dict]:
    table_services = _extract_services_from_tables(tables_by_page)
    form_services = _extract_services_from_forms(forms_by_page)
    merged_services = _merge_service_lists(
        table_services,
        form_services,
        merge_missing_fields=True,
    )

    _log_service_debug_snapshot(
        "table_services_extracted",
        mergeLogicVersion=SERVICE_MERGE_LOGIC_VERSION,
        tableServices=table_services,
        formSupportServices=form_services,
        services=merged_services,
        count=len(merged_services),
    )
    return merged_services


def _row_dates(row: list[Any]) -> list[date]:
    parsed_dates = []
    for cell in row:
        parsed = _parse_date_to_date(_normalize_cell_text(cell))
        if parsed:
            parsed_dates.append(parsed)
        if len(parsed_dates) == 2:
            break
    return parsed_dates


def _extract_duration_from_row(row: list[Any]) -> int | None:
    for cell in row:
        cell_text = _normalize_cell_text(cell)
        if not cell_text:
            continue

        if re.search(r"durata\s*[:=]?\s*\d{1,3}\s*(?:mesi?|anni?)\b", cell_text, flags=re.IGNORECASE):
            continue

        explicit_match = re.search(r"(?:durata|giorni?|gg\.?)\s*[:=]?\s*(\d{1,3})", cell_text, flags=re.IGNORECASE)
        if explicit_match:
            return int(explicit_match.group(1))

    for cell in row:
        cell_text = _normalize_cell_text(cell)
        if re.fullmatch(r"\d{1,3}", cell_text):
            value = int(cell_text)
            if 0 < value <= 366:
                return value
    return None


def _clean_absence_description(text_value: str) -> str:
    cleaned = text_value
    cleaned = re.sub(r"durata\s*:\s*\d+\s*giorni?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"%\s*retribuzione\s*:\s*[0-9.,]+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"retribuzione\s*:\s*[0-9.,]+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -;,.:")
    cleaned = re.sub(r"(?:\b(?:o|e|ed)\b)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -;,.:")
    return cleaned.strip()


def _normalized_text_contains_hint(normalized_text: str, hints: set[str]) -> bool:
    return any(hint in normalized_text for hint in hints)


def _count_matching_hints(normalized_text: str, hints: set[str]) -> int:
    return sum(1 for hint in hints if hint in normalized_text)


def _row_joined_text(row: list[Any]) -> str:
    return " ".join(_normalize_cell_text(cell) for cell in row if _normalize_cell_text(cell))


def _extract_absence_code(text_value: str | None) -> str | None:
    if not text_value:
        return None

    cleaned = _clean_absence_description(text_value)
    normalized = normalize_text_for_match(cleaned)
    if not _normalized_text_contains_hint(normalized, ABSENCE_ROW_HINTS):
        return None

    match = re.search(r"\(([A-Z]{1,5}\d{0,3})\)", cleaned, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def _absence_description_key(text_value: str | None) -> str:
    if not text_value:
        return ""

    cleaned = _clean_absence_description(text_value)
    code = _extract_absence_code(cleaned)
    if code:
        return code

    normalized = normalize_text_for_match(cleaned)
    normalized = re.sub(r"(?:\b(?:o|e|ed)\b)\s*$", "", normalized).strip()
    return normalized


def _is_probable_absence_description(text_value: str | None) -> bool:
    if not text_value:
        return False

    cleaned = _clean_absence_description(text_value)
    normalized = normalize_text_for_match(cleaned)
    if not normalized:
        return False

    absence_hint_count = _count_matching_hints(normalized, ABSENCE_ROW_HINTS)
    service_hint_count = _count_matching_hints(normalized, SERVICE_ROW_HINTS)
    has_absence_hint = absence_hint_count > 0
    has_service_hint = service_hint_count > 0
    has_absence_code = bool(_extract_absence_code(cleaned))

    if service_hint_count >= 2 and absence_hint_count == 0:
        return False
    if has_service_hint and not has_absence_hint and not has_absence_code:
        return False
    if len(cleaned) > 140 and service_hint_count >= 1 and absence_hint_count <= 1:
        return False

    return has_absence_hint or has_absence_code


def _extract_absence_description_from_row(row: list[Any]) -> str | None:
    parts = []
    date_values = _row_dates(row)
    dates_left = len(date_values)

    for cell in row:
        cell_text = _normalize_cell_text(cell)
        if not cell_text:
            continue

        if dates_left and _parse_date_to_date(cell_text):
            dates_left -= 1
            continue

        if re.fullmatch(r"\d+(?:[.,]\d+)?", cell_text):
            continue

        if re.fullmatch(r"\d{4}/\d{4}", cell_text):
            continue

        parts.append(cell_text)

    if not parts:
        return None

    cleaned = _clean_absence_description(" ".join(parts))
    return cleaned or None


def _row_contains_absence_hint(row: list[Any]) -> bool:
    normalized_row = normalize_text_for_match(_row_joined_text(row))
    return _normalized_text_contains_hint(normalized_row, ABSENCE_ROW_HINTS)


def _row_contains_service_hint(row: list[Any]) -> bool:
    normalized_row = normalize_text_for_match(_row_joined_text(row))
    return _normalized_text_contains_hint(normalized_row, SERVICE_ROW_HINTS)


def _table_has_absence_header(rows: list[list[Any]]) -> bool:
    header_text = normalize_text_for_match(
        " ".join(
            _normalize_cell_text(cell)
            for row in rows[:3]
            for cell in row
            if _normalize_cell_text(cell)
        )
    )
    return any(token in header_text for token in ("assenze", "permessi", "motivo assenza", "assenza a s", "assenze a s"))


def _looks_like_absence_row(row: list[Any], table_is_absence: bool = False) -> bool:
    if not isinstance(row, list):
        return False

    dates = _row_dates(row)
    if len(dates) < 2:
        return False

    description = _extract_absence_description_from_row(row)
    duration = _extract_duration_from_row(row)
    has_absence_hint = _row_contains_absence_hint(row)
    has_service_hint = _row_contains_service_hint(row)

    if has_service_hint and not has_absence_hint:
        return False
    if description and len(description) > 140 and has_service_hint:
        return False

    if has_absence_hint:
        return bool(description and _is_probable_absence_description(description))

    if table_is_absence:
        return bool(duration is not None and description and _is_probable_absence_description(description))

    return False


def _build_absence_item_for_schema(assenze_schema: dict, row: list[Any]) -> dict | None:
    dates = _row_dates(row)
    if len(dates) < 2:
        return None

    description = _extract_absence_description_from_row(row)
    if not _is_probable_absence_description(description):
        return None

    duration = _extract_duration_from_row(row)
    item = {}

    if "periodo_fruizione_inizio" in assenze_schema:
        item["periodo_fruizione_inizio"] = dates[0].isoformat()
    elif "data_inizio_assenza" in assenze_schema:
        item["data_inizio_assenza"] = dates[0].isoformat()

    if "periodo_fruizione_fine" in assenze_schema:
        item["periodo_fruizione_fine"] = dates[1].isoformat()
    elif "data_fine_assenza" in assenze_schema:
        item["data_fine_assenza"] = dates[1].isoformat()

    if "tipologia_assenza" in assenze_schema and description:
        item["tipologia_assenza"] = description
    elif "tipo_assenza" in assenze_schema and description:
        item["tipo_assenza"] = description

    if duration is not None:
        if "durata_assenza_giorni" in assenze_schema:
            item["durata_assenza_giorni"] = duration
        elif "durata_interruzione_giorni" in assenze_schema:
            item["durata_interruzione_giorni"] = duration

    return item if item else None


def _absence_identity(absence: dict) -> tuple:
    start_value = absence.get("periodo_fruizione_inizio") or absence.get("data_inizio_assenza")
    end_value = absence.get("periodo_fruizione_fine") or absence.get("data_fine_assenza")
    return (start_value, end_value)


def _absence_signature(absence: dict) -> tuple:
    description = absence.get("tipologia_assenza") or absence.get("tipo_assenza") or ""
    return _absence_identity(absence) + (_absence_description_key(description),)


def _descriptions_loosely_match(left_description: str | None, right_description: str | None) -> bool:
    left_key = _absence_description_key(left_description)
    right_key = _absence_description_key(right_description)
    if not left_key or not right_key:
        return True
    if left_key == right_key:
        return True

    left_code = _extract_absence_code(left_description)
    right_code = _extract_absence_code(right_description)
    if left_code and right_code and left_code == right_code:
        return True

    return left_key in right_key or right_key in left_key


def _choose_preferred_absence_description(current: str | None, candidate: str | None) -> str | None:
    options = [value for value in (current, candidate) if value]
    if not options:
        return None

    def score(value: str) -> tuple:
        cleaned = _clean_absence_description(value)
        normalized = normalize_text_for_match(cleaned)
        base_score = 0
        if _extract_absence_code(cleaned):
            base_score += 100
        if _normalized_text_contains_hint(normalized, ABSENCE_ROW_HINTS):
            base_score += 40
        if _normalized_text_contains_hint(normalized, SERVICE_ROW_HINTS):
            base_score -= 100
        if re.search(r"(?:\b(?:o|e|ed)\b)\s*$", cleaned, flags=re.IGNORECASE):
            base_score -= 10
        return (base_score, -len(cleaned))

    return max(options, key=score)


def _service_identity(service: dict) -> tuple:
    service_range = _extract_service_range(service)
    if service_range:
        return (service_range[0].isoformat(), service_range[1].isoformat())
    school_year = service.get("anno_scolastico")
    return (school_year,) if school_year else tuple()


def _service_signature(service: dict) -> tuple:
    return _service_identity(service) + (
        normalize_text_for_match(service.get("qualifica_professionale")),
        normalize_text_for_match(service.get("tipo_servizio")),
    )


def _services_loosely_match(left_service: dict, right_service: dict) -> bool:
    left_identity = _service_identity(left_service)
    right_identity = _service_identity(right_service)
    return bool(left_identity and right_identity and left_identity == right_identity)


def _service_sort_key(service: dict) -> tuple:
    service_range = _extract_service_range(service)
    if service_range:
        return service_range[0], service_range[1]
    return date.max, date.max


def _merge_service_lists(
    primary_services: list[dict],
    secondary_services: list[dict],
    *,
    merge_missing_fields: bool = False,
) -> list[dict]:
    merged = []
    index_by_signature = {}

    for source in (primary_services or []) + (secondary_services or []):
        if not isinstance(source, dict):
            continue

        signature = _service_signature(source)
        match_idx = index_by_signature.get(signature)
        if match_idx is None:
            for idx, current in enumerate(merged):
                if _services_loosely_match(current, source):
                    match_idx = idx
                    break

        if match_idx is not None:
            if merge_missing_fields:
                current = merged[match_idx]
                for key, value in source.items():
                    if key == "assenze":
                        continue
                    if current.get(key) in (None, "", []) and value not in (None, "", []):
                        current[key] = value
                index_by_signature[_service_signature(current)] = match_idx
            continue

        index_by_signature[signature] = len(merged)
        merged.append(dict(source))

    return sorted(merged, key=_service_sort_key)


def _merge_absence_lists(
    primary_absences: list[dict],
    secondary_absences: list[dict],
    *,
    prefer_primary: bool = False,
    merge_missing_fields: bool = False,
) -> list[dict]:
    merged = []
    index_by_signature = {}

    for source in (primary_absences or []) + (secondary_absences or []):
        if not isinstance(source, dict):
            continue

        signature = _absence_signature(source)
        match_idx = index_by_signature.get(signature)
        if match_idx is None:
            source_identity = _absence_identity(source)
            for idx, current in enumerate(merged):
                if _absence_identity(current) == source_identity:
                    match_idx = idx
                    break

        if match_idx is not None:
            if merge_missing_fields:
                current = merged[match_idx]
                for key, value in source.items():
                    if key in {"tipologia_assenza", "tipo_assenza"}:
                        if prefer_primary:
                            if current.get(key) in (None, "", []) and value not in (None, "", []):
                                current[key] = value
                        else:
                            preferred = _choose_preferred_absence_description(current.get(key), value)
                            if preferred:
                                current[key] = preferred
                        continue
                    if current.get(key) in (None, "", []) and value not in (None, "", []):
                        current[key] = value
                index_by_signature[_absence_signature(current)] = match_idx
            continue

        index_by_signature[signature] = len(merged)
        merged.append(dict(source))

    return merged


def _extract_absences_from_tables(tables_by_page: list[dict], assenze_schema: dict) -> list[dict]:
    extracted_absences = []
    previous_table_was_absence = False

    for page_number, table_index, rows in _iter_tables_in_page_order(tables_by_page):
        has_header = _table_has_absence_header(rows)
        candidate_rows = [row for row in rows if _looks_like_absence_row(row, table_is_absence=(has_header or previous_table_was_absence))]
        table_is_absence = bool(has_header or (previous_table_was_absence and candidate_rows))

        if not table_is_absence:
            previous_table_was_absence = False
            continue

        previous_table_was_absence = True
        for row in candidate_rows:
            absence_item = _build_absence_item_for_schema(assenze_schema, row)
            if absence_item:
                extracted_absences.append(absence_item)

        logger.info(
            "service_table_absence_scan page=%s table=%s detected_rows=%s",
            page_number,
            table_index,
            len(candidate_rows),
        )

    _log_service_debug_snapshot(
        "table_absences_extracted",
        absences=extracted_absences,
        count=len(extracted_absences),
    )
    return extracted_absences


def _assign_absences_to_services(services: list[dict], absences: list[dict]) -> list[dict]:
    assigned_services = []
    for service in services:
        if not isinstance(service, dict):
            continue
        service_copy = dict(service)
        service_copy["assenze"] = []
        assigned_services.append(service_copy)

    if not assigned_services or not absences:
        return assigned_services

    for absence in absences:
        if not isinstance(absence, dict):
            continue

        absence_range = _extract_absence_range(absence)
        if not absence_range:
            logger.info(
                "service_absence_unassigned reason=missing_absence_range absence=%s",
                json.dumps(absence, ensure_ascii=False),
            )
            continue

        candidate_matches = []
        for idx, service in enumerate(assigned_services):
            service_range = _extract_service_range(service)
            if not service_range:
                continue
            if _range_contains(service_range, absence_range):
                candidate_matches.append((idx, service_range))

        if not candidate_matches:
            logger.info(
                "service_absence_unassigned reason=no_service_contains_absence start=%s end=%s absence=%s",
                absence_range[0],
                absence_range[1],
                json.dumps(absence, ensure_ascii=False),
            )
            continue

        best_idx, _ = max(
            candidate_matches,
            key=lambda item: (item[1][0].toordinal(), -((item[1][1] - item[1][0]).days)),
        )
        assigned_services[best_idx]["assenze"].append(absence)

    for service in assigned_services:
        service["assenze"] = sorted(
            service.get("assenze") or [],
            key=lambda absence: (
                (_extract_absence_range(absence) or (date.max, date.max))[0],
                (_extract_absence_range(absence) or (date.max, date.max))[1],
            ),
        )

    return assigned_services


def _merge_table_absences_into_raw_extracted(
    schema_section: dict,
    extraction_view: dict,
    raw_extracted: dict,
) -> dict:
    campi = schema_section.get("campi", {})
    servizi_node = campi.get("servizi") or {}
    servizi_schema = servizi_node.get("item_struttura", {})
    assenze_schema = ((servizi_schema.get("assenze") or {}).get("item_struttura") or {})
    service_fields_schema = {k: v for k, v in servizi_schema.items() if k != "assenze"}

    normalized_services = _normalize_list_against_item_schema(
        service_fields_schema,
        (raw_extracted or {}).get("servizi"),
    )
    fallback_services = _normalize_list_against_item_schema(
        service_fields_schema,
        _extract_services_with_support(
            extraction_view.get("tablesByPage", []),
            extraction_view.get("formsByPage", []),
        ),
    )
    normalized_services = _merge_service_lists(normalized_services, fallback_services)
    if not normalized_services:
        return raw_extracted or {}

    table_absences = _normalize_list_against_item_schema(
        assenze_schema,
        _extract_absences_from_tables(extraction_view.get("tablesByPage", []), assenze_schema),
    )

    merged = dict(raw_extracted or {})
    merged["servizi"] = _assign_absences_to_services(normalized_services, table_absences)
    _log_service_debug_snapshot(
        "table_fallback_merge",
        mergeLogicVersion=SERVICE_MERGE_LOGIC_VERSION,
        genericRawServices=(raw_extracted or {}).get("servizi"),
        normalizedServices=normalized_services,
        tableFallbackServices=fallback_services,
        tableAbsences=table_absences,
        finalServices=merged.get("servizi"),
    )
    logger.info(
        "service_table_absence_fallback_done services=%s flat_absences=%s assigned_absences=%s",
        len(normalized_services),
        len(table_absences),
        sum(len(service.get("assenze") or []) for service in merged.get("servizi") or []),
    )
    return merged


def _merge_specialized_service_extraction(
    model_id: str,
    schema_section_name: str,
    schema_section: dict,
    extraction_view: dict,
    raw_extracted: dict,
) -> dict:
    campi = schema_section.get("campi", {})
    anagrafica_schema = campi.get("anagrafica", {})
    servizi_node = campi.get("servizi") or {}
    servizi_schema = servizi_node.get("item_struttura", {})
    assenze_schema = ((servizi_schema.get("assenze") or {}).get("item_struttura") or {})
    service_fields_schema = {k: v for k, v in servizi_schema.items() if k != "assenze"}

    specialized_raw = extract_service_components_with_model(
        model_id=model_id,
        schema_section_name=schema_section_name,
        schema_section=schema_section,
        extraction_view=extraction_view,
    )

    normalized_anagrafica = normalize_against_schema(
        anagrafica_schema,
        specialized_raw.get("anagrafica"),
    )
    llm_normalized_services = _normalize_list_against_item_schema(
        service_fields_schema,
        specialized_raw.get("servizi"),
    )
    fallback_services = _normalize_list_against_item_schema(
        service_fields_schema,
        _extract_services_with_support(
            extraction_view.get("tablesByPage", []),
            extraction_view.get("formsByPage", []),
        ),
    )
    merged_services = _merge_service_lists(llm_normalized_services, fallback_services)
    llm_normalized_absences = _normalize_list_against_item_schema(
        assenze_schema,
        specialized_raw.get("assenze"),
    )
    table_absences = _normalize_list_against_item_schema(
        assenze_schema,
        _extract_absences_from_tables(extraction_view.get("tablesByPage", []), assenze_schema),
    )
    merged_absences = _merge_absence_lists(
        llm_normalized_absences,
        table_absences,
        prefer_primary=True,
    )

    merged = dict(raw_extracted or {})
    if not _is_effectively_empty(normalized_anagrafica):
        merged["anagrafica"] = normalized_anagrafica

    if merged_services:
        merged["servizi"] = _assign_absences_to_services(merged_services, merged_absences)

    _log_service_debug_snapshot(
        "specialized_merge",
        mergeLogicVersion=SERVICE_MERGE_LOGIC_VERSION,
        genericRawServices=(raw_extracted or {}).get("servizi"),
        genericRawAnagrafica=(raw_extracted or {}).get("anagrafica"),
        specializedNormalizedAnagrafica=normalized_anagrafica,
        specializedNormalizedServices=llm_normalized_services,
        tableFallbackServices=fallback_services,
        mergedServices=merged_services,
        llmNormalizedAbsences=llm_normalized_absences,
        tableFallbackAbsences=table_absences,
        mergedAbsences=merged_absences,
        finalServices=merged.get("servizi"),
    )
    logger.info(
        "service_specialized_merge_done schema_section=%s services=%s flat_absences=%s assigned_absences=%s",
        schema_section_name,
        len(merged_services),
        len(merged_absences),
        sum(len(service.get("assenze") or []) for service in merged.get("servizi") or []),
    )
    return merged


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _format_date_parts(year: int, month: int, day: int, italian_format: bool) -> str:
    if italian_format:
        return f"{day:02d}/{month:02d}/{year:04d}"
    return f"{year:04d}-{month:02d}-{day:02d}"


def _try_parse_date_string(value: str, italian_format: bool) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None

    formats = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(candidate, fmt)
            return _format_date_parts(parsed.year, parsed.month, parsed.day, italian_format)
        except ValueError:
            continue

    compact_match = re.fullmatch(r"(\d{2})[ ]+(\d{2})[ ]+(\d{4})", candidate)
    if compact_match:
        day, month, year = map(int, compact_match.groups())
        return _format_date_parts(year, month, day, italian_format)

    return None


def _normalize_date_value(value: Any, field_schema: dict) -> Any:
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    else:
        value = str(value).strip()
        if not value:
            return None

    field_type = str(field_schema.get("tipo", "")).upper()
    italian_format = "DD/MM/YYYY" in field_type

    normalized_value = _try_parse_date_string(value, italian_format=italian_format)
    return normalized_value or value


def _normalize_scalar(value: Any, field_schema: dict) -> Any:
    default_value = field_schema.get("default_se_assente")

    if value is None:
        if default_value is None:
            return None
        value = default_value

    if isinstance(value, str):
        value = value.strip()
        if not value:
            if default_value is None:
                return None
            value = default_value

    field_type = str(field_schema.get("tipo", ""))

    if field_type.startswith("Boolean"):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in {"true", "si", "s", "yes", "presente", "vero"}:
                return True
            if lowered in {"false", "no", "n", "assente", "falso"}:
                return False
        return None

    if field_type.startswith("Date"):
        return _normalize_date_value(value, field_schema)

    if field_type.startswith("Int"):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    return value


EQUIVALENT_DATE_FIELD_PAIRS = [
    ("data_immissione_in_ruolo", "data_decorrenza_giuridica"),
    ("data_decorrenza_economica", "data_assunzione_in_servizio"),
]


def _apply_equivalent_date_fields(normalized_value: Any) -> Any:
    if isinstance(normalized_value, list):
        return [_apply_equivalent_date_fields(item) for item in normalized_value]

    if not isinstance(normalized_value, dict):
        return normalized_value

    for key, value in list(normalized_value.items()):
        normalized_value[key] = _apply_equivalent_date_fields(value)

    for left_field, right_field in EQUIVALENT_DATE_FIELD_PAIRS:
        if left_field not in normalized_value or right_field not in normalized_value:
            continue

        left_value = normalized_value.get(left_field)
        right_value = normalized_value.get(right_field)
        if left_value and not right_value:
            normalized_value[right_field] = left_value
        elif right_value and not left_value:
            normalized_value[left_field] = right_value

    return normalized_value


def _parse_date_to_date(value: Any):
    if not isinstance(value, str):
        return None

    normalized_value = _try_parse_date_string(value, italian_format=True) or _try_parse_date_string(value, italian_format=False)
    candidate = (normalized_value or value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue
    return None


def _extract_range(record: dict, candidate_pairs: list[tuple[str, str]]):
    for start_field, end_field in candidate_pairs:
        start_value = _parse_date_to_date(record.get(start_field))
        end_value = _parse_date_to_date(record.get(end_field))
        if start_value and end_value:
            return start_value, end_value
    return None


def _school_year_range_from_value(value: Any):
    if not isinstance(value, str):
        return None

    match = re.fullmatch(r"\s*(\d{4})\s*[-/]\s*(\d{4})\s*", value)
    if not match:
        return None

    year_start, year_end = map(int, match.groups())
    if year_end != year_start + 1:
        return None
    return date(year_start, 9, 1), date(year_end, 8, 31)


def _extract_flexible_range(record: dict, candidate_pairs: list[tuple[str, str]]):
    for start_field, end_field in candidate_pairs:
        start_value = _parse_date_to_date(record.get(start_field))
        end_value = _parse_date_to_date(record.get(end_field))
        if start_value and end_value:
            return start_value, end_value
        if start_value and not end_value:
            return start_value, start_value
        if end_value and not start_value:
            return end_value, end_value

    school_year_range = _school_year_range_from_value(record.get("anno_scolastico"))
    if school_year_range:
        return school_year_range
    return None


def _extract_service_range(record: dict):
    return _extract_flexible_range(
        record,
        [
            ("periodo_retribuito_inizio", "periodo_retribuito_fine"),
            ("data_inizio", "data_fine"),
            ("periodo_riferimento_inizio", "periodo_riferimento_fine"),
        ],
    )


def _extract_absence_range(record: dict):
    return _extract_flexible_range(
        record,
        [
            ("periodo_fruizione_inizio", "periodo_fruizione_fine"),
            ("data_inizio_assenza", "data_fine_assenza"),
        ],
    )


def _range_contains(container_range, contained_range) -> bool:
    if not container_range or not contained_range:
        return True
    container_start, container_end = container_range
    contained_start, contained_end = contained_range
    return container_start <= contained_start and contained_end <= container_end


def _ranges_overlap(left_range, right_range) -> bool:
    if not left_range or not right_range:
        return True
    left_start, left_end = left_range
    right_start, right_end = right_range
    return left_start <= right_end and right_start <= left_end


def _filter_service_absences(normalized_value: Any) -> Any:
    if isinstance(normalized_value, list):
        return [_filter_service_absences(item) for item in normalized_value]

    if not isinstance(normalized_value, dict):
        return normalized_value

    for key, value in list(normalized_value.items()):
        normalized_value[key] = _filter_service_absences(value)

    services = normalized_value.get("servizi")
    if not isinstance(services, list):
        return normalized_value

    for service in services:
        if not isinstance(service, dict):
            continue

        absences = service.get("assenze")
        if not isinstance(absences, list) or not absences:
            continue

        service_range = _extract_service_range(service)
        filtered_absences = []
        for absence in absences:
            if not isinstance(absence, dict):
                continue

            description = absence.get("tipologia_assenza") or absence.get("tipo_assenza")
            if description and not _is_probable_absence_description(description):
                logger.info(
                    "service_absence_discarded reason=non_absence_description absence=%s",
                    json.dumps(absence, ensure_ascii=False),
                )
                continue

            absence_range = _extract_absence_range(absence)
            if absence_range and service_range and not _range_contains(service_range, absence_range):
                continue
            filtered_absences.append(absence)

        deduplicated_absences = _merge_absence_lists(filtered_absences, [])
        service["assenze"] = sorted(
            deduplicated_absences,
            key=lambda absence: (
                (_extract_absence_range(absence) or (date.max, date.max))[0],
                (_extract_absence_range(absence) or (date.max, date.max))[1],
            ),
        )

    return normalized_value

def _is_effectively_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return all(_is_effectively_empty(item) for item in value)
    if isinstance(value, dict):
        return all(_is_effectively_empty(item) for item in value.values())
    return False


def normalize_against_schema(schema_node: dict, raw_value: Any) -> Any:
    if not isinstance(schema_node, dict):
        return None

    if "item_struttura" in schema_node:
        items = raw_value if isinstance(raw_value, list) else []
        normalized_items = []
        for item in items:
            normalized_item = normalize_against_schema(schema_node["item_struttura"], item)
            if not _is_effectively_empty(normalized_item):
                normalized_items.append(normalized_item)
        return normalized_items

    if "tipo" in schema_node:
        return _normalize_scalar(raw_value, schema_node)

    source = raw_value if isinstance(raw_value, dict) else {}
    normalized = {}
    for field_name, field_schema in schema_node.items():
        if isinstance(field_schema, dict):
            normalized[field_name] = normalize_against_schema(field_schema, source.get(field_name))
    return _filter_service_absences(_apply_equivalent_date_fields(normalized))


SCHOOL_ORDER_SEQUENCE = ("infanzia", "primaria", "secondaria")


def _clean_header(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9/]+", " ", text).strip()


def _cell(row: list[Any], index: int) -> Any:
    return row[index] if 0 <= index < len(row) else ""


def _int_cell(value: Any) -> int:
    text = re.sub(r"\s+", "", str(value or "").strip())
    return int(text) if re.fullmatch(r"\d+", text) else 0


def _school_year_cell(value: Any) -> str | None:
    match = re.search(r"\b(\d{4})\s*/\s*(\d{2,4})\b", str(value or ""))
    return f"{match.group(1)}/{match.group(2)}" if match else None


def _preruolo_table_pairs(rows: list[list[Any]]) -> tuple[int, list[tuple[int, int]]] | None:
    for row_index, row in enumerate(rows[:5]):
        pairs = []
        for column_index in range(max(len(row) - 1, 0)):
            left = _clean_header(_cell(row, column_index))
            right = _clean_header(_cell(row, column_index + 1))
            if left in {"mese", "mesi"} and right in {"giorno", "giorni", "gg"}:
                pairs.append((column_index, column_index + 1))
        if pairs:
            return row_index, pairs
    return None


def _school_order_for_pair(
    rows: list[list[Any]],
    header_row_index: int,
    month_col: int,
    day_col: int,
    pair_index: int,
    pairs_count: int,
) -> str | None:
    for header_row in reversed(rows[max(0, header_row_index - 3) : header_row_index]):
        for column_index in (month_col, day_col):
            label = _clean_header(_cell(header_row, column_index))
            for order in SCHOOL_ORDER_SEQUENCE:
                if label == order:
                    return order
    return SCHOOL_ORDER_SEQUENCE[pair_index] if pairs_count == len(SCHOOL_ORDER_SEQUENCE) else None


def _school_year_column(rows: list[list[Any]], header_row_index: int) -> int | None:
    max_columns = max((len(row) for row in rows), default=0)
    for column_index in range(max_columns):
        header = " ".join(_clean_header(_cell(row, column_index)) for row in rows[: header_row_index + 1])
        if "anno" in header and "scolastico" in header:
            return column_index
    return None


def _extract_decreto_preruolo_from_tables(tables_by_page: list[dict]) -> list[dict]:
    items = []
    for page in tables_by_page or []:
        for table in page.get("items", []) or []:
            rows = [row for row in table.get("rows", []) if isinstance(row, list)]
            pair_data = _preruolo_table_pairs(rows)
            if not pair_data:
                continue

            header_row_index, pairs = pair_data
            year_col = _school_year_column(rows, header_row_index)
            order_pairs = [
                (_school_order_for_pair(rows, header_row_index, month_col, day_col, idx, len(pairs)), month_col, day_col)
                for idx, (month_col, day_col) in enumerate(pairs)
            ]

            for row in rows[header_row_index + 1 :]:
                year = _school_year_cell(_cell(row, year_col)) if year_col is not None else None
                year = year or next((_school_year_cell(cell) for cell in row if _school_year_cell(cell)), None)
                if not year:
                    continue

                for order, month_col, day_col in order_pairs:
                    mesi = _int_cell(_cell(row, month_col))
                    giorni = _int_cell(_cell(row, day_col))
                    if order and (mesi or giorni):
                        items.append(
                            {
                                "anno_scolastico": year,
                                "ordine_scuola": order,
                                "anni": 0,
                                "mesi": mesi,
                                "giorni": giorni,
                            }
                        )
    return items


def _override_decreto_preruolo_from_tables(
    schema_section: dict,
    extraction_view: dict,
    extracted_fields: dict,
) -> dict:
    field_schema = schema_section.get("campi", {}).get("servizi_preruolo_riconosciuti")
    if not field_schema:
        return extracted_fields

    table_items = _extract_decreto_preruolo_from_tables(extraction_view.get("tablesByPage", []))
    if not table_items:
        return extracted_fields

    updated = dict(extracted_fields or {})
    updated["servizi_preruolo_riconosciuti"] = normalize_against_schema(field_schema, table_items)
    logger.info("decreto_preruolo_table_override count=%s", len(updated["servizi_preruolo_riconosciuti"]))
    return updated


# ---------------------------------------------------------------------------
# Item building
# ---------------------------------------------------------------------------

def resolve_id_pratica(classified_document: dict, clean_document: dict) -> str:
    candidates = [
        classified_document.get("id_pratica"),
        classified_document.get("idPratica"),
        clean_document.get("id_pratica"),
        clean_document.get("idPratica"),
        clean_document.get("metadata", {}).get("id_pratica"),
        os.environ.get("DEFAULT_ID_PRATICA"),
    ]
    for candidate in candidates:
        if candidate:
            return str(candidate)
    raise ValueError("Impossibile determinare id_pratica")


def build_dynamo_item(
    id_pratica: str,
    schema_section_name: str,
    schema_section: dict,
    classified_document: dict,
    extracted_fields: dict,
    classified_key: str,
    clean_document: dict,
    model_id: str,
) -> dict:
    sk_prefix = schema_section.get("sk_prefix")
    if not sk_prefix:
        raise ValueError(f"La sezione {schema_section_name} non contiene sk_prefix")

    tipo_documento = sk_prefix.split("#", 1)[1] if "#" in sk_prefix else sk_prefix
    document_uuid = uuid.uuid5(uuid.NAMESPACE_URL, f"{id_pratica}:{classified_key}")

    item = {
        "PK": f"PRATICA#{id_pratica}",
        "SK": f"{sk_prefix}#{document_uuid}",
        "tipo_documento": tipo_documento,
        "schema_section": schema_section_name,
        "documentType": classified_document.get("documentType"),
        "sourceClassifiedKey": classified_key,
        "sourceCleanKey": classified_document.get("sourceCleanKey"),
        "sourceFile": clean_document.get("sourceFile"),
        "sourceRawConsolidatedKey": clean_document.get("sourceRawConsolidatedKey"),
        "modelId": model_id,
        "extractedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    item.update(extracted_fields)
    return item


# ---------------------------------------------------------------------------
# Servizi ingestor trigger
# ---------------------------------------------------------------------------

def _decreto_has_date_soglie(document_items: list[dict]) -> bool:
    """
    Verifica che almeno un item decreto_ricostruzione abbia sia
    data_decorrenza_giuridica (o data_immissione_in_ruolo) sia
    data_conferma_in_ruolo valorizzati.
    """
    for item in document_items:
        if item.get("tipo_documento") != "decreto_ricostruzione":
            continue
        dati = item.get("dati_professionali") or {}
        has_decorrenza = bool(
            dati.get("data_decorrenza_giuridica") or dati.get("data_immissione_in_ruolo")
        )
        has_conferma = bool(dati.get("data_conferma_in_ruolo"))
        if has_decorrenza and has_conferma:
            return True
    return False


def _servizi_already_ingested(id_pratica: str, dest_table_name: str) -> bool:
    """
    Controlla se esistono già item SERVIZIO_PRERUOLO# o SERVIZIO_INRUOLO#
    nella tabella Servizi_Certificati. Evita di triggerare ingestion duplicate.
    """
    dest_table = dynamodb.Table(dest_table_name)
    for sk_prefix in ("SERVIZIO_PRERUOLO#", "SERVIZIO_INRUOLO#"):
        response = dest_table.query(
            KeyConditionExpression=(
                Key("PK").eq(f"PRATICA#{id_pratica}")
                & Key("SK").begins_with(sk_prefix)
            ),
            Select="COUNT",
            Limit=1,
        )
        if response.get("Count", 0) > 0:
            return True
    return False


def _check_and_trigger_servizi_ingestor(
    id_pratica: str,
    just_written_tipo_documento: str,
    updated_metadata_state: dict,
    document_items: list[dict],
) -> bool:
    """
    Valuta se le precondizioni per l'ingestion dei servizi sono soddisfatte
    e, in caso affermativo, invoca la lambda servizi_ingestor in modalità
    asincrona (Event).

    Precondizioni:
      1. Il documento appena scritto è rilevante (documento servizi o decreto).
      2. Il metadata state contiene una fonte_documento_servizi valida.
      3. Esiste almeno un documento servizi del tipo corretto tra gli item estratti.
      4. Esiste almeno un decreto_ricostruzione con entrambe le date soglia.
      5. I servizi non sono già stati ingestati (idempotenza).

    Restituisce True se il trigger è stato inviato, False altrimenti.
    """
    ingestor_function_name = os.environ.get("SERVIZI_INGESTOR_FUNCTION_NAME")
    if not ingestor_function_name:
        # La variabile non è configurata: la funzionalità è disabilitata.
        logger.info(
            "servizi_ingestor_trigger_skipped id_pratica=%s reason=SERVIZI_INGESTOR_FUNCTION_NAME_not_set",
            id_pratica,
        )
        return False

    # Precondizione 1: il documento appena scritto è rilevante.
    if just_written_tipo_documento not in SERVIZI_TRIGGER_DOCUMENT_TYPES:
        logger.info(
            "servizi_ingestor_trigger_skipped id_pratica=%s reason=document_type_not_relevant tipo_documento=%s",
            id_pratica,
            just_written_tipo_documento,
        )
        return False

    # Precondizione 2: fonte_documento_servizi valorizzata nel metadata.
    fonte = updated_metadata_state.get("fonte_documento_servizi")
    if fonte not in ALTERNATIVE_SERVICE_DOCUMENT_TYPES:
        logger.info(
            "servizi_ingestor_trigger_skipped id_pratica=%s reason=fonte_documento_servizi_missing_or_invalid fonte=%s",
            id_pratica,
            fonte,
        )
        return False

    # Precondizione 3: documento servizi del tipo corretto già presente.
    present_service_docs = [
        item for item in document_items
        if item.get("tipo_documento") == fonte
    ]
    if not present_service_docs:
        logger.info(
            "servizi_ingestor_trigger_skipped id_pratica=%s reason=service_document_not_yet_extracted fonte=%s",
            id_pratica,
            fonte,
        )
        return False

    # Precondizione 4: decreto_ricostruzione con date soglia presente.
    if not _decreto_has_date_soglie(document_items):
        logger.info(
            "servizi_ingestor_trigger_skipped id_pratica=%s reason=decreto_ricostruzione_date_soglie_missing",
            id_pratica,
        )
        return False

    # Precondizione 5: idempotenza — i servizi non sono già stati ingestati.
    dest_table_name = os.environ.get("SERVIZI_DEST_TABLE", "Servizi_Certificati")
    try:
        if _servizi_already_ingested(id_pratica, dest_table_name):
            logger.info(
                "servizi_ingestor_trigger_skipped id_pratica=%s reason=servizi_already_ingested",
                id_pratica,
            )
            return False
    except Exception as exc:  # noqa: BLE001
        # Non blocchiamo l'extractor per un errore di check idempotenza.
        logger.warning(
            "servizi_ingestor_idempotency_check_failed id_pratica=%s error=%s — proceeding with trigger",
            id_pratica,
            exc,
        )

    # Tutte le precondizioni soddisfatte: invocazione asincrona.
    payload = {
        "id_pratica": id_pratica,
        "fonte_documento": fonte,
        # Passiamo il trigger_source per tracciabilità nei log dell'ingestor.
        "trigger_source": "entity_extractor",
        "triggered_by_tipo_documento": just_written_tipo_documento,
    }

    try:
        lambda_client.invoke(
            FunctionName=ingestor_function_name,
            InvocationType="Event",  # asincrono: fire-and-forget
            Payload=json.dumps(payload).encode(),
        )
        logger.info(
            "servizi_ingestor_trigger_sent id_pratica=%s fonte=%s ingestor=%s payload=%s",
            id_pratica,
            fonte,
            ingestor_function_name,
            json.dumps(payload, ensure_ascii=False),
        )
        return True
    except ClientError as exc:
        # Non propaghiamo: l'extractor ha già completato il suo lavoro.
        logger.error(
            "servizi_ingestor_trigger_failed id_pratica=%s error=%s",
            id_pratica,
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    logger.info("entity_extractor_event_received event=%s", json.dumps(event))

    classified_prefix = os.environ.get("CLASSIFIED_PREFIX", "classified").strip("/")
    model_id = os.environ.get("EXTRACTOR_MODEL_ID", "eu.amazon.nova-pro-v1:0")
    category_model_id = os.environ.get("CATEGORY_CLASSIFIER_MODEL_ID", model_id)
    extractor_function_name = (
        getattr(context, "function_name", None)
        or os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
        or os.environ.get("ENTITY_EXTRACTOR_FUNCTION_NAME")
    )
    table = get_table()

    saved_items = []
    skipped = []

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            skipped.append({"reason": "non_s3_record"})
            continue

        bucket = record["s3"]["bucket"]["name"]
        classified_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        logger.info("entity_extractor_record bucket=%s key=%s", bucket, classified_key)

        parsed = parse_classified_key(classified_key, classified_prefix)
        if not parsed:
            logger.info("entity_extractor_skip key=%s reason=invalid_classified_key", classified_key)
            skipped.append({"key": classified_key, "reason": "invalid_classified_key"})
            continue

        classified_document = load_json_object(bucket, classified_key)
        if not classified_document:
            logger.info("entity_extractor_skip key=%s reason=missing_classified_document", classified_key)
            skipped.append({"key": classified_key, "reason": "missing_classified_document"})
            continue

        document_type = classified_document.get("documentType")
        if classified_document.get("discard") or document_type == "NON_CLASSIFICATO":
            logger.info("entity_extractor_skip key=%s reason=discarded_document document_type=%s", classified_key, document_type)
            skipped.append({"key": classified_key, "reason": "discarded_document", "documentType": document_type})
            continue

        source_clean_key = classified_document.get("sourceCleanKey")
        if not source_clean_key:
            logger.info("entity_extractor_skip key=%s reason=missing_source_clean_key", classified_key)
            skipped.append({"key": classified_key, "reason": "missing_source_clean_key"})
            continue

        clean_document = load_json_object(bucket, source_clean_key)
        if not clean_document:
            logger.info("entity_extractor_skip key=%s reason=missing_clean_document source_clean_key=%s", classified_key, source_clean_key)
            skipped.append({"key": classified_key, "reason": "missing_clean_document", "sourceCleanKey": source_clean_key})
            continue

        id_pratica = resolve_id_pratica(classified_document, clean_document)
        pratica_metadata = load_pratica_metadata(table, id_pratica)
        categoria_personale = normalize_categoria_personale(pratica_metadata.get("categoria_personale"))
        if categoria_personale:
            pratica_metadata = {**pratica_metadata, "categoria_personale": categoria_personale}
        categoria_just_determined = False
        categoria_result = None

        if not categoria_personale:
            if not _is_decreto_ricostruzione_document(document_type):
                logger.info(
                    "entity_extractor_skip key=%s reason=categoria_personale_not_available document_type=%s",
                    classified_key,
                    document_type,
                )
                skipped.append(
                    {
                        "key": classified_key,
                        "reason": "categoria_personale_not_available",
                        "documentType": document_type,
                    }
                )
                continue

            try:
                categoria_result = classify_categoria_personale_with_model(
                    model_id=category_model_id,
                    clean_document=clean_document,
                    classified_document=classified_document,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "categoria_personale_classification_failed key=%s error=%s",
                    classified_key,
                    exc,
                )
                skipped.append(
                    {
                        "key": classified_key,
                        "reason": "categoria_personale_classification_failed",
                        "documentType": document_type,
                        "error": str(exc),
                    }
                )
                continue

            categoria_personale = normalize_categoria_personale(categoria_result.get("categoria_personale"))
            if not categoria_personale or categoria_result.get("confidence") == "LOW":
                logger.info(
                    "entity_extractor_skip key=%s reason=categoria_personale_not_determined result=%s",
                    classified_key,
                    json.dumps(categoria_result, ensure_ascii=False),
                )
                skipped.append(
                    {
                        "key": classified_key,
                        "reason": "categoria_personale_not_determined",
                        "documentType": document_type,
                        "categoriaResult": categoria_result,
                    }
                )
                continue

            save_categoria_personale_metadata(table, id_pratica, categoria_result)
            pratica_metadata = {**pratica_metadata, "categoria_personale": categoria_personale}
            categoria_just_determined = True

        schema = load_schema_for_category(categoria_personale)

        schema_section_name, schema_section = resolve_schema_section(document_type, schema, pratica_metadata)
        if not schema_section:
            logger.info("entity_extractor_skip key=%s reason=unsupported_or_ambiguous_document_type document_type=%s", classified_key, document_type)
            skipped.append(
                {
                    "key": classified_key,
                    "reason": "unsupported_or_ambiguous_document_type",
                    "documentType": document_type,
                    "categoria_personale": categoria_personale,
                    "schemaFile": schema_filename_for_categoria(categoria_personale),
                }
            )
            continue

        existing_item = find_existing_document_item(table, id_pratica, classified_key)
        if existing_item:
            logger.info("entity_extractor_skip key=%s reason=document_already_extracted existing_sk=%s", classified_key, existing_item.get("SK"))
            pending_classified_triggered = 0
            if categoria_just_determined:
                classified_keys = list_classified_keys_for_pratica(
                    bucket,
                    classified_prefix,
                    id_pratica,
                    current_key=classified_key,
                )
                pending_classified_triggered = trigger_extraction_for_classified_keys(
                    function_name=extractor_function_name,
                    bucket=bucket,
                    classified_keys=classified_keys,
                    current_key=classified_key,
                )
            skipped.append(
                {
                    "key": classified_key,
                    "reason": "document_already_extracted",
                    "existingSk": existing_item.get("SK"),
                    "pendingClassifiedTriggered": pending_classified_triggered,
                }
            )
            continue

        extraction_view = build_extraction_view(clean_document, classified_document)
        logger.info(
            "entity_extractor_input_ready key=%s document_type=%s schema_section=%s pages=%s forms_pages=%s tables_pages=%s",
            classified_key,
            document_type,
            schema_section_name,
            len(extraction_view["pagesText"]),
            len(extraction_view["formsByPage"]),
            len(extraction_view["tablesByPage"]),
        )

        if _is_service_document_section(schema_section):
            raw_extracted = {}
            try:
                logger.info("entity_extractor_service_specialization_start key=%s schema_section=%s", classified_key, schema_section_name)
                raw_extracted = _merge_specialized_service_extraction(
                    model_id=model_id,
                    schema_section_name=schema_section_name,
                    schema_section=schema_section,
                    extraction_view=extraction_view,
                    raw_extracted={},
                )
                logger.info("entity_extractor_service_specialization_done key=%s schema_section=%s", classified_key, schema_section_name)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "entity_extractor_service_specialization_failed key=%s schema_section=%s error=%s",
                    classified_key,
                    schema_section_name,
                    exc,
                )
                try:
                    raw_extracted = _merge_table_absences_into_raw_extracted(
                        schema_section=schema_section,
                        extraction_view=extraction_view,
                        raw_extracted=raw_extracted,
                    )
                    logger.info(
                        "entity_extractor_service_table_fallback_done key=%s schema_section=%s",
                        classified_key,
                        schema_section_name,
                    )
                except Exception as fallback_exc:  # noqa: BLE001
                    logger.warning(
                        "entity_extractor_service_table_fallback_failed key=%s schema_section=%s error=%s",
                        classified_key,
                        schema_section_name,
                        fallback_exc,
                    )
        else:
            logger.info("entity_extractor_bedrock_call_start key=%s model_id=%s", classified_key, model_id)
            raw_extracted = extract_entities_with_model(model_id, schema_section_name, schema_section, extraction_view)
            logger.info("entity_extractor_bedrock_call_done key=%s", classified_key)

        extracted_fields = normalize_against_schema(schema_section.get("campi", {}), raw_extracted)
        extracted_fields = _override_decreto_preruolo_from_tables(
            schema_section=schema_section,
            extraction_view=extraction_view,
            extracted_fields=extracted_fields,
        )
        if _is_service_document_section(schema_section):
            _log_service_debug_snapshot(
                "normalized_fields",
                mergeLogicVersion=SERVICE_MERGE_LOGIC_VERSION,
                key=classified_key,
                schemaSection=schema_section_name,
                extractedFields=extracted_fields,
            )
        logger.info("entity_extractor_normalized key=%s schema_section=%s", classified_key, schema_section_name)

        ensure_metadata_item(table, id_pratica)
        item = build_dynamo_item(
            id_pratica=id_pratica,
            schema_section_name=schema_section_name,
            schema_section=schema_section,
            classified_document=classified_document,
            extracted_fields=extracted_fields,
            classified_key=classified_key,
            clean_document=clean_document,
            model_id=model_id,
        )
        table.put_item(Item=item)
        logger.info("entity_extractor_dynamo_write_done pk=%s sk=%s", item["PK"], item["SK"])

        # Aggiorna il metadata DOPO aver scritto il documento, così il metadata
        # riflette lo stato aggiornato (incluso il documento appena scritto).
        metadata_state = refresh_metadata_from_documents(table, id_pratica, pratica_metadata)

        # ------------------------------------------------------------------ #
        # Valuta se triggerare la servizi_ingestor.                           #
        # Viene chiamata DOPO refresh_metadata, quindi document_items è già   #
        # aggiornato con l'item appena scritto.                               #
        # Usiamo query_document_items per avere la lista fresca.              #
        # ------------------------------------------------------------------ #
        sk_prefix = schema_section.get("sk_prefix", "")
        just_written_tipo_documento = sk_prefix.split("#", 1)[1] if "#" in sk_prefix else sk_prefix

        document_items_fresh = query_document_items(table, id_pratica)
        trigger_sent = _check_and_trigger_servizi_ingestor(
            id_pratica=id_pratica,
            just_written_tipo_documento=just_written_tipo_documento,
            updated_metadata_state=metadata_state,
            document_items=document_items_fresh,
        )
        pending_classified_triggered = 0
        if categoria_just_determined:
            classified_keys = list_classified_keys_for_pratica(
                bucket,
                classified_prefix,
                id_pratica,
                current_key=classified_key,
            )
            pending_classified_triggered = trigger_extraction_for_classified_keys(
                function_name=extractor_function_name,
                bucket=bucket,
                classified_keys=classified_keys,
                current_key=classified_key,
            )

        saved_items.append(
            {
                "PK": item["PK"],
                "SK": item["SK"],
                "documentType": document_type,
                "schemaSection": schema_section_name,
                "categoriaPersonale": categoria_personale,
                "schemaFile": schema_filename_for_categoria(categoria_personale),
                "metadataState": metadata_state,
                "serviziIngestorTriggered": trigger_sent,
                "pendingClassifiedTriggered": pending_classified_triggered,
            }
        )

    logger.info(
        "entity_extractor_completed saved_items=%s skipped=%s",
        json.dumps(saved_items, ensure_ascii=False),
        json.dumps(skipped, ensure_ascii=False),
    )
    return {"savedItems": saved_items, "skipped": skipped}
