import json
import logging
import os
import re
import unicodedata
import urllib.parse
import uuid
from datetime import datetime, timezone
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
sfn_client = boto3.client("stepfunctions", region_name=os.environ.get("AWS_REGION", "eu-central-1"))

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


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def _schema_candidates(schema_filename: str = "rts_schema.json") -> list[Path]:
    if schema_filename == "rts_schema_ata.json":
        env_var = "RTS_SCHEMA_ATA_PATH"
    elif schema_filename == "schema_stipendi.json":
        env_var = "RTS_SCHEMA_STIPENDI_PATH"
    else:
        env_var = "RTS_SCHEMA_PATH"
    
    env_path = os.environ.get(env_var)
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))

    base_dir = Path(__file__).parent
    # Per schema_stipendi.json, cerca nella cartella schemas al livello superiore
    if schema_filename == "schema_stipendi.json":
        candidates.extend(
            [
                base_dir.parent / "schemas" / schema_filename,
                base_dir / schema_filename,
            ]
        )
    else:
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


def schema_filename_for_categoria(categoria_personale: str | None, tipo_servizio: str | None = None) -> str:
    # Se il tipo_servizio è "stipendi", usa sempre lo schema stipendi
    if tipo_servizio == "stipendi":
        return "schema_stipendi.json"
    
    # Altrimenti usa la logica classica per il flusso controlli
    return "rts_schema_ata.json" if categoria_personale == "ATA" else "rts_schema.json"


def load_schema_for_category(categoria_personale: str, tipo_servizio: str | None = None) -> dict:
    return load_schema(schema_filename_for_categoria(categoria_personale, tipo_servizio))


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


SERVICE_DOCUMENT_PAYLOAD_FIELDS = {"servizi"}


def schema_section_for_entity_extraction(schema_section: dict) -> tuple[dict, set[str]]:
    if not _is_service_document_section(schema_section):
        return schema_section, set()

    campi = schema_section.get("campi", {})
    if not isinstance(campi, dict):
        return schema_section, set()

    removed_fields = set(campi) & SERVICE_DOCUMENT_PAYLOAD_FIELDS
    if not removed_fields:
        return schema_section, set()

    extraction_section = dict(schema_section)
    extraction_section["campi"] = {
        field_name: field_schema
        for field_name, field_schema in campi.items()
        if field_name not in removed_fields
    }
    return extraction_section, removed_fields


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
    return _apply_equivalent_date_fields(normalized)


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
    just_written_item: dict | None = None,
    bucket: str | None = None,
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

    source_document_item = None
    if (
        isinstance(just_written_item, dict)
        and just_written_item.get("tipo_documento") == fonte
    ):
        source_document_item = just_written_item
    else:
        present_service_docs.sort(key=lambda item: item.get("extractedAt") or "", reverse=True)
        source_document_item = present_service_docs[0]

    # Tutte le precondizioni soddisfatte: invocazione asincrona.
    payload = {
        "id_pratica": id_pratica,
        "fonte_documento": fonte,
        "trigger_source": "entity_extractor",
        "triggered_by_tipo_documento": just_written_tipo_documento,
        "bucket": bucket,
        "source_documento_sk": source_document_item.get("SK") if source_document_item else None,
        "source_classified_key": source_document_item.get("sourceClassifiedKey") if source_document_item else None,
        "source_clean_key": source_document_item.get("sourceCleanKey") if source_document_item else None,
        "categoria_personale": updated_metadata_state.get("categoria_personale"),
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
# Checklist Step Functions trigger
# ---------------------------------------------------------------------------

# Mapping categoria_personale → tipo_checklist usato dall'orchestratore
_CHECKLIST_TYPE_BY_CATEGORIA: dict[str, str] = {
    "docente": "ricostruzione_carriera_docenti",
    "ATA":     "ricostruzione_carriera_ata",
}


def _check_and_trigger_checklist_sfn(
    id_pratica: str,
    updated_metadata_state: dict,
    document_items: list[dict],
    documenti_attesi: int,
) -> bool:
    """
    Avvia la Step Functions dei controlli checklist quando tutti i documenti
    caricati per la pratica sono stati estratti.

    Precondizioni:
      1. Variabile d'ambiente CHECKLIST_SFN_ARN configurata.
      2. documenti_attesi > 0 e len(document_items) >= documenti_attesi
         (tutti i documenti caricati dal FE sono stati estratti).
      3. categoria_personale nota (serve per scegliere la checklist corretta).
      4. La Step Functions non è già stata avviata (flag checklist_sfn_triggered
         salvato su DynamoDB per idempotenza).

    Restituisce True se l'esecuzione è stata avviata, False altrimenti.
    """
    sfn_arn = os.environ.get("CHECKLIST_SFN_ARN")
    if not sfn_arn:
        logger.info(
            "checklist_sfn_trigger_skipped id_pratica=%s reason=CHECKLIST_SFN_ARN_not_set",
            id_pratica,
        )
        return False

    # Precondizione 2: tutti i documenti caricati sono stati estratti.
    estratti = len(document_items)
    if documenti_attesi <= 0 or estratti < documenti_attesi:
        logger.info(
            "checklist_sfn_trigger_skipped id_pratica=%s reason=extraction_incomplete "
            "estratti=%s documenti_attesi=%s",
            id_pratica,
            estratti,
            documenti_attesi,
        )
        return False

    # Precondizione 3: categoria_personale nota → seleziona tipo_checklist.
    categoria_personale = updated_metadata_state.get("categoria_personale", "")
    tipo_checklist = os.environ.get(
        "CHECKLIST_TYPE",
        _CHECKLIST_TYPE_BY_CATEGORIA.get(categoria_personale, ""),
    )
    if not tipo_checklist:
        logger.warning(
            "checklist_sfn_trigger_skipped id_pratica=%s reason=tipo_checklist_not_determinable "
            "categoria_personale=%s",
            id_pratica,
            categoria_personale,
        )
        return False

    # Precondizione 4: idempotenza — non riavviare se già triggerato.
    pratiche_table_name = os.environ.get("PRATICHE_TABLE") or os.environ.get("DYNAMO_TABLE")
    if pratiche_table_name:
        try:
            tbl = dynamodb.Table(pratiche_table_name)
            resp = tbl.get_item(Key={"PK": f"PRATICA#{id_pratica}", "SK": "METADATA"})
            if resp.get("Item", {}).get("checklist_sfn_triggered"):
                logger.info(
                    "checklist_sfn_trigger_skipped id_pratica=%s reason=already_triggered",
                    id_pratica,
                )
                return False
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "checklist_sfn_idempotency_check_failed id_pratica=%s error=%s — proceeding with trigger",
                id_pratica,
                exc,
            )

    # Tutte le precondizioni soddisfatte: avvio asincrono della Step Functions.
    import hashlib
    execution_name = (
        f"{id_pratica[:40]}-"
        + hashlib.md5(id_pratica.encode()).hexdigest()[:8]  # noqa: S324
    )

    payload = {
        "id_pratica":     id_pratica,
        "tipo_checklist": tipo_checklist,
    }

    try:
        sfn_client.start_execution(
            stateMachineArn=sfn_arn,
            name=execution_name,
            input=json.dumps(payload, ensure_ascii=False),
        )
        logger.info(
            "checklist_sfn_trigger_sent id_pratica=%s tipo_checklist=%s execution_name=%s sfn_arn=%s",
            id_pratica,
            tipo_checklist,
            execution_name,
            sfn_arn,
        )
    except sfn_client.exceptions.ExecutionAlreadyExists:
        logger.info(
            "checklist_sfn_trigger_skipped id_pratica=%s reason=execution_already_exists execution_name=%s",
            id_pratica,
            execution_name,
        )
        return False
    except ClientError as exc:
        logger.error(
            "checklist_sfn_trigger_failed id_pratica=%s error=%s",
            id_pratica,
            exc,
        )
        return False

    # Salva il flag di idempotenza su DynamoDB.
    if pratiche_table_name:
        try:
            tbl = dynamodb.Table(pratiche_table_name)
            tbl.update_item(
                Key={"PK": f"PRATICA#{id_pratica}", "SK": "METADATA"},
                UpdateExpression="SET checklist_sfn_triggered = :v, checklist_sfn_execution = :e",
                ExpressionAttributeValues={
                    ":v": True,
                    ":e": execution_name,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "checklist_sfn_flag_write_failed id_pratica=%s error=%s",
                id_pratica,
                exc,
            )

    return True


def _check_and_trigger_genera_prospetto(
    bucket: str,
    id_pratica: str,
    tipo_servizio: str | None,
    document_items: list[dict],
) -> bool:
    """
    Verifica se per il servizio STIPENDI esistono sia decreto_ricostruzione che visto.
    Se entrambi presenti, scrive un file trigger nel bucket per avviare genera_prospetto.
    
    Args:
        bucket: Nome del bucket S3
        id_pratica: ID della pratica
        tipo_servizio: Tipo di servizio (es: "stipendi")
        document_items: Lista di tutti i documenti della pratica
    
    Returns:
        True se il trigger è stato creato, False altrimenti
    """
    # Solo per il servizio stipendi
    if tipo_servizio != "stipendi":
        return False
    
    # Verifica che ci siano sia decreto che visto
    has_decreto = False
    has_visto = False
    
    for doc in document_items:
        sk = doc.get("SK", "")
        if sk.startswith("DOCUMENTO#decreto_ricostruzione#"):
            has_decreto = True
        elif sk.startswith("DOCUMENTO#visto#"):
            has_visto = True
    
    # Se manca uno dei due, non fare nulla
    if not (has_decreto and has_visto):
        logger.info(
            "genera_prospetto_trigger_skip id_pratica=%s has_decreto=%s has_visto=%s",
            id_pratica,
            has_decreto,
            has_visto,
        )
        return False
    
    # Scrivi il file trigger nel bucket
    trigger_key = f"output/ready/stipendi/{id_pratica}/_trigger.json"
    
    try:
        # Crea un file JSON con informazioni sulla pratica
        trigger_content = json.dumps(
            {
                "id_pratica": id_pratica,
                "tipo_servizio": tipo_servizio,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "trigger_type": "genera_prospetto",
            },
            ensure_ascii=False,
            indent=2,
        )
        
        s3.put_object(
            Bucket=bucket,
            Key=trigger_key,
            Body=trigger_content,
            ContentType="application/json",
        )
        
        logger.info(
            "genera_prospetto_trigger_created id_pratica=%s bucket=%s key=%s",
            id_pratica,
            bucket,
            trigger_key,
        )
        return True
        
    except ClientError as exc:
        logger.error(
            "genera_prospetto_trigger_failed id_pratica=%s error=%s",
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

        # Estrai tipo_flusso dal path (es: stipendi, controlli)
        tipo_flusso_from_path = parsed.get("tipo_flusso")

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

        # Estrai tipo_servizio dai metadata, con fallback dal path
        tipo_servizio = pratica_metadata.get("tipo_servizio") or tipo_flusso_from_path
        schema = load_schema_for_category(categoria_personale, tipo_servizio)

        schema_section_name, schema_section = resolve_schema_section(document_type, schema, pratica_metadata)
        if not schema_section:
            logger.info(
                "entity_extractor_skip key=%s reason=unsupported_or_ambiguous_document_type document_type=%s tipo_servizio=%s schema_file=%s",
                classified_key,
                document_type,
                tipo_servizio,
                schema_filename_for_categoria(categoria_personale, tipo_servizio),
            )
            skipped.append(
                {
                    "key": classified_key,
                    "reason": "unsupported_or_ambiguous_document_type",
                    "documentType": document_type,
                    "categoria_personale": categoria_personale,
                    "tipo_servizio": tipo_servizio,
                    "schemaFile": schema_filename_for_categoria(categoria_personale, tipo_servizio),
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

        extraction_schema_section, delegated_fields = schema_section_for_entity_extraction(schema_section)
        if delegated_fields:
            logger.info(
                "entity_extractor_service_fields_delegated key=%s schema_section=%s fields=%s owner=lambda_function_services",
                classified_key,
                schema_section_name,
                sorted(delegated_fields),
            )

        logger.info("entity_extractor_bedrock_call_start key=%s model_id=%s", classified_key, model_id)
        raw_extracted = extract_entities_with_model(
            model_id,
            schema_section_name,
            extraction_schema_section,
            extraction_view,
        )
        logger.info("entity_extractor_bedrock_call_done key=%s", classified_key)

        extracted_fields = normalize_against_schema(extraction_schema_section.get("campi", {}), raw_extracted)
        if not _is_service_document_section(schema_section):
            extracted_fields = _override_decreto_preruolo_from_tables(
                schema_section=schema_section,
                extraction_view=extraction_view,
                extracted_fields=extracted_fields,
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
        if _is_service_document_section(schema_section):
            item["serviceExtractionOwner"] = "lambda_function_services"
            item["serviceExtractionStatus"] = "PENDING"
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
            just_written_item=item,
            bucket=bucket,
        )
        documenti_attesi = int((pratica_metadata or {}).get("documenti_attesi", 0))
        checklist_sfn_triggered = _check_and_trigger_checklist_sfn(
            id_pratica=id_pratica,
            updated_metadata_state=metadata_state,
            document_items=document_items_fresh,
            documenti_attesi=documenti_attesi,
        )
        # ------------------------------------------------------------------ #
        # Valuta se triggerare genera_prospetto per il servizio stipendi     #
        # Solo se ci sono sia decreto_ricostruzione che visto                #
        # ------------------------------------------------------------------ #
        genera_prospetto_triggered = _check_and_trigger_genera_prospetto(
            bucket=bucket,
            id_pratica=id_pratica,
            tipo_servizio=tipo_servizio,
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
                "tipoServizio": tipo_servizio,
                "schemaFile": schema_filename_for_categoria(categoria_personale, tipo_servizio),
                "metadataState": metadata_state,
                "serviziIngestorTriggered": trigger_sent,
                "checklistSfnTriggered": checklist_sfn_triggered,
                "generaProspettoTriggered": genera_prospetto_triggered,
                "pendingClassifiedTriggered": pending_classified_triggered,
            }
        )

    logger.info(
        "entity_extractor_completed saved_items=%s skipped=%s",
        json.dumps(saved_items, ensure_ascii=False),
        json.dumps(skipped, ensure_ascii=False),
    )
    return {"savedItems": saved_items, "skipped": skipped}
