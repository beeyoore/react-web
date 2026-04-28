"""
Lambda: genera_prospetto
GET /pratica/{id_pratica}/prospetto

Compila il template Excel del "Prospetto di applicazione del decreto di
ricostruzione carriera" sostituendo i placeholder [Campo] con i dati
estratti da DynamoDB (METADATA + DOCUMENTO#decreto_ricostruzione#...).

Risposta 200:
{
  "url": "https://s3.amazonaws.com/..."   ← presigned URL download (GET)
}

Struttura DynamoDB attesa per item decreto_ricostruzione:
  PK: "PRATICA#{id_pratica}"
  SK: "DOCUMENTO#decreto_ricostruzione#{uuid}"
  tipo_documento: "decreto_ricostruzione"

  dati_anagrafici:
    codice_fiscale, data_nascita, nome_cognome (o nome + cognome separati)

  dati_professionali:
    qualifica_funzionale
    data_immissione_in_ruolo  (= data_decorrenza_giuridica)
    data_decorrenza_giuridica
    data_decorrenza_economica (= data_assunzione_in_servizio)
    data_assunzione_in_servizio
    data_conferma_in_ruolo
    classe_stipendiale
    data_scadenza_stipendi

  intestazione:
    numero_decreto, data_decreto, istituto_amministrante

  articolo_2:
    classe_stipendiale, data_scadenza
    periodo_totale_fini_giuridici_economici  (es. "X anni Y mesi Z giorni")
    periodo_totale_soli_fini_economici

  articolo_4 (o lista assegni):
    codice_assegno, importo_assegno_ad_personam, numero_mensilita
    data_decorrenza_assegno, data_scadenza_assegno

  visti (lista o dict):
    numero_visto, data_visto

  assenze (lista):
    data_inizio (o periodo_fruizione_inizio), data_fine (o periodo_fruizione_fine)
    tipologia_assenza

  METADATA:
    prescrizione: "Sì" / "No"
    data_prescrizione

Env vars:
  DYNAMODB_TABLE       – default: RTS_Pratiche
  REGION               – default: eu-central-1
  TEMPLATE_BUCKET      – bucket S3 con template Excel  (OBBLIGATORIO)
  TEMPLATE_KEY         – key S3 del template           (default: templates/prospetto_ricostruzione.xlsx)
  OUTPUT_BUCKET        – bucket S3 output              (default: TEMPLATE_BUCKET)
  OUTPUT_PREFIX        – prefix S3 output              (default: prospetti)
  PRESIGNED_URL_EXPIRY – secondi validità URL          (default: 3600)
"""

import io
import json
import logging
import os
import re

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from openpyxl import load_workbook

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("REGION", "eu-central-1")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "RTS_Pratiche")
TEMPLATE_BUCKET = os.environ["TEMPLATE_BUCKET"]
TEMPLATE_KEY = os.environ.get("TEMPLATE_KEY", "templates/prospetto_ricostruzione.xlsx")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", TEMPLATE_BUCKET)
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "prospetti")
PRESIGNED_URL_EXPIRY = int(os.environ.get("PRESIGNED_URL_EXPIRY", "3600"))

dynamodb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------------
# Mapping qualifica funzionale → codice NoiPA
# ---------------------------------------------------------------------------

QUALIFICA_NOIIPA_MAP = {
    # Docenti
    "docente scuola materna": "KA05",
    "docente scuola elementare": "KA05",
    "docente scuola infanzia": "KA05",
    "docente primaria": "KA05",
    "docente scuola media": "KA07",
    "docente scuola secondaria di i grado": "KA07",
    "docente diplomato istituti secondari": "KA06",
    "docente diplomato secondaria ii grado": "KA06",
    "docente laureato istituti secondari": "KA08",
    "docente laureato secondaria ii grado": "KA08",
    "docente istituti secondari ii grado": "KA06",
    # ATA
    "collaboratore scolastico": "KA41",
    "collaboratori scolastici": "KA41",
    "operatore scolastico": "KA42",
    "assistente amministrativo": "KA43",
    "assistente tecnico": "KA43",
    "direttore dei servizi generali e amministrativi": "KA12",
    "dsga": "KA12",
    # Capi istituto
    "capo di istituto": "KA11",
    "dirigente scolastico": "KA12",
    # AFAM
    "afam operatore": "KC41",
    "afam assistente": "KC43",
    "afam funzionario": "KC17",
    "afam elevata qualificazione": "KC16",
}


def _noiipa_qualifica(qualifica: str | None) -> str:
    if not qualifica:
        return ""
    lower = qualifica.lower().strip()
    for pattern, code in QUALIFICA_NOIIPA_MAP.items():
        if pattern in lower:
            return f"{code} - {qualifica}"
    return qualifica


# ---------------------------------------------------------------------------
# Helpers DynamoDB
# ---------------------------------------------------------------------------

def _get(d: dict | None, *keys, default=""):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d if d is not None else default


def _first(*values, default=""):
    for v in values:
        if v:
            return v
    return default


def query_metadata(id_pratica: str) -> dict:
    table = dynamodb.Table(DYNAMODB_TABLE)
    resp = table.get_item(Key={"PK": f"PRATICA#{id_pratica}", "SK": "METADATA"})
    return resp.get("Item") or {}


def query_documento_decreto(id_pratica: str) -> dict | None:
    """Restituisce il primo item decreto_ricostruzione trovato."""
    table = dynamodb.Table(DYNAMODB_TABLE)
    kwargs = {
        "KeyConditionExpression": (
            Key("PK").eq(f"PRATICA#{id_pratica}")
            & Key("SK").begins_with("DOCUMENTO#decreto_ricostruzione#")
        )
    }
    items = []
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last
    # Prefer the most recent extraction
    if not items:
        return None
    items.sort(key=lambda x: x.get("extractedAt", ""), reverse=True)
    return items[0]


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------

def _extract_nome_cognome(decreto: dict) -> tuple[str, str]:
    """
    Restituisce (cognome, nome) dall'item decreto.
    Cerca prima campi separati, poi divide nome_cognome (formato COGNOME NOME).
    """
    anag = decreto.get("dati_anagrafici") or {}
    cognome = _get(anag, "cognome") or _get(decreto, "cognome")
    nome = _get(anag, "nome") or _get(decreto, "nome")
    if cognome and nome:
        return str(cognome), str(nome)

    nome_cognome = (
        _get(anag, "nome_cognome")
        or _get(decreto, "nome_cognome")
        or ""
    )
    if nome_cognome:
        parts = nome_cognome.split()
        if len(parts) >= 2:
            # Convenzione italiana: COGNOME NOME
            return parts[0], " ".join(parts[1:])
        return nome_cognome, ""
    return cognome or "", nome or ""


def _format_assenza(a: dict) -> str:
    inizio = (
        a.get("periodo_fruizione_inizio")
        or a.get("data_inizio_assenza")
        or a.get("data_inizio")
        or ""
    )
    fine = (
        a.get("periodo_fruizione_fine")
        or a.get("data_fine_assenza")
        or a.get("data_fine")
        or ""
    )
    tipo = a.get("tipologia_assenza") or a.get("tipo_assenza") or a.get("tipo") or ""
    if inizio and fine:
        base = f"dal {inizio} al {fine}"
    elif inizio:
        base = f"dal {inizio}"
    else:
        return tipo or ""
    return f"{base} ({tipo})" if tipo else base


def _format_anzianita(val) -> str:
    """Formatta un valore di anzianità come 'X anni Y mesi Z giorni' o stringa raw."""
    if isinstance(val, dict):
        anni = val.get("anni", 0) or 0
        mesi = val.get("mesi", 0) or 0
        giorni = val.get("giorni", 0) or 0
        parts = []
        if anni:
            parts.append(f"{anni} ann{'o' if anni == 1 else 'i'}")
        if mesi:
            parts.append(f"{mesi} mes{'e' if mesi == 1 else 'i'}")
        if giorni:
            parts.append(f"{giorni} giorn{'o' if giorni == 1 else 'i'}")
        return ", ".join(parts) if parts else "0 giorni"
    return str(val) if val else ""


def _prox_variazione_automatica_stipendi(classe_stipendiale: str | None) -> str:
    """
    Classe "00" → una sola variazione → flag "Sì".
    Classe diversa → questa riga è quella target → flag "Sì".
    (Le righe precedenti non sono nel template e vanno aggiunte manualmente.)
    """
    return "Sì"


def _soggetto_a_prescrizione(metadata: dict) -> str:
    prescrizione = metadata.get("prescrizione", "No")
    if str(prescrizione).strip().lower() in ("sì", "si", "s", "yes", "true", "1"):
        data = metadata.get("data_prescrizione") or ""
        return f"con prescrizione al {data}".strip() if data else "con prescrizione"
    return ""


# ---------------------------------------------------------------------------
# Placeholder replacement
# ---------------------------------------------------------------------------

def _replace_all(text: str, mapping: dict) -> str:
    """Sostituisce tutti i placeholder [key] nel testo con i valori del mapping."""
    for placeholder, value in mapping.items():
        text = text.replace(f"[{placeholder}]", str(value) if value is not None else "")
    return text


def _build_placeholder_map(decreto: dict, metadata: dict) -> dict:
    anag = decreto.get("dati_anagrafici") or {}
    prof = decreto.get("dati_professionali") or {}
    int_ = decreto.get("intestazione") or {}
    art2 = decreto.get("articolo_2") or {}
    art4 = decreto.get("articolo_4") or {}

    # Supporta sia singolo dict che lista per art4
    if isinstance(art4, list):
        art4 = art4[0] if art4 else {}

    visti = decreto.get("visti") or {}
    if isinstance(visti, list):
        visti = visti[0] if visti else {}

    assenze_raw = decreto.get("assenze") or []

    cognome, nome = _extract_nome_cognome(decreto)

    codice_fiscale = (
        _get(anag, "codice_fiscale")
        or _get(decreto, "codice_fiscale")
        or metadata.get("codice_fiscale", "")
    )
    data_nascita = _get(anag, "data_nascita") or _get(decreto, "data_nascita") or ""

    qualifica_raw = (
        _get(prof, "qualifica_funzionale")
        or _get(prof, "qualifica")
        or _get(decreto, "qualifica_funzionale")
        or ""
    )

    data_nomina_in_ruolo = _first(
        _get(prof, "data_immissione_in_ruolo"),
        _get(prof, "data_decorrenza_giuridica"),
    )
    data_decorrenza_giuridica = _first(
        _get(prof, "data_decorrenza_giuridica"),
        _get(prof, "data_immissione_in_ruolo"),
    )
    data_decorrenza_economica = _first(
        _get(prof, "data_decorrenza_economica"),
        _get(prof, "data_assunzione_in_servizio"),
        _get(art2, "data_decorrenza_economica"),
    )
    classe_stipendiale = (
        _get(art2, "classe_stipendiale")
        or _get(prof, "classe_stipendiale")
        or _get(decreto, "classe_stipendiale")
        or ""
    )
    data_scadenza_stipendi = (
        _get(art2, "data_scadenza")
        or _get(prof, "data_scadenza_stipendi")
        or _get(prof, "data_scadenza")
        or ""
    )
    data_conferma_in_ruolo = (
        _get(prof, "data_conferma_in_ruolo")
        or _get(prof, "data_conferma_ruolo")
        or _get(art2, "data_conferma_in_ruolo")
        or ""
    )

    numero_decreto = _get(int_, "numero_decreto") or _get(decreto, "numero_decreto") or ""
    data_decreto = _get(int_, "data_decreto") or _get(decreto, "data_decreto") or ""
    istituto = (
        _get(int_, "istituto_amministrante")
        or _get(int_, "istituto")
        or _get(decreto, "istituto_amministrante")
        or ""
    )

    codice_assegno = _get(art4, "codice_assegno") or _get(art4, "codice") or ""
    importo_assegno = (
        _get(art4, "importo_assegno_ad_personam")
        or _get(art4, "importo")
        or ""
    )
    numero_mensilita = (
        _get(art4, "numero_mensilita")
        or _get(art4, "natura_importo")
        or ""
    )
    data_decorrenza_assegno = (
        _get(art4, "data_decorrenza")
        or _get(art4, "data_decorrenza_assegno")
        or data_decorrenza_economica
        or ""
    )
    data_scadenza_assegno = (
        _get(art4, "data_scadenza")
        or _get(art4, "data_scadenza_assegno")
        or ""
    )

    numero_visto = _get(visti, "numero_visto") or _get(visti, "numero") or ""
    data_visto = _get(visti, "data_visto") or _get(visti, "data") or ""

    periodo_giur_econ = _format_anzianita(
        _get(art2, "periodo_totale_fini_giuridici_economici")
        or _get(art2, "anzianita_fini_giuridici_economici")
        or _get(decreto, "periodo_totale_fini_giuridici_economici")
    )
    periodo_solo_econ = _format_anzianita(
        _get(art2, "periodo_totale_soli_fini_economici")
        or _get(art2, "anzianita_soli_fini_economici")
        or _get(decreto, "periodo_totale_soli_fini_economici")
    )

    prox_var_stipendi = _prox_variazione_automatica_stipendi(classe_stipendiale)
    soggetto_prescriz = _soggetto_a_prescrizione(metadata)
    qualifica_noiipa = _noiipa_qualifica(qualifica_raw)

    return {
        # Anagrafica
        "Codice Fiscale": codice_fiscale,
        "Data di nascita": data_nascita,
        "Cognome": cognome,
        "Nome": nome,
        # Date professionali
        "Data di nomina in ruolo": data_nomina_in_ruolo,
        "Data di decorrenza giuridica": data_decorrenza_giuridica,
        "Data di decorrenza giuridca": data_decorrenza_giuridica,   # typo nel template originale
        "Data di decorrenza economica": data_decorrenza_economica,
        # [Data di scadenza] in B27 = stipendi, in B34 = assegni → gestiti separatamente
        "Data di scadenza": data_scadenza_stipendi,
        # Stipendi
        "Qualifica professionale": qualifica_noiipa,
        "Classe stipendiale": classe_stipendiale,
        # Assegni
        "Data decorrenza": data_decorrenza_assegno,
        "Codice assegno": codice_assegno,
        "Importo assegno ad personam": importo_assegno,
        "Numero mensilità": numero_mensilita,
        # Annotazioni
        "numero del decreto": numero_decreto,
        "data del decreto": data_decreto,
        "Istituto dell'amministrato": istituto,
        "soggetto a prescrizione": soggetto_prescriz,
        "numero del visto": numero_visto,
        "data del visto": data_visto,
        "Data conferma in ruolo": data_conferma_in_ruolo,
        "Periodo totale anzianità ai fini giuridici ed economici": periodo_giur_econ,
        "Periodo totale anzianità ai soli fini economici": periodo_solo_econ,
        # alias tabella art. 2
        "Periodo totale di anzianità a fini giuridici ed economici": periodo_giur_econ,
        "Periodo totale di anzianità ai soli fini economici": periodo_solo_econ,
        # campi privati: gestiti direttamente in fill_excel
        "_assenze": assenze_raw,
        "_data_scadenza_assegno": data_scadenza_assegno,
        "_prox_var_stipendi": prox_var_stipendi,
    }


def _fill_assenze_in_cell(text: str, assenze: list[dict]) -> str:
    """
    Sostituisce i placeholder '1. [Durata assenza]', '2. [Durata assenza]', '3. [Durata assenza]'
    con i dati reali delle assenze.
    Le voci in eccesso restano vuote (sostituisce col trattino).
    """
    for i in range(1, 20):
        idx = i - 1
        placeholder = f"{i}. [Durata assenza]"
        if placeholder not in text:
            break
        if idx < len(assenze):
            value = f"{i}. {_format_assenza(assenze[idx])}"
        else:
            value = f"{i}."
        text = text.replace(placeholder, value)
    return text


# ---------------------------------------------------------------------------
# Excel template filling
# ---------------------------------------------------------------------------

SHEET_NAME = "Prospetto applicazione"


def fill_excel(template_bytes: bytes, mapping: dict) -> bytes:
    wb = load_workbook(io.BytesIO(template_bytes))

    if SHEET_NAME not in wb.sheetnames:
        raise ValueError(f"Sheet '{SHEET_NAME}' non trovato nel template")

    ws = wb[SHEET_NAME]
    assenze = mapping.pop("_assenze", [])
    data_scadenza_assegno = mapping.pop("_data_scadenza_assegno", "")
    prox_var_stipendi = mapping.pop("_prox_var_stipendi", "Sì")

    # Mapping specifico per B34: [Data di scadenza] = scadenza assegno
    mapping_assegni = dict(mapping)
    if data_scadenza_assegno:
        mapping_assegni["Data di scadenza"] = data_scadenza_assegno

    for row in ws.iter_rows():
        for cell in row:
            if not isinstance(cell.value, str):
                continue

            coord = cell.coordinate
            new_value = cell.value

            # Gestione assenze (B4)
            new_value = _fill_assenze_in_cell(new_value, assenze)

            # B34: variazione assegni → usa scadenza assegno
            if coord == "B34":
                new_value = _replace_all(new_value, mapping_assegni)
            else:
                new_value = _replace_all(new_value, mapping)

            # B27: "Sì/No" è un testo letterale (non tra []), sostituisci con valore calcolato
            if coord == "B27":
                new_value = new_value.replace("Sì/No", prox_var_stipendi)

            if new_value != cell.value:
                cell.value = new_value

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def load_template() -> bytes:
    obj = s3.get_object(Bucket=TEMPLATE_BUCKET, Key=TEMPLATE_KEY)
    return obj["Body"].read()


def save_output(id_pratica: str, excel_bytes: bytes) -> str:
    key = f"{OUTPUT_PREFIX}/{id_pratica}/prospetto_{id_pratica}.xlsx"
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=key,
        Body=excel_bytes,
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ContentDisposition=f'attachment; filename="prospetto_{id_pratica}.xlsx"',
    )
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": OUTPUT_BUCKET, "Key": key},
        ExpiresIn=PRESIGNED_URL_EXPIRY,
    )
    return url


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body, ensure_ascii=False, default=str),
    }


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    logger.info("EVENT: %s", json.dumps(event, default=str))

    http_method = event.get("httpMethod") or (
        event.get("requestContext", {}).get("http", {}).get("method", "")
    )
    if http_method == "OPTIONS":
        return response(200, {})

    path_params = event.get("pathParameters") or {}
    id_pratica = path_params.get("id_pratica", "").strip()

    if not id_pratica:
        return response(400, {"error": "id_pratica mancante nel path"})

    logger.info("Generating prospetto for pratica: %s", id_pratica)

    # 1. Leggi METADATA
    try:
        metadata = query_metadata(id_pratica)
    except ClientError as e:
        return response(500, {"error": f"DynamoDB METADATA: {e.response['Error']['Message']}"})

    if not metadata:
        return response(404, {"error": f"Pratica {id_pratica} non trovata"})

    # 2. Leggi decreto di ricostruzione
    try:
        decreto = query_documento_decreto(id_pratica)
    except ClientError as e:
        return response(500, {"error": f"DynamoDB decreto: {e.response['Error']['Message']}"})

    if not decreto:
        return response(404, {"error": "Nessun documento decreto_ricostruzione trovato per questa pratica"})

    # 3. Costruisci mappa placeholder
    mapping = _build_placeholder_map(decreto, metadata)

    logger.info(
        "Placeholder map keys: %s",
        [k for k in mapping if not k.startswith("_")]
    )

    # 4. Carica template
    try:
        template_bytes = load_template()
    except ClientError as e:
        return response(500, {"error": f"Impossibile caricare il template: {e.response['Error']['Message']}"})

    # 5. Compila Excel
    try:
        excel_bytes = fill_excel(template_bytes, mapping)
    except Exception as e:
        logger.exception("Errore compilazione Excel")
        return response(500, {"error": f"Errore compilazione Excel: {str(e)}"})

    # 6. Salva su S3 e restituisci URL
    try:
        url = save_output(id_pratica, excel_bytes)
    except ClientError as e:
        return response(500, {"error": f"Errore salvataggio S3: {e.response['Error']['Message']}"})

    logger.info("Prospetto generato: %s", url.split("?")[0])
    return response(200, {"url": url, "id_pratica": id_pratica})
