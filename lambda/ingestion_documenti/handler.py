"""
Lambda unificata — ingestion documenti
======================================
Flussi supportati:
  • PDF / PNG / JPG / JPEG  →  Textract diretto
  • P7M                     →  estrazione firme CAdES
                               + conversione PDF inline
                               + Textract sul PDF estratto

Path S3 atteso:
  pratiche/<tipo_servizio>/<id_pratica>/input/<file>
  tipo_servizio ∈ {"controlli", "stipendi"}

Variabili d'ambiente
---------------------
OUTPUT_BUCKET               bucket dove scrivere tutti gli output
RAW_PREFIX                  (default: "raw")
DOCUMENT_CONTEXT_PREFIX     (default: "document_context")
SIGNATURES_PREFIX           (default: "signatures")
DEFAULT_ID_PRATICA          fallback id_pratica se non deducibile
"""

import hashlib
import json
import logging
import os
import urllib.parse
from pathlib import Path

import boto3
from asn1crypto import cms
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Client AWS
# ---------------------------------------------------------------------------
s3 = boto3.client("s3")
textract = boto3.client("textract")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------
TEXTRACT_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg"}
P7M_EXTENSION = ".p7m"
ALLOWED_TIPI = {"controlli", "stipendi"}


# ===========================================================================
# SEZIONE 1 — Helpers generici (path, id, prefissi)
# ===========================================================================

def is_textract_document(key: str) -> bool:
    return Path(key).suffix.lower() in TEXTRACT_EXTENSIONS


def is_p7m_document(key: str) -> bool:
    return key.lower().endswith(P7M_EXTENSION)


def extract_pratica_info_from_source_key(source_key: str) -> tuple[str | None, str | None]:
    """
    Riconosce il pattern  pratiche/<tipo_servizio>/<id_pratica>/input/<file>
    e restituisce (id_pratica, tipo_servizio).
    Restituisce (None, None) se il path non corrisponde.
    """
    parts = [p for p in source_key.strip("/").split("/") if p]
    if len(parts) >= 5 and parts[0] == "pratiche" and parts[3] == "input":
        tipo_servizio = parts[1] if parts[1] in ALLOWED_TIPI else None
        id_pratica = parts[2]
        return id_pratica, tipo_servizio
    return None, None


def build_practice_output_prefix(prefix: str, tipo_servizio: str | None, id_pratica: str | None) -> str:
    """
    Costruisce il prefisso S3 di output includendo tipo_servizio e id_pratica.
    Esempio: build_practice_output_prefix("raw", "controlli", "MARIO_20260421T...")
             → "output/raw/controlli/MARIO_20260421T..."
    """
    normalized = prefix.strip("/")
    parts = ["output", normalized]
    if tipo_servizio:
        parts.append(tipo_servizio)
    if id_pratica:
        parts.append(id_pratica)
    return "/".join(parts)


def build_document_id(source_bucket: str, source_key: str) -> str:
    filename = source_key.rsplit("/", 1)[-1]
    base_name = Path(filename).stem
    safe_base = "".join(
        c if c.isalnum() or c in ("-", "_") else "_"
        for c in base_name
    ).strip("_") or "document"
    digest = hashlib.sha1(
        f"{source_bucket}/{source_key}".encode("utf-8")
    ).hexdigest()[:12]
    return f"{safe_base}__{digest}"


# ===========================================================================
# SEZIONE 2 — Contesto documento (metadati S3 + id_pratica + tipo_servizio)
# ===========================================================================

def load_document_context(source_bucket: str, source_key: str) -> dict:
    """
    Legge i metadati S3 e il path per estrarre id_pratica e tipo_servizio.
    """
    response = s3.head_object(Bucket=source_bucket, Key=source_key)
    metadata = response.get("Metadata", {})

    id_pratica_from_key, tipo_servizio_from_key = extract_pratica_info_from_source_key(source_key)

    id_pratica = (
        id_pratica_from_key
        or metadata.get("id_pratica")
        or metadata.get("id-pratica")
        or metadata.get("idpratica")
        or os.environ.get("DEFAULT_ID_PRATICA")
    )

    tipo_servizio = tipo_servizio_from_key or metadata.get("tipo_servizio")

    if id_pratica_from_key:
        id_pratica_source = "source_key"
    elif any(metadata.get(k) for k in ("id_pratica", "id-pratica", "idpratica")):
        id_pratica_source = "s3_metadata"
    elif os.environ.get("DEFAULT_ID_PRATICA"):
        id_pratica_source = "default_env"
    else:
        id_pratica_source = None

    return {
        "id_pratica": id_pratica,
        "tipo_servizio": tipo_servizio,
        "id_pratica_source": id_pratica_source,
        "sourceBucket": source_bucket,
        "sourceKey": source_key,
        "sourceMetadata": metadata,
    }


# ===========================================================================
# SEZIONE 3 — Key builders (output bucket)
# ===========================================================================

def build_context_key(
    document_context_prefix: str,
    document_id: str,
    tipo_servizio: str | None = None,
    id_pratica: str | None = None,
) -> str:
    scoped = build_practice_output_prefix(document_context_prefix, tipo_servizio, id_pratica)
    return f"{scoped}/{document_id}.json"


def build_signatures_key(
    signatures_prefix: str,
    source_file: str,
    tipo_servizio: str | None = None,
    id_pratica: str | None = None,
) -> str:
    scoped = build_practice_output_prefix(signatures_prefix, tipo_servizio, id_pratica)
    return f"{scoped}/{source_file}.json"


def build_extracted_pdf_key(
    pdf_source_file: str,
    tipo_servizio: str | None = None,
    id_pratica: str | None = None,
) -> str:
    scoped = build_practice_output_prefix("extracted_pdf", tipo_servizio, id_pratica)
    return f"{scoped}/{pdf_source_file}"


# ===========================================================================
# SEZIONE 4 — P7M: estrazione firme e contenuto PDF
# ===========================================================================

def extract_p7m_signatures(raw_bytes: bytes) -> list:
    try:
        info = cms.ContentInfo.load(raw_bytes)
        signed_data = info["content"]
        signatures = []
        for cert_choice in signed_data["certificates"]:
            cert = cert_choice.chosen
            cn = None
            for rdn in cert.subject.chosen:
                for atv in rdn:
                    if atv["type"].dotted == "2.5.4.3":
                        cn = str(atv["value"].chosen)
                        break
            tbs = cert["tbs_certificate"]
            signatures.append({
                "tipo": "CAdES",
                "nome": cn,
                "data_inizio": str(tbs["validity"]["not_before"].chosen.native),
                "data_fine": str(tbs["validity"]["not_after"].chosen.native),
            })
        return signatures
    except Exception as exc:
        logger.warning("p7m_signature_extraction_failed reason=%s", exc)
        return []


def extract_p7m_content(p7m_bytes: bytes) -> bytes | None:
    try:
        info = cms.ContentInfo.load(p7m_bytes)
        signed_data = info["content"]
        content_type = signed_data["encap_content_info"]["content_type"].native
        logger.info("p7m_content_type=%s", content_type)

        raw_content = signed_data["encap_content_info"]["content"]
        if raw_content.native is None:
            logger.error("p7m_content_empty")
            return None

        pdf_bytes = raw_content.native
        return bytes(pdf_bytes) if isinstance(pdf_bytes, memoryview) else pdf_bytes
    except Exception as exc:
        logger.error("p7m_extraction_exception reason=%s", exc)
        return None


def save_signatures(
    output_bucket: str,
    signatures_key: str,
    signatures: list,
    extra_meta: dict | None = None,
) -> None:
    payload = {"signatures": signatures}
    if extra_meta:
        payload.update(extra_meta)
    s3.put_object(
        Bucket=output_bucket,
        Key=signatures_key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("p7m_signatures_saved key=%s count=%s", signatures_key, len(signatures))


def process_p7m(
    source_bucket: str,
    source_key: str,
    output_bucket: str,
    signatures_prefix: str,
) -> dict | None:
    """
    1. Scarica il P7M
    2. Estrae id_pratica e tipo_servizio dal path
    3. Estrae e salva le firme CAdES
    4. Estrae il PDF inline e lo carica nell'output bucket
    5. Restituisce un dict con le info per avviare Textract
    """
    logger.info("p7m_processing_start bucket=%s key=%s", source_bucket, source_key)

    id_pratica, tipo_servizio = extract_pratica_info_from_source_key(source_key)
    source_file = Path(source_key).name

    p7m_bytes = s3.get_object(Bucket=source_bucket, Key=source_key)["Body"].read()

    signatures = extract_p7m_signatures(p7m_bytes)
    signatures_key = build_signatures_key(signatures_prefix, source_file, tipo_servizio, id_pratica)
    save_signatures(
        output_bucket,
        signatures_key,
        signatures,
        extra_meta={
            "source_p7m_key": source_key,
            "id_pratica": id_pratica,
            "tipo_servizio": tipo_servizio,
            "source_file": source_file,
        },
    )

    pdf_bytes = extract_p7m_content(p7m_bytes)
    if not pdf_bytes:
        logger.error("p7m_pdf_extraction_failed key=%s", source_key)
        return None

    pdf_source_file = source_file
    if pdf_source_file.lower().endswith(".p7m"):
        pdf_source_file = pdf_source_file[: -len(".p7m")]

    pdf_output_key = build_extracted_pdf_key(pdf_source_file, tipo_servizio, id_pratica)
    s3.put_object(
        Bucket=output_bucket,
        Key=pdf_output_key,
        Body=pdf_bytes,
        ContentType="application/pdf",
    )
    logger.info("p7m_pdf_uploaded bucket=%s key=%s", output_bucket, pdf_output_key)

    return {
        "id_pratica": id_pratica,
        "tipo_servizio": tipo_servizio,
        "original_p7m_source_key": source_key,
        "original_p7m_source_file": source_file,
        "pdf_source_file": pdf_source_file,
        "pdf_bucket": output_bucket,
        "pdf_key": pdf_output_key,
        "signatures_key": signatures_key,
    }


# ===========================================================================
# SEZIONE 5 — Textract: avvio analisi e scrittura contesto
# ===========================================================================

def start_textract_job(
    *,
    source_bucket: str,
    source_key: str,
    output_bucket: str,
    document_id: str,
    raw_prefix: str,
    tipo_servizio: str | None,
    id_pratica: str | None,
    document_context_prefix: str,
    extra_context: dict | None = None,
) -> dict:
    scoped_raw_prefix = build_practice_output_prefix(raw_prefix, tipo_servizio, id_pratica)
    raw_output_prefix = f"{scoped_raw_prefix}/{document_id}"

    context_key = build_context_key(document_context_prefix, document_id, tipo_servizio, id_pratica)

    logger.info(
        "textract_start_analysis source_bucket=%s source_key=%s "
        "document_id=%s raw_prefix=%s tipo_servizio=%s id_pratica=%s",
        source_bucket, source_key, document_id, raw_output_prefix, tipo_servizio, id_pratica,
    )

    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": source_bucket, "Name": source_key}},
        FeatureTypes=["TABLES", "FORMS"],
        OutputConfig={"S3Bucket": output_bucket, "S3Prefix": raw_output_prefix},
    )

    context_document = {
        "documentId": document_id,
        "jobId": response["JobId"],
        "sourceBucket": source_bucket,
        "sourceKey": source_key,
        "rawPrefix": raw_output_prefix,
        "tipo_servizio": tipo_servizio,
        "id_pratica": id_pratica,
    }
    if extra_context:
        context_document.update(extra_context)

    s3.put_object(
        Bucket=output_bucket,
        Key=context_key,
        Body=json.dumps(context_document, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        "textract_context_written context_key=%s document_id=%s tipo_servizio=%s id_pratica=%s",
        context_key, document_id, tipo_servizio, id_pratica,
    )

    return {
        "jobId": response["JobId"],
        "sourceBucket": source_bucket,
        "sourceKey": source_key,
        "outputBucket": output_bucket,
        "documentId": document_id,
        "rawPrefix": raw_output_prefix,
        "contextKey": context_key,
        "tipoServizio": tipo_servizio,
        "idPratica": id_pratica,
    }


# ===========================================================================
# SEZIONE 6 — Lambda handler
# ===========================================================================

def lambda_handler(event, context):
    logger.info("unified_event_received event=%s", json.dumps(event))

    output_bucket = os.environ["OUTPUT_BUCKET"]
    raw_prefix = os.environ.get("RAW_PREFIX", "raw").strip("/")
    document_context_prefix = os.environ.get("DOCUMENT_CONTEXT_PREFIX", "document_context").strip("/")
    signatures_prefix = os.environ.get("SIGNATURES_PREFIX", "signatures").strip("/")

    started_jobs: list[dict] = []

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            logger.info("unified_skip reason=non_s3_record")
            continue

        source_bucket = record["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        logger.info(
            "unified_record source_bucket=%s source_key=%s output_bucket=%s",
            source_bucket, source_key, output_bucket,
        )

        # ------------------------------------------------------------------
        # Ramo A — P7M
        # ------------------------------------------------------------------
        if is_p7m_document(source_key):
            p7m_result = process_p7m(
                source_bucket=source_bucket,
                source_key=source_key,
                output_bucket=output_bucket,
                signatures_prefix=signatures_prefix,
            )
            if not p7m_result:
                logger.warning("unified_skip key=%s reason=p7m_processing_failed", source_key)
                continue

            pdf_document_id = build_document_id(p7m_result["pdf_bucket"], p7m_result["pdf_key"])

            job = start_textract_job(
                source_bucket=p7m_result["pdf_bucket"],
                source_key=p7m_result["pdf_key"],
                output_bucket=output_bucket,
                document_id=pdf_document_id,
                raw_prefix=raw_prefix,
                tipo_servizio=p7m_result["tipo_servizio"],
                id_pratica=p7m_result["id_pratica"],
                document_context_prefix=document_context_prefix,
                extra_context={
                    "originalP7mSourceBucket": source_bucket,
                    "originalP7mSourceKey": source_key,
                    "originalP7mSourceFile": p7m_result["original_p7m_source_file"],
                    "signaturesKey": p7m_result["signatures_key"],
                    "id_pratica_source": "source_key" if p7m_result["id_pratica"] else None,
                },
            )
            job["p7mConverted"] = True
            job["originalP7mSourceKey"] = source_key
            job["signaturesKey"] = p7m_result["signatures_key"]
            started_jobs.append(job)
            continue

        # ------------------------------------------------------------------
        # Ramo B — PDF / immagine diretta
        # ------------------------------------------------------------------
        if is_textract_document(source_key):
            document_context = load_document_context(source_bucket, source_key)
            document_id = build_document_id(source_bucket, source_key)

            job = start_textract_job(
                source_bucket=source_bucket,
                source_key=source_key,
                output_bucket=output_bucket,
                document_id=document_id,
                raw_prefix=raw_prefix,
                tipo_servizio=document_context.get("tipo_servizio"),
                id_pratica=document_context.get("id_pratica"),
                document_context_prefix=document_context_prefix,
                extra_context={
                    "id_pratica_source": document_context.get("id_pratica_source"),
                    "sourceMetadata": document_context.get("sourceMetadata", {}),
                },
            )
            started_jobs.append(job)
            continue

        logger.info("unified_skip source_key=%s reason=unsupported_format", source_key)

    logger.info("unified_completed started_jobs=%s", json.dumps(started_jobs, ensure_ascii=False))
    return {"startedJobs": started_jobs}
