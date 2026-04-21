import json
import logging
import os
import re
import unicodedata
import urllib.parse
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
bedrock = boto3.client(
    "bedrock-runtime",
    config=Config(read_timeout=120, connect_timeout=10, retries={"max_attempts": 3}),
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_practice_output_prefix(prefix, id_pratica=None):
    normalized_prefix = prefix.strip("/")
    if id_pratica:
        return f"output/{normalized_prefix}/{id_pratica}"
    return normalized_prefix


VALID_DOCUMENT_TYPES = [
    "Decreto di ricostruzione della carriera",
    "Istanza ricostruzione di carriera",
    "Certificato di servizi/Dichiarazione di servizi/Autocertificazione dei servizi",
    "Contratto a tempo indeterminato",
    "Decreto superamento anno di formazione e prova",
    "Presa d'atto di conferma in ruolo del personale",
    "Titolo di studio",
    "Foglio di congedo illimitato",
    "Decreto di mancato superamento prova",
    "Osservazione precedente",
    "NON_CLASSIFICATO",
]

DOCUMENT_TYPE_HINTS = {
    "Decreto di ricostruzione della carriera": [
        "decreto di ricostruzione della carriera",
        "ricostruzione della carriera",
        "anzianita complessiva preruolo",
        "anzianità complessiva preruolo",
        "posizione stipendiale",
        "servizi preruolo riconosciuti",
        "ai fini giuridici ed economici",
        "ai soli fini economici",
    ],
    "Istanza ricostruzione di carriera": [
        "istanza ricostruzione di carriera",
        "domanda di ricostruzione della carriera",
        "richiesta di ricostruzione della carriera",
    ],
    "Certificato di servizi/Dichiarazione di servizi/Autocertificazione dei servizi": [
        "certificato di servizi",
        "dichiarazione di servizi",
        "autocertificazione dei servizi",
        "autocertificazione di servizi",
    ],
    "Contratto a tempo indeterminato": [
        "contratto a tempo indeterminato",
        "assunzione a tempo indeterminato",
    ],
    "Decreto superamento anno di formazione e prova": [
        "decreto superamento anno di formazione e prova",
        "superamento anno di formazione e prova",
        "anno di formazione e prova",
        "anno di prova",
    ],
    "Presa d'atto di conferma in ruolo del personale": [
        "presa d atto di conferma in ruolo del personale",
        "presa d'atto di conferma in ruolo del personale",
        "conferma in ruolo del personale",
        "conferma in ruolo",
    ],
    "Titolo di studio": [
        "titolo di studio",
        "diploma",
        "laurea",
        "maturita",
        "maturita magistrale",
        "abilitazione",
    ],
    "Foglio di congedo illimitato": [
        "foglio di congedo illimitato",
        "congedo illimitato",
    ],
    "Decreto di mancato superamento prova": [
        "decreto di mancato superamento prova",
        "mancato superamento prova",
        "mancato superamento del periodo di prova",
        "mancato superamento dell anno di prova",
    ],
    "Osservazione precedente": [
        "osservazione precedente",
        "risposta a osservazione",
        "risposta ad osservazione",
        "osservazione inviata in precedenza",
    ],
}


def normalize_text_for_match(value):
    if not isinstance(value, str):
        return ""

    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.replace(chr(8217), "'").replace("`", "'")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9']+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


CANONICAL_DOCUMENT_TYPES = {
    normalize_text_for_match(document_type): document_type
    for document_type in VALID_DOCUMENT_TYPES
}

DOCUMENT_TYPE_ALIASES = {}
for canonical_document_type, hints in DOCUMENT_TYPE_HINTS.items():
    for hint in hints:
        DOCUMENT_TYPE_ALIASES[normalize_text_for_match(hint)] = canonical_document_type

DECRETO_DOCUMENT_TYPE = "Decreto di ricostruzione della carriera"

DECRETO_STRONG_HINTS = {
    "decreto di ricostruzione della carriera",
    "ricostruzione della carriera",
    "anzianita complessiva preruolo",
    "anzianita di ruolo",
    "posizione stipendiale",
    "servizi preruolo riconosciuti",
    "ai fini giuridici ed economici",
    "ai soli fini economici",
}

DECRETO_NEGATIVE_HINTS = {
    "domanda richiesta di ricostruzione carriera",
    "domanda di ricostruzione carriera",
    "domanda richiesta di ricostruzione della carriera",
    "istanza ricostruzione di carriera",
    "richiesta di ricostruzione della carriera",
}

SYSTEM_PROMPT = """
Sei un classificatore documentale prudente per documenti amministrativi scolastici italiani.

Devi classificare il documento in UNA sola delle seguenti classi:
- Decreto di ricostruzione della carriera
- Istanza ricostruzione di carriera
- Certificato di servizi/Dichiarazione di servizi/Autocertificazione dei servizi
- Contratto a tempo indeterminato
- Decreto superamento anno di formazione e prova
- Presa d'atto di conferma in ruolo del personale
- Titolo di studio
- Foglio di congedo illimitato
- Decreto di mancato superamento prova
- Osservazione precedente
- NON_CLASSIFICATO

Regole:
- Sii prudente.
- Se gli indizi non sono sufficienti, troppo deboli o ambigui, restituisci NON_CLASSIFICATO.
- Se due classi sono entrambe plausibili ma nessuna e' dominante, restituisci NON_CLASSIFICATO.
- NON_CLASSIFICATO e' un esito valido, non un errore.
- Il valore di `documentType` deve essere copiato ESATTAMENTE da una delle classi ammesse, senza parafrasi, senza abbreviazioni e senza testo aggiuntivo.
- Se documentType e' NON_CLASSIFICATO, confidence deve essere LOW e discard deve essere true.
- confidence deve essere HIGH, MEDIUM oppure LOW.
- Per "Decreto di ricostruzione della carriera", considera segnali forti diciture come:
  "decreto di ricostruzione della carriera", "ricostruzione della carriera",
  "anzianita complessiva preruolo", "posizione stipendiale",
  "ai fini giuridici ed economici", "ai soli fini economici".
- Se il documento contiene piu segnali forti tipici del decreto e non e una semplice istanza/domanda dell'interessato, classificalo come "Decreto di ricostruzione della carriera".
- Restituisci solo JSON puro, senza testo aggiuntivo.

Schema di output:
{
  "documentType": "string",
  "confidence": "HIGH|MEDIUM|LOW",
  "discard": true,
  "reasoning": "string"
}
""".strip()


def parse_clean_key(key, clean_prefix):
    parts = key.split("/")

    if len(parts) == 2 and parts[0] == clean_prefix and parts[1].lower().endswith(".json"):
        filename = parts[1]
        base_name = filename[:-5] if filename.lower().endswith(".json") else filename
        return {
            "filename": filename,
            "base_name": base_name,
            "id_pratica": None,
        }

    if (
        len(parts) == 5
        and parts[0] == "pratiche"
        and parts[2] == "output"
        and parts[3] == clean_prefix
        and parts[4].lower().endswith(".json")
    ):
        filename = parts[4]
        base_name = filename[:-5] if filename.lower().endswith(".json") else filename
        return {
            "filename": filename,
            "base_name": base_name,
            "id_pratica": parts[1],
        }

    if (
        len(parts) == 4
        and parts[0] == "output"
        and parts[1] == clean_prefix
        and parts[3].lower().endswith(".json")
    ):
        filename = parts[3]
        base_name = filename[:-5] if filename.lower().endswith(".json") else filename
        return {
            "filename": filename,
            "base_name": base_name,
            "id_pratica": parts[2],
        }

    return None


def build_output_key(classified_prefix, base_name, id_pratica=None):
    scoped_prefix = build_practice_output_prefix(classified_prefix, id_pratica)
    return f"{scoped_prefix}/{base_name}.classification.json"


def load_json_object(bucket, key):
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise

    return json.loads(body)


def get_text_block(response_body):
    content = response_body.get("output", {}).get("message", {}).get("content", [])
    texts = [block["text"] for block in content if "text" in block]
    return "".join(texts).strip()


def parse_model_json(raw_text):
    text = raw_text.strip()

    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]

    return json.loads(text)


def build_classification_input(document, source_key):
    pages_text = []
    for page in document.get("pagesText", [])[:3]:
        pages_text.append(
            {
                "page": page.get("page"),
                "text": (page.get("text") or "")[:1500],
            }
        )

    text_preview = (document.get("textPreview") or document.get("text") or "")[:6000]

    return {
        "sourceCleanKey": source_key,
        "sourceRawConsolidatedKey": document.get("sourceRawConsolidatedKey"),
        "sourceFile": document.get("sourceFile"),
        "metadata": document.get("metadata", {}),
        "summary": document.get("summary", {}),
        "textPreview": text_preview,
        "pagesText": pages_text,
        "formsByPage": document.get("formsByPage", []),
        "tablesByPage": document.get("tablesByPage", []),
    }


def _classification_text_for_rules(classification_input):
    parts = [
        classification_input.get("sourceFile") or "",
        classification_input.get("textPreview") or "",
    ]
    for page in classification_input.get("pagesText", []):
        parts.append(page.get("text") or "")
    for page in classification_input.get("tablesByPage", []):
        for table in page.get("items", []) or []:
            for row in table.get("rows", []) or []:
                parts.extend(str(cell or "") for cell in row)
    return normalize_text_for_match(" ".join(parts))


def deterministic_decreto_override(classification_input):
    normalized_text = _classification_text_for_rules(classification_input)
    if not normalized_text:
        return None

    matched_hints = sorted(hint for hint in DECRETO_STRONG_HINTS if hint in normalized_text)
    matched_negative_hints = sorted(hint for hint in DECRETO_NEGATIVE_HINTS if hint in normalized_text)

    has_explicit_decreto = "decreto di ricostruzione della carriera" in matched_hints
    has_decreto_structure = (
        "ricostruzione della carriera" in matched_hints
        and (
            "ai fini giuridici ed economici" in matched_hints
            or "ai soli fini economici" in matched_hints
            or "posizione stipendiale" in matched_hints
            or "anzianita complessiva preruolo" in matched_hints
        )
    )

    if matched_negative_hints and not (has_explicit_decreto or len(matched_hints) >= 3):
        return None
    if not (has_explicit_decreto or has_decreto_structure or len(matched_hints) >= 3):
        return None

    return {
        "documentType": DECRETO_DOCUMENT_TYPE,
        "confidence": "HIGH",
        "discard": False,
        "reasoning": "Override deterministico: trovati segnali forti del decreto di ricostruzione carriera: "
        + ", ".join(matched_hints[:6]),
    }


def classify_document(model_id, classification_input):
    user_prompt = (
        "Classifica il seguente documento usando solo le classi ammesse.\n"
        "Se non ci sono evidenze sufficienti, usa NON_CLASSIFICATO.\n\n"
        f"Classi ammesse da copiare letteralmente in documentType:\n{json.dumps(VALID_DOCUMENT_TYPES, ensure_ascii=False)}\n\n"
        f"Documento:\n{json.dumps(classification_input, ensure_ascii=False)}"
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"text": user_prompt}],
            }
        ],
        "system": [{"text": SYSTEM_PROMPT}],
        "inferenceConfig": {
            "maxTokens": 400,
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
    return parse_model_json(raw_text)


def validate_classification(result):
    document_type = result.get("documentType")
    confidence = result.get("confidence", "LOW")
    reasoning = result.get("reasoning", "").strip()

    normalized_document_type = normalize_text_for_match(document_type)
    canonical_document_type = (
        CANONICAL_DOCUMENT_TYPES.get(normalized_document_type)
        or DOCUMENT_TYPE_ALIASES.get(normalized_document_type)
    )
    if not canonical_document_type:
        reasoning_candidates = set()
        normalized_reasoning = normalize_text_for_match(reasoning)
        for alias, candidate_document_type in DOCUMENT_TYPE_ALIASES.items():
            if alias and alias in normalized_reasoning:
                reasoning_candidates.add(candidate_document_type)
        if len(reasoning_candidates) == 1:
            canonical_document_type = next(iter(reasoning_candidates))
            logger.info(
                "classify_document_type_inferred_from_reasoning raw_document_type=%s inferred_document_type=%s",
                document_type,
                canonical_document_type,
            )

    if canonical_document_type:
        document_type = canonical_document_type
    else:
        document_type = "NON_CLASSIFICATO"
        confidence = "LOW"
        reasoning = reasoning or "Il modello ha restituito una classe non valida."

    if confidence not in {"HIGH", "MEDIUM", "LOW"}:
        confidence = "LOW"

    if document_type == "NON_CLASSIFICATO":
        confidence = "LOW"
        discard = True
        reasoning = reasoning or "Il contenuto non presenta elementi sufficienti e univoci per assegnare una delle classi previste."
    else:
        discard = False
        reasoning = reasoning or "Classificazione eseguita senza motivazione esplicita."

    return {
        "documentType": document_type,
        "confidence": confidence,
        "discard": discard,
        "reasoning": reasoning,
    }


def lambda_handler(event, context):
    logger.info("classify_event_received event=%s", json.dumps(event))

    clean_prefix = os.environ.get("CLEAN_PREFIX", "clean").strip("/")
    classified_prefix = os.environ.get("CLASSIFIED_PREFIX", "classified").strip("/")
    model_id = os.environ.get("BEDROCK_MODEL_ID", "eu.amazon.nova-lite-v1:0")

    saved_files = []

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            logger.info("classify_skip reason=non_s3_record")
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        source_etag = record["s3"]["object"].get("eTag")
        logger.info("classify_event bucket=%s key=%s etag=%s", bucket, key, source_etag)
        parsed = parse_clean_key(key, clean_prefix)
        if not parsed:
            logger.info("classify_skip key=%s reason=invalid_clean_key", key)
            continue

        output_key = build_output_key(
            classified_prefix,
            parsed["base_name"],
            parsed.get("id_pratica"),
        )
        existing_output = load_json_object(bucket, output_key)
        if existing_output and existing_output.get("sourceCleanKey") == key and existing_output.get("sourceCleanETag") == source_etag:
            logger.info(
                "classify_skip output_key=%s reason=classification_already_current source_key=%s",
                output_key,
                key,
            )
            continue

        logger.info("classify_read_clean_start key=%s", key)
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        document = json.loads(body)
        logger.info("classify_read_clean_done key=%s size_bytes=%s", key, len(body))

        classification_input = build_classification_input(document, key)
        logger.info(
            "classify_input_ready key=%s model_id=%s text_len=%s pages=%s forms_page=%s tables_pages=%s",
            key,
            model_id,
            len(classification_input["textPreview"]),
            len(classification_input["pagesText"]),
            len(classification_input["formsByPage"]),
            len(classification_input["tablesByPage"]),
        )
        logger.info("classify_bedrock_call_start key=%s model_id=%s", key, model_id)
        try:
            raw_result = classify_document(model_id, classification_input)
        except json.JSONDecodeError:
            logger.warning("classify_model_parse_fallback key=%s reason=invalid_model_json", key)
            raw_result = {
                "documentType": "NON_CLASSIFICATO",
                "confidence": "LOW",
                "discard": True,
                "reasoning": "Il modello ha restituito una risposta non valida per la classificazione.",
            }
        logger.info("classify_model_raw_result key=%s raw_result=%s", key, json.dumps(raw_result, ensure_ascii=False))
        classification = validate_classification(raw_result)
        decreto_override = deterministic_decreto_override(classification_input)
        if decreto_override and classification["documentType"] != DECRETO_DOCUMENT_TYPE:
            logger.info(
                "classify_decreto_deterministic_override key=%s previous_document_type=%s override_reasoning=%s",
                key,
                classification["documentType"],
                decreto_override["reasoning"],
            )
            classification = decreto_override
        logger.info(
            "classify_bedrock_call_done key=%s document_type=%s confidence=%s discard=%s",
            key,
            classification["documentType"],
            classification["confidence"],
            classification["discard"],
        )

        output = {
            "sourceCleanKey": key,
            "sourceCleanETag": source_etag,
            "sourceFile": parsed["filename"],
            "documentType": classification["documentType"],
            "confidence": classification["confidence"],
            "discard": classification["discard"],
            "reasoning": classification["reasoning"],
            "modelId": model_id,
            "classifiedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        logger.info("classify_write_output_start output_key=%s", output_key)
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(output, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("classify_write_output_done output_key=%s", output_key)

        saved_files.append(
            {
                "sourceKey": key,
                "outputKey": output_key,
                "documentType": output["documentType"],
            }
        )

    logger.info("classify_completed saved_files=%s", json.dumps(saved_files, ensure_ascii=False))
    return {"savedFiles": saved_files}

