"""
Orchestratore deterministico per checklist di controllo.

Flusso:
1. Riceve id_pratica e tipo_checklist
2. Legge i metadati della pratica da DynamoDB
3. Carica lo schema della checklist da S3 (schemas/{tipo_checklist}.json)
4. Filtra i controlli in base alle condizioni (es. qualifica == secondaria)
5. Costruisce l'input per Step Functions separando bedrock_agent da lambda diretta
6. Invoca lo Step Functions Express Workflow (sync)
7. Restituisce il risultato aggregato al chiamante
"""

import json
import logging
import os
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Client AWS ---
s3_client      = boto3.client("s3")
dynamodb       = boto3.resource("dynamodb")
sfn_client     = boto3.client("stepfunctions")

# --- Variabili d'ambiente ---
CHECKLIST_BUCKET = os.environ["CHECKLIST_BUCKET"]           # es. "my-checklist-bucket"
CHECKLIST_PREFIX = os.environ.get("CHECKLIST_PREFIX", "schemas")
PRATICHE_TABLE   = os.environ["PRATICHE_TABLE"]             # tabella DynamoDB pratiche
SFN_ARN          = os.environ["SFN_STATE_MACHINE_ARN"]      # ARN Step Functions Express

# Cache in-memory dello schema (valida per tutta la vita del container Lambda)
_schema_cache: dict = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_checklist_schema(tipo_checklist: str) -> dict:
    """
    Carica il JSON dello schema da S3 con cache in-memory.
    Path atteso: s3://{CHECKLIST_BUCKET}/{CHECKLIST_PREFIX}/{tipo_checklist}.json
    """
    if tipo_checklist in _schema_cache:
        logger.info(f"Schema '{tipo_checklist}' servito da cache in-memory")
        return _schema_cache[tipo_checklist]

    key = f"{CHECKLIST_PREFIX}/{tipo_checklist}.json"
    logger.info(f"Caricamento schema da s3://{CHECKLIST_BUCKET}/{key}")

    try:
        response = s3_client.get_object(Bucket=CHECKLIST_BUCKET, Key=key)
        schema = json.loads(response["Body"].read().decode("utf-8"))
        _schema_cache[tipo_checklist] = schema
        logger.info(f"Schema caricato: {len(schema.get('controlli', []))} controlli")
        return schema
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise ValueError(f"Schema checklist non trovato: s3://{CHECKLIST_BUCKET}/{key}")
        raise


def get_pratica_metadata(id_pratica: str) -> dict:
    """
    Legge i metadati della pratica da DynamoDB.
    Campi usati per le condizioni:
      - qualifica_funzionale
      - fonte_documento_servizi
    """
    table = dynamodb.Table(PRATICHE_TABLE)
    try:
        response = table.get_item(Key={"id_pratica": id_pratica})
    except ClientError as e:
        raise RuntimeError(f"Errore DynamoDB lettura pratica '{id_pratica}': {e}")

    item = response.get("Item")
    if not item:
        raise ValueError(f"Pratica non trovata in DynamoDB: '{id_pratica}'")

    logger.info(f"Metadati pratica '{id_pratica}': qualifica='{item.get('qualifica_funzionale')}' "
                f"fonte='{item.get('fonte_documento_servizi')}'")
    return item


def evaluate_condition(condition: str | None, metadata: dict) -> bool:
    """
    Valuta la condizione di un controllo rispetto ai metadati della pratica.

    Condizioni supportate:
      null / None                          → sempre incluso
      "qualifica == secondaria"            → incluso se qualifica contiene 'secondaria' o 'media'
      "qualifica == primaria"              → incluso se qualifica contiene 'primaria' o 'elementare'
      "qualifica == infanzia"              → incluso se qualifica contiene 'infanzia' o 'materna'
      "fonte == autocertificazione_servizi"
      "fonte == dichiarazione_servizi"
      "fonte == certificato_servizio"

    Aggiungere nuove condizioni estendendo condition_map.
    """
    if not condition:
        return True

    qualifica = metadata.get("qualifica_funzionale", "").lower()
    fonte     = metadata.get("fonte_documento_servizi", "").lower()

    condition_map = {
        "qualifica == secondaria":              any(k in qualifica for k in ("secondaria", "media", "superiore")),
        "qualifica == primaria":                any(k in qualifica for k in ("primaria", "elementare")),
        "qualifica == infanzia":                any(k in qualifica for k in ("infanzia", "materna")),
        "fonte == autocertificazione_servizi":  fonte == "autocertificazione_servizi",
        "fonte == dichiarazione_servizi":       fonte == "dichiarazione_servizi",
        "fonte == certificato_servizio":        fonte == "certificato_servizio",
    }

    result = condition_map.get(condition.strip())
    if result is None:
        logger.warning(f"Condizione non riconosciuta: '{condition}' — controllo incluso per default")
        return True

    logger.info(f"Condizione '{condition}' → {result}")
    return result


def build_sfn_input(id_pratica: str, controlli_attivi: list[dict]) -> dict:
    """
    Costruisce il payload per Step Functions.

    Ogni item dell'array 'controlli' ha questa struttura comune:
    {
        "controllo_id":    str,
        "descrizione":     str,
        "macro_categoria": str,
        "tipo_invocazione": "bedrock_agent" | "lambda",
        -- se bedrock_agent:
            "agent_id":  str,
            "alias_id":  str,
            "prompt":    str  (con {id_pratica} già sostituito)
        -- se lambda:
            "lambda_function_name": str,
            "payload":              dict
    }
    """
    items = []
    for c in controlli_attivi:
        tipo = c.get("tipo_invocazione", "bedrock_agent")

        if tipo == "bedrock_agent":
            prompt = c["prompt"].replace("{id_pratica}", id_pratica)
            items.append({
                "controllo_id":     c["controllo_id"],
                "descrizione":      c.get("descrizione", ""),
                "macro_categoria":  c.get("macro_categoria", ""),
                "tipo_invocazione": "bedrock_agent",
                "agent_id":         c["agent_id"],
                "alias_id":         c["alias_id"],
                "prompt":           prompt,
            })

        elif tipo == "lambda":
            items.append({
                "controllo_id":         c["controllo_id"],
                "descrizione":          c.get("descrizione", ""),
                "macro_categoria":      c.get("macro_categoria", ""),
                "tipo_invocazione":     "lambda",
                "lambda_function_name": c["lambda_function_name"],
                "payload":              {"id_pratica": id_pratica},
            })

        else:
            logger.warning(f"tipo_invocazione non riconosciuto per '{c['controllo_id']}': '{tipo}' — skippato")

    logger.info(
        f"Payload SFN: {len(items)} controlli totali "
        f"({sum(1 for i in items if i['tipo_invocazione'] == 'bedrock_agent')} bedrock, "
        f"{sum(1 for i in items if i['tipo_invocazione'] == 'lambda')} lambda)"
    )
    return {
        "id_pratica": id_pratica,
        "controlli":  items,
    }


def invoke_step_functions(execution_name: str, payload: dict) -> dict:
    """
    Avvia l'Express Workflow in modalità sincrona e aspetta il completamento.
    Timeout massimo: 29s (limite API Gateway).
    Se i controlli sono lenti valutare il passaggio a pattern asincrono con polling.
    """
    logger.info(f"Avvio Step Functions sync execution: '{execution_name}'")
    try:
        response = sfn_client.start_sync_execution(
            stateMachineArn=SFN_ARN,
            name=execution_name,
            input=json.dumps(payload, ensure_ascii=False),
        )
    except ClientError as e:
        raise RuntimeError(f"Errore avvio Step Functions: {e}")

    status = response.get("status")
    logger.info(f"Step Functions terminato con status: '{status}'")

    if status == "FAILED":
        cause = response.get("cause", "sconosciuto")
        error = response.get("error", "")
        raise RuntimeError(f"Step Functions fallita — error: '{error}', cause: '{cause}'")

    if status == "TIMED_OUT":
        raise RuntimeError("Step Functions execution scaduta (timeout)")

    output = response.get("output")
    return json.loads(output) if output else {}


# ---------------------------------------------------------------------------
# Handler principale
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point Lambda.

    Payload atteso (body JSON da API GW o invocazione diretta):
    {
        "id_pratica":     "prt_test_20260402_006",
        "tipo_checklist": "ricostruzione_carriera_docenti"
    }

    Risposta in caso di successo:
    {
        "id_pratica":         "prt_test_20260402_006",
        "tipo_checklist":     "ricostruzione_carriera_docenti",
        "controlli_eseguiti": 20,
        "risultati": [
            {
                "controllo_id":    "corrispondenza_nome_cognome",
                "macro_categoria": "Corrispondenza dei dati anagrafici",
                "esito":           "PASS" | "FAIL" | "NON_VERIFICABILE",
                "motivazione":     "...",
                ...
            },
            ...
        ]
    }
    """
    logger.info(f"Event ricevuto: {json.dumps(event)}")

    # --- Parse input (API GW proxy integration o invocazione diretta) ---
    body = event
    if "body" in event:
        body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]

    id_pratica     = body.get("id_pratica", "").strip()
    tipo_checklist = body.get("tipo_checklist", "").strip()

    if not id_pratica or not tipo_checklist:
        return _response(400, {"error": "I campi 'id_pratica' e 'tipo_checklist' sono obbligatori"})

    try:
        # 1. Metadati pratica — necessari per valutare le condizioni
        metadata = get_pratica_metadata(id_pratica)

        # 2. Schema checklist da S3
        schema = load_checklist_schema(tipo_checklist)
        tutti_i_controlli = schema.get("controlli", [])
        logger.info(f"Controlli totali nello schema '{tipo_checklist}': {len(tutti_i_controlli)}")

        # 3. Filtro condizioni — es. salta classe concorso se scuola primaria
        controlli_attivi = [
            c for c in tutti_i_controlli
            if evaluate_condition(c.get("condizione"), metadata)
        ]
        logger.info(
            f"Controlli attivi: {len(controlli_attivi)} | "
            f"Saltati per condizione: {len(tutti_i_controlli) - len(controlli_attivi)}"
        )

        if not controlli_attivi:
            return _response(200, {
                "id_pratica":         id_pratica,
                "tipo_checklist":     tipo_checklist,
                "controlli_eseguiti": 0,
                "messaggio":          "Nessun controllo applicabile per questa pratica",
                "risultati":          [],
            })

        # 4. Costruzione payload Step Functions
        sfn_input = build_sfn_input(id_pratica, controlli_attivi)

        # 5. Invocazione Step Functions sync
        # Nome execution unico, max 80 chars
        execution_name = f"{id_pratica[:40]}-{context.aws_request_id[:8]}"
        sfn_result = invoke_step_functions(execution_name, sfn_input)

        # 6. Risposta finale
        return _response(200, {
            "id_pratica":         id_pratica,
            "tipo_checklist":     tipo_checklist,
            "controlli_eseguiti": len(controlli_attivi),
            "risultati":          sfn_result.get("risultati", []),
        })

    except ValueError as e:
        logger.error(f"Errore di validazione: {e}")
        return _response(404, {"error": str(e)})
    except RuntimeError as e:
        logger.error(f"Errore runtime: {e}")
        return _response(500, {"error": str(e)})
    except Exception as e:
        logger.exception("Errore imprevisto nel lambda_handler")
        return _response(500, {"error": f"Errore interno: {str(e)}"})


def _response(status_code: int, body: dict) -> dict:
    """Formatta la risposta HTTP compatibile con API Gateway proxy integration."""
    return {
        "statusCode": status_code,
        "headers":    {"Content-Type": "application/json"},
        "body":       json.dumps(body, ensure_ascii=False, default=str),
    }