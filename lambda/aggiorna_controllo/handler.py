"""
Lambda: aggiorna_controllo
PATCH /controllo/{id_pratica}

Gestisce due azioni sui controlli tramite il campo "action":

  action = "convalida"
    Imposta convalidato=True su N controlli.
    Body:
    {
      "action":        "convalida",
      "tipo":          "preliminare" | "amm_contabile",
      "controllo_ids": ["id1", "id2", ...]
    }
    Risposta 200: { "updated": ["id1", "id2", ...] }

  action = "modifica"
    Aggiorna esito e motivazione di un singolo controllo (modifica manuale
    operatore). Resetta convalidato=False.
    Body:
    {
      "action":      "modifica",
      "controllo_id": "nome_del_controllo",
      "esito":        "superato" | "non_superato" | "non_verificabile",
      "motivazione":  "testo libero",
      "tipo":         "amm_contabile"   -- opzionale, default: amm_contabile
    }
    Risposta 200: { "updated": "nome_del_controllo" }

Env vars:
  CONTROLLI_TABLE  – default: Controlli
  REGION           – default: eu-central-1
"""

import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("REGION", "eu-central-1")
CONTROLLI_TABLE = os.environ.get("CONTROLLI_TABLE", "Controlli")

dynamodb = boto3.resource("dynamodb", region_name=REGION)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "PATCH, OPTIONS",
    "Content-Type": "application/json",
}

SK_PREFIX = {
    "preliminare":   "PRELIMINARE#",
    "amm_contabile": "AMM_CONTABILE#",
}

ESITI_VALIDI = {"superato", "non_superato", "non_verificabile"}


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body, ensure_ascii=False, default=str),
    }


def handle_convalida(body: dict, id_pratica: str, table, now: str) -> dict:
    tipo: str = body.get("tipo", "").strip()
    if tipo not in SK_PREFIX:
        return response(400, {"error": f"tipo non valido: '{tipo}'. Valori accettati: {list(SK_PREFIX)}"})

    controllo_ids: list = body.get("controllo_ids", [])
    if not controllo_ids:
        return response(400, {"error": "controllo_ids mancante o vuoto"})

    sk_prefix = SK_PREFIX[tipo]
    updated = []

    for ctrl_id in controllo_ids:
        sk = f"{sk_prefix}{ctrl_id}"
        try:
            table.update_item(
                Key={"PK": f"PRATICA#{id_pratica}", "SK": sk},
                UpdateExpression="SET convalidato = :val, aggiornato_at = :ts",
                ExpressionAttributeValues={":val": True, ":ts": now},
                ConditionExpression="attribute_exists(PK)",
            )
            updated.append(ctrl_id)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ConditionalCheckFailedException":
                return response(404, {"error": f"Controllo non trovato: {sk}"})
            return response(500, {"error": f"Errore DynamoDB: {e.response['Error']['Message']}"})

    return response(200, {"updated": updated})


def handle_modifica(body: dict, id_pratica: str, table, now: str) -> dict:
    controllo_id: str = body.get("controllo_id", "").strip()
    if not controllo_id:
        return response(400, {"error": "controllo_id mancante"})

    esito: str = body.get("esito", "").strip()
    if esito not in ESITI_VALIDI:
        return response(400, {"error": f"esito non valido: '{esito}'. Valori accettati: {sorted(ESITI_VALIDI)}"})

    motivazione: str = body.get("motivazione", "").strip()
    if not motivazione:
        return response(400, {"error": "motivazione mancante"})

    tipo: str = body.get("tipo", "amm_contabile").strip()
    if tipo not in SK_PREFIX:
        return response(400, {"error": f"tipo non valido: '{tipo}'. Valori accettati: {list(SK_PREFIX)}"})

    sk = f"{SK_PREFIX[tipo]}{controllo_id}"

    try:
        table.update_item(
            Key={"PK": f"PRATICA#{id_pratica}", "SK": sk},
            UpdateExpression=(
                "SET esito = :esito, "
                "motivazione = :motivazione, "
                "modificato = :modificato, "
                "modificato_at = :ts, "
                "aggiornato_at = :ts, "
                "convalidato = :convalidato"
            ),
            ExpressionAttributeValues={
                ":esito":       esito,
                ":motivazione": motivazione,
                ":modificato":  True,
                ":ts":          now,
                ":convalidato": False,
            },
            ConditionExpression="attribute_exists(PK)",
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ConditionalCheckFailedException":
            return response(404, {"error": f"Controllo non trovato: {sk}"})
        return response(500, {"error": f"Errore DynamoDB: {e.response['Error']['Message']}"})

    return response(200, {"updated": controllo_id})


def lambda_handler(event: dict, context) -> dict:
    print("EVENT:", json.dumps(event, default=str))

    http_method = event.get("httpMethod") or (
        event.get("requestContext", {}).get("http", {}).get("method", "")
    )
    if http_method == "OPTIONS":
        return response(200, {})

    path_params = event.get("pathParameters") or {}
    id_pratica = path_params.get("id_pratica", "").strip()
    if not id_pratica:
        return response(400, {"error": "id_pratica mancante nel path"})

    raw_body = event.get("body") or "{}"
    if isinstance(raw_body, str):
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            return response(400, {"error": "Body JSON non valido"})
    else:
        body = raw_body

    action: str = body.get("action", "").strip()
    if action not in ("convalida", "modifica"):
        return response(400, {"error": f"action non valida: '{action}'. Valori accettati: convalida, modifica"})

    table = dynamodb.Table(CONTROLLI_TABLE)
    now = datetime.now(timezone.utc).isoformat()

    if action == "convalida":
        return handle_convalida(body, id_pratica, table, now)
    return handle_modifica(body, id_pratica, table, now)
