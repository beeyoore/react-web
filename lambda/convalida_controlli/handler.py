"""
Lambda: convalida_controlli
PATCH /convalida/{id_pratica}

Imposta convalidato=True per i controlli specificati.
Supporta sia i controlli preliminari che quelli amministrativo-contabili
tramite il campo "tipo" nel body.

Body JSON:
{
  "tipo": "preliminare",          -- obbligatorio: "preliminare" | "amm_contabile"
  "controllo_ids": ["verifica_decreto_ricostruzione", ...]
}

Risposta 200:
{
  "updated": ["verifica_decreto_ricostruzione", ...]
}

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

# Mappa tipo → prefisso SK nella tabella Controlli
SK_PREFIX = {
    "preliminare":    "PRELIMINARE#",
    "amm_contabile":  "AMM_CONTABILE#",
}


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body, ensure_ascii=False, default=str),
    }


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

    # Parse body
    raw_body = event.get("body") or "{}"
    if isinstance(raw_body, str):
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError:
            return response(400, {"error": "Body JSON non valido"})
    else:
        body = raw_body

    tipo: str = body.get("tipo", "").strip()
    if tipo not in SK_PREFIX:
        return response(400, {"error": f"tipo non valido: '{tipo}'. Valori accettati: {list(SK_PREFIX)}"})

    controllo_ids: list[str] = body.get("controllo_ids", [])
    if not controllo_ids:
        return response(400, {"error": "controllo_ids mancante o vuoto"})

    sk_prefix = SK_PREFIX[tipo]
    table = dynamodb.Table(CONTROLLI_TABLE)
    now = datetime.now(timezone.utc).isoformat()
    updated = []

    for ctrl_id in controllo_ids:
        sk = f"{sk_prefix}{ctrl_id}"
        try:
            table.update_item(
                Key={"PK": f"PRATICA#{id_pratica}", "SK": sk},
                UpdateExpression="SET convalidato = :val, aggiornato_at = :ts",
                ExpressionAttributeValues={":val": True, ":ts": now},
                ConditionExpression="attribute_exists(PK)",  # item must exist
            )
            updated.append(ctrl_id)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ConditionalCheckFailedException":
                return response(404, {"error": f"Controllo non trovato: {sk}"})
            return response(500, {"error": f"Errore DynamoDB: {e.response['Error']['Message']}"})

    return response(200, {"updated": updated})
