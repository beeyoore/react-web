"""
Lambda: get_pratica
GET /pratica/{id_pratica}

Legge il METADATA dalla tabella RTS_Pratiche e i controlli dalla tabella Controlli.

Risposta 200:
{
  "id_pratica": "MARIO_ROSSI_20260421T143022Z",
  "status": "CREATA",
  "tipo_servizio": "controlli",
  "created_at": "...",
  "flag_completezza": false,
  "categoria_personale": "docente",
  "controlli_preliminari": [
    {
      "id": "verifica_decreto_ricostruzione",
      "nome": "Verifica presenza decreto ricostruzione carriera",
      "esito": "non_superato",
      "convalidato": false,
      "aggiornato_at": "..."
    },
    ...
  ],
  "controlli_amm_contabili": []
}

Env vars:
  DYNAMODB_TABLE   – default: RTS_Pratiche  (METADATA pratica)
  CONTROLLI_TABLE  – default: Controlli     (item controlli)
  REGION           – default: eu-central-1
"""

import json
import os

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

REGION = os.environ.get("REGION", "eu-central-1")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "RTS_Pratiche")
CONTROLLI_TABLE = os.environ.get("CONTROLLI_TABLE", "Controlli")

dynamodb = boto3.resource("dynamodb", region_name=REGION)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Content-Type": "application/json",
}


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body, ensure_ascii=False, default=str),
    }


def query_metadata(id_pratica: str) -> dict | None:
    table = dynamodb.Table(DYNAMODB_TABLE)
    resp = table.get_item(Key={"PK": f"PRATICA#{id_pratica}", "SK": "METADATA"})
    return resp.get("Item")


def query_controlli(id_pratica: str) -> list[dict]:
    table = dynamodb.Table(CONTROLLI_TABLE)
    kwargs = {"KeyConditionExpression": Key("PK").eq(f"PRATICA#{id_pratica}")}
    items = []
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def build_controllo_item(item: dict) -> dict:
    return {
        "id": item.get("SK", "").split("#", 1)[-1],
        "nome": item.get("nome", ""),
        "esito": item.get("esito", "non_avviato"),
        "convalidato": item.get("convalidato", False),
        "aggiornato_at": item.get("aggiornato_at"),
    }


def lambda_handler(event: dict, context) -> dict:
    print("EVENT:", json.dumps(event, default=str))

    # Support both REST API (v1) and HTTP API (v2) payload formats
    http_method = event.get("httpMethod") or (
        event.get("requestContext", {}).get("http", {}).get("method", "")
    )
    if http_method == "OPTIONS":
        return response(200, {})

    path_params = event.get("pathParameters") or {}
    id_pratica = path_params.get("id_pratica", "").strip()
    print(f"id_pratica: {id_pratica!r}")

    if not id_pratica:
        return response(400, {"error": "id_pratica mancante nel path"})

    try:
        metadata = query_metadata(id_pratica)
    except ClientError as e:
        return response(500, {"error": f"Errore DynamoDB (pratiche): {e.response['Error']['Message']}"})

    if not metadata:
        return response(404, {"error": f"Pratica {id_pratica} non trovata"})

    try:
        controlli_items = query_controlli(id_pratica)
    except ClientError as e:
        return response(500, {"error": f"Errore DynamoDB (controlli): {e.response['Error']['Message']}"})

    controlli_preliminari = []
    controlli_amm_contabili = []

    for item in controlli_items:
        sk = item.get("SK", "")
        if sk.startswith("PRELIMINARE#"):
            controlli_preliminari.append(build_controllo_item(item))
        elif sk.startswith("AMM_CONTABILE#"):
            controlli_amm_contabili.append(build_controllo_item(item))

    controlli_preliminari.sort(key=lambda c: c["id"])
    controlli_amm_contabili.sort(key=lambda c: c["id"])

    return response(200, {
        "id_pratica": metadata.get("id_pratica", id_pratica),
        "user_id": metadata.get("user_id"),
        "tipo_servizio": metadata.get("tipo_servizio"),
        "status": metadata.get("status"),
        "created_at": metadata.get("created_at"),
        "flag_completezza": metadata.get("flag_completezza", False),
        "categoria_personale": metadata.get("categoria_personale"),
        "controlli_preliminari": controlli_preliminari,
        "controlli_amm_contabili": controlli_amm_contabili,
    })
