"""
Lambda: apri_pratica
POST /apri-pratica

Body JSON:
{
  "user_id":       "MARIO_ROSSI",           ← obbligatorio
  "tipo_servizio": "controlli" | "stipendi" ← obbligatorio
  "file_names":    ["doc.pdf", "atto.p7m"]  ← lista nomi file da caricare
}

Risposta 200:
{
  "id_pratica": "MARIO_ROSSI_20260421T143022Z",
  "status": "CREATA",
  "presigned_urls": [
    { "nome": "doc.pdf",  "url": "https://s3.amazonaws.com/..." },
    { "nome": "atto.p7m", "url": "https://s3.amazonaws.com/..." }
  ]
}

Il frontend usa i presigned_urls per fare PUT direttamente su S3,
senza passare per API Gateway (nessun limite di payload).

Env vars (opzionali, hanno default):
  DYNAMODB_TABLE      – default: RTS_Pratiche
  S3_BUCKET           – default: igit-chcklstai-s3-input-euc1
  S3_PREFIX           – default: pratiche
  REGION              – default: eu-central-1
  PRESIGNED_URL_EXPIRY – secondi validità URL, default: 300
"""

import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("REGION", "eu-central-1")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "RTS_Pratiche")
S3_BUCKET = os.environ.get("S3_BUCKET", "igit-chcklstai-s3-input-euc1")
S3_PREFIX = os.environ.get("S3_PREFIX", "pratiche")
PRESIGNED_URL_EXPIRY = int(os.environ.get("PRESIGNED_URL_EXPIRY", "300"))

dynamodb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
}


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body, ensure_ascii=False),
    }


ALLOWED_TIPI = {"controlli", "stipendi"}


def create_dynamodb_entry(id_pratica: str, user_id: str, tipo_servizio: str, documenti_attesi: int) -> None:
    table = dynamodb.Table(DYNAMODB_TABLE)
    table.put_item(
        Item={
            "PK": f"PRATICA#{id_pratica}",
            "SK": "METADATA",
            "id_pratica": id_pratica,
            "user_id": user_id,
            "tipo_servizio": tipo_servizio,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "status": "CREATA",
            "documenti_attesi": documenti_attesi,
        }
    )


def generate_presigned_url(id_pratica: str, tipo_servizio: str, nome: str) -> str:
    key = f"{S3_PREFIX}/{tipo_servizio}/{id_pratica}/input/{nome}"
    return s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=PRESIGNED_URL_EXPIRY,
    )


def lambda_handler(event: dict, context) -> dict:
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        body = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return response(400, {"error": "Body JSON non valido"})

    user_id = body.get("user_id", "").strip()
    tipo_servizio = body.get("tipo_servizio", "").strip().lower()
    file_names = body.get("file_names", [])

    if not user_id:
        return response(400, {"error": "user_id mancante nel body"})

    if tipo_servizio not in ALLOWED_TIPI:
        return response(400, {"error": f"tipo_servizio deve essere uno tra: {', '.join(ALLOWED_TIPI)}"})

    if not file_names:
        return response(400, {"error": "file_names mancante o vuoto"})

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    id_pratica = f"{user_id}_{timestamp}"

    try:
        create_dynamodb_entry(id_pratica, user_id, tipo_servizio, len(file_names))
    except ClientError as e:
        return response(500, {"error": f"Errore DynamoDB: {e.response['Error']['Message']}"})

    presigned_urls = []
    for nome in file_names:
        try:
            url = generate_presigned_url(id_pratica, tipo_servizio, nome)
            presigned_urls.append({"nome": nome, "url": url})
        except ClientError as e:
            return response(500, {"error": f"Errore generazione URL per {nome}: {e.response['Error']['Message']}"})

    return response(200, {
        "id_pratica": id_pratica,
        "status": "CREATA",
        "presigned_urls": presigned_urls,
    })
