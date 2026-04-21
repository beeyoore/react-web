"""
Lambda: apri_pratica
POST /apri-pratica

Body JSON:
{
  "user_id": "MARIO_ROSSI",   ← obbligatorio, passato dal frontend
  "documenti": [
    { "nome": "doc.pdf", "contenuto": "<base64>" },
    ...
  ]
}

Env vars (opzionali, hanno default):
  DYNAMODB_TABLE – default: RTS_Pratiche
  S3_BUCKET      – default: igit-chcklstai-s3-input-euc1
  S3_PREFIX      – default: pratiche
  REGION         – default: eu-central-1
"""

import json
import base64
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("REGION", "eu-central-1")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "RTS_Pratiche")
S3_BUCKET = os.environ.get("S3_BUCKET", "igit-chcklstai-s3-input-euc1")
S3_PREFIX = os.environ.get("S3_PREFIX", "pratiche")

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


def create_dynamodb_entry(id_pratica: str, user_id: str) -> None:
    table = dynamodb.Table(DYNAMODB_TABLE)
    table.put_item(
        Item={
            "PK": f"PRATICA#{id_pratica}",
            "SK": "METADATA",
            "id_pratica": id_pratica,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "status": "CREATA",
        }
    )


def upload_document(id_pratica: str, nome: str, contenuto_b64: str) -> None:
    data = base64.b64decode(contenuto_b64)
    key = f"{S3_PREFIX}/{id_pratica}/input/{nome}"
    content_type = "application/pdf" if nome.lower().endswith(".pdf") else "application/octet-stream"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)


def lambda_handler(event: dict, context) -> dict:
    # Preflight CORS
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        body = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        return response(400, {"error": "Body JSON non valido"})

    user_id = body.get("user_id", "").strip()
    documenti = body.get("documenti", [])

    if not user_id:
        return response(400, {"error": "user_id mancante nel body"})

    if not documenti:
        return response(400, {"error": "Nessun documento ricevuto"})

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    id_pratica = f"{user_id}_{timestamp}"

    try:
        create_dynamodb_entry(id_pratica, user_id)
    except ClientError as e:
        return response(500, {"error": f"Errore DynamoDB: {e.response['Error']['Message']}"})

    errori_upload = []
    for doc in documenti:
        nome = doc.get("nome", "documento")
        contenuto_b64 = doc.get("contenuto", "")
        if not contenuto_b64:
            errori_upload.append(nome)
            continue
        try:
            upload_document(id_pratica, nome, contenuto_b64)
        except ClientError as e:
            errori_upload.append(nome)

    if errori_upload:
        return response(
            207,
            {
                "id_pratica": id_pratica,
                "status": "CREATA",
                "warning": f"Upload fallito per: {', '.join(errori_upload)}",
            },
        )

    return response(200, {"id_pratica": id_pratica, "status": "CREATA"})
