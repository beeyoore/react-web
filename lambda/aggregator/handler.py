"""
Result Aggregator Lambda.

Riceve l'output del Map State di Step Functions (array di risultati dei controlli)
e per ogni controllo fa un update_item sulla tabella DynamoDB creata dal FE,
aggiungendo i campi prodotti dall'agente (motivazione, dettagli, ecc.).

Struttura chiavi DynamoDB:
  PK: "PRATICA#{id_pratica}"         es. "PRATICA#MARIO_ROSSI_20260422T105352Z"
  SK: "PRELIMINARE#{controllo_id}"   es. "PRELIMINARE#corrispondenza_nome_cognome"

Campi aggiornati:
  - esito            (PASS/FAIL/NON_VERIFICABILE → normalizzato a superato/non_superato/non_verificabile)
  - motivazione      (spiegazione testuale dall'agente)
  - dettaglio        (JSON completo della risposta agente, per audit)
  - aggiornato_at    (timestamp aggiornamento)
  - elaborato        (bool, True quando il Lambda ha scritto il risultato)
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Client AWS ---
dynamodb = boto3.resource("dynamodb")

# --- Variabili d'ambiente ---
RISULTATI_TABLE = os.environ["RISULTATI_TABLE"]   # tabella DynamoDB creata dal FE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalizza_esito(esito_raw: str | None) -> str:
    """
    Normalizza l'esito dell'agente al formato atteso dalla tabella DynamoDB.

    Mapping:
      PASS / SUPERATO / superato       → "superato"
      FAIL / NON_SUPERATO / non_superato → "non_superato"
      NON_VERIFICABILE / *             → "non_verificabile"
    """
    if not esito_raw:
        return "non_verificabile"

    esito_upper = esito_raw.strip().upper()

    if esito_upper in ("PASS", "SUPERATO"):
        return "superato"
    if esito_upper in ("FAIL", "NON_SUPERATO", "FAILED"):
        return "non_superato"

    return "non_verificabile"


def estrai_campi_risposta(risultato: dict) -> tuple[str, str, str]:
    """
    Estrae esito, motivazione e dettaglio dal risultato di un controllo.

    Il risultato può avere due forme:
      1. Risposta diretta agente Bedrock:
         { "esito": "PASS", "motivazione": "...", ... }
      2. Wrapper con risposta annidata:
         { "controllo_id": "...", "risposta_agente": { "esito": "PASS", ... } }

    Restituisce: (esito_normalizzato, motivazione, dettaglio_json)
    """
    # Caso 2: risposta annidata nel wrapper
    risposta = risultato.get("risposta_agente", risultato)

    # Se la risposta è una stringa JSON, prova a parsarla
    if isinstance(risposta, str):
        try:
            risposta = json.loads(risposta)
        except (json.JSONDecodeError, TypeError):
            # Risposta testuale non parsabile — trattata come motivazione
            return "non_verificabile", risposta, risposta

    esito_raw   = risposta.get("esito") or risposta.get("id_controllo") and risposta.get("esito")
    motivazione = (
        risposta.get("motivazione")
        or risposta.get("message")
        or risposta.get("messaggio")
        or "Nessuna motivazione disponibile"
    )
    dettaglio = json.dumps(risposta, ensure_ascii=False, default=str)

    return normalizza_esito(esito_raw), motivazione, dettaglio


def aggiorna_controllo_dynamo(
    table,
    id_pratica: str,
    controllo_id: str,
    esito: str,
    motivazione: str,
    dettaglio: str,
    timestamp: str,
) -> None:
    """
    Esegue update_item su DynamoDB per aggiornare il risultato di un controllo.
    Aggiunge i campi prodotti dall'agente senza sovrascrivere quelli esistenti
    (nome, convalidato, ecc.) che appartengono al FE.
    """
    pk = f"PRATICA#{id_pratica}"
    sk = f"PRELIMINARE#{controllo_id}"

    try:
        table.update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression=(
                "SET esito = :esito, "
                "motivazione = :motivazione, "
                "dettaglio = :dettaglio, "
                "aggiornato_at = :ts, "
                "elaborato = :elaborato"
            ),
            ExpressionAttributeValues={
                ":esito":      esito,
                ":motivazione": motivazione,
                ":dettaglio":  dettaglio,
                ":ts":         timestamp,
                ":elaborato":  True,
            },
            # Aggiorna solo se l'item esiste già (creato dal FE)
            ConditionExpression="attribute_exists(PK)",
        )
        logger.info(f"Aggiornato: PK={pk} SK={sk} esito={esito}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(
                f"Item non trovato in DynamoDB: PK={pk} SK={sk} — "
                f"il FE potrebbe non aver ancora creato il record"
            )
        else:
            logger.error(f"Errore DynamoDB update_item PK={pk} SK={sk}: {e}")
            raise


# ---------------------------------------------------------------------------
# Handler principale
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point Lambda — invocato da Step Functions dopo il Map State.

    Payload atteso da Step Functions:
    {
        "id_pratica": "MARIO_ROSSI_20260422T105352Z",
        "risultati": [
            {
                "controllo_id":    "corrispondenza_nome_cognome",
                "macro_categoria": "Corrispondenza dei dati anagrafici",
                "risposta_agente": {
                    "esito":       "PASS",
                    "motivazione": "Nome e cognome corrispondenti in tutti i documenti",
                    ...
                }
            },
            ...
        ]
    }

    Risposta:
    {
        "id_pratica":   "MARIO_ROSSI_20260422T105352Z",
        "aggiornati":   20,
        "errori":       0,
        "timestamp":    "2026-04-22T10:54:23Z"
    }
    """
    logger.info(f"Event ricevuto: {json.dumps(event, default=str)}")

    id_pratica = event.get("id_pratica", "").strip()
    risultati  = event.get("risultati", [])

    if not id_pratica:
        raise ValueError("Campo 'id_pratica' mancante nel payload")

    if not risultati:
        logger.warning(f"Nessun risultato da aggregare per pratica '{id_pratica}'")
        return {"id_pratica": id_pratica, "aggiornati": 0, "errori": 0}

    table     = dynamodb.Table(RISULTATI_TABLE)
    timestamp = datetime.now(timezone.utc).isoformat()
    aggiornati = 0
    errori     = 0

    for risultato in risultati:
        controllo_id = risultato.get("controllo_id", "")
        if not controllo_id:
            logger.warning(f"Risultato senza controllo_id, skippato: {risultato}")
            errori += 1
            continue

        try:
            esito, motivazione, dettaglio = estrai_campi_risposta(risultato)
            aggiorna_controllo_dynamo(
                table       = table,
                id_pratica  = id_pratica,
                controllo_id= controllo_id,
                esito       = esito,
                motivazione = motivazione,
                dettaglio   = dettaglio,
                timestamp   = timestamp,
            )
            aggiornati += 1

        except Exception as e:
            logger.error(f"Errore aggiornamento controllo '{controllo_id}': {e}")
            errori += 1
            # Continua con gli altri controlli — un errore non blocca il batch
            continue

    logger.info(
        f"Aggregazione completata per pratica '{id_pratica}': "
        f"{aggiornati} aggiornati, {errori} errori"
    )

    return {
        "id_pratica":  id_pratica,
        "aggiornati":  aggiornati,
        "errori":      errori,
        "timestamp":   timestamp,
    }