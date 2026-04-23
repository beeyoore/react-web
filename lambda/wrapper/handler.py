"""
Bedrock Agent Wrapper Lambda.

Invocato dal Map State di Step Functions per ogni controllo della checklist.
Gestisce due tipi di invocazione in base al campo 'tipo_invocazione':

  - "bedrock_agent": invoca un agente Bedrock tramite invoke_agent e
                     restituisce il JSON di esito parsato dalla risposta
  - "lambda":        invoca direttamente una Lambda (es. verifica-abilitazione,
                     classifica-assenze) e restituisce il suo output

Output standardizzato per il Result Aggregator:
{
    "controllo_id":    str,
    "macro_categoria": str,
    "risposta_agente": dict   ← JSON di esito dell'agente/lambda
}
"""

import json
import logging
import os
import re
import uuid

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Client AWS ---
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime")
lambda_client         = boto3.client("lambda")

# --- Variabili d'ambiente ---
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")


# ---------------------------------------------------------------------------
# Helpers — invocazione Bedrock Agent
# ---------------------------------------------------------------------------

def invoke_bedrock_agent(agent_id: str, alias_id: str, prompt: str) -> str:
    """
    Invoca un agente Bedrock e restituisce la risposta testuale completa.
    Gestisce lo streaming della risposta tramite EventStream.
    """
    session_id = str(uuid.uuid4())
    logger.info(f"Invocazione Bedrock agent_id={agent_id} alias_id={alias_id} session={session_id}")

    try:
        response = bedrock_agent_runtime.invoke_agent(
            agentId       = agent_id,
            agentAliasId  = alias_id,
            sessionId     = session_id,
            inputText     = prompt,
        )
    except ClientError as e:
        raise RuntimeError(f"Errore invoke_agent (agent={agent_id}): {e}")

    # Legge lo stream di completamento
    completion_text = ""
    event_stream = response.get("completion", [])
    for event in event_stream:
        chunk = event.get("chunk", {})
        bytes_data = chunk.get("bytes", b"")
        if bytes_data:
            completion_text += bytes_data.decode("utf-8")

    logger.info(f"Risposta Bedrock ricevuta ({len(completion_text)} chars)")
    return completion_text


def parse_risposta_agente(testo: str) -> dict:
    """
    Tenta di estrarre un JSON valido dalla risposta testuale dell'agente.

    Strategia:
      1. Parse diretto (se la risposta è già JSON puro)
      2. Estrazione dal primo blocco ```json ... ``` trovato
      3. Ricerca del primo oggetto JSON { ... } nel testo
      4. Fallback: restituisce la risposta come campo 'testo_grezzo'
    """
    testo = testo.strip()

    # 1. Parse diretto
    try:
        return json.loads(testo)
    except (json.JSONDecodeError, ValueError):
        pass

    # 2. Blocco ```json ... ```
    match = re.search(r"```json\s*(.*?)\s*```", testo, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            pass

    # 3. Primo oggetto JSON nel testo
    match = re.search(r"\{.*\}", testo, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except (json.JSONDecodeError, ValueError):
            pass

    # 4. Fallback
    logger.warning("Impossibile parsare la risposta come JSON — restituito testo grezzo")
    return {
        "esito":       "NON_VERIFICABILE",
        "motivazione": "Risposta dell'agente non in formato JSON atteso",
        "testo_grezzo": testo,
    }


# ---------------------------------------------------------------------------
# Helpers — invocazione Lambda diretta
# ---------------------------------------------------------------------------

def invoke_lambda(function_name: str, payload: dict) -> dict:
    """
    Invoca una Lambda direttamente (RequestResponse sincrono)
    e restituisce il body parsato della risposta.
    """
    logger.info(f"Invocazione Lambda diretta: {function_name}")
    try:
        response = lambda_client.invoke(
            FunctionName   = function_name,
            InvocationType = "RequestResponse",
            Payload        = json.dumps(payload).encode("utf-8"),
        )
    except ClientError as e:
        raise RuntimeError(f"Errore invoke Lambda '{function_name}': {e}")

    # Legge il payload di risposta
    response_payload = json.loads(response["Payload"].read().decode("utf-8"))

    # Gestisce il caso in cui la Lambda restituisca un body annidato (API GW style)
    if "body" in response_payload:
        body = response_payload["body"]
        if isinstance(body, str):
            try:
                return json.loads(body)
            except (json.JSONDecodeError, ValueError):
                return {"esito": "NON_VERIFICABILE", "motivazione": body}
        return body

    return response_payload


# ---------------------------------------------------------------------------
# Handler principale
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point Lambda — invocato dal Map State di Step Functions.

    Payload atteso per bedrock_agent:
    {
        "controllo_id":     "corrispondenza_nome_cognome",
        "descrizione":      "Corrispondenza del nome e cognome",
        "macro_categoria":  "Corrispondenza dei dati anagrafici",
        "tipo_invocazione": "bedrock_agent",
        "agent_id":         "CN875JR89B",
        "alias_id":         "X1NQB7LGQN",
        "prompt":           "id_pratica: MARIO_ROSSI_... Controllo: ..."
    }

    Payload atteso per lambda:
    {
        "controllo_id":          "verifica_assenze_interruttive",
        "descrizione":           "Verifica delle assenze interruttive",
        "macro_categoria":       "Riconoscibilità dei servizi e delle assenze",
        "tipo_invocazione":      "lambda",
        "lambda_function_name":  "classifica-assenze",
        "payload":               {"id_pratica": "MARIO_ROSSI_..."}
    }

    Output (stesso formato per entrambi i tipi):
    {
        "controllo_id":    "corrispondenza_nome_cognome",
        "macro_categoria": "Corrispondenza dei dati anagrafici",
        "risposta_agente": {
            "esito":       "PASS",
            "motivazione": "Nome e cognome corrispondenti in tutti i documenti",
            ...
        }
    }
    """
    logger.info(f"Wrapper invocato per controllo: {event.get('controllo_id')}")

    controllo_id    = event.get("controllo_id", "sconosciuto")
    macro_categoria = event.get("macro_categoria", "")
    tipo            = event.get("tipo_invocazione", "bedrock_agent")

    try:
        if tipo == "bedrock_agent":
            agent_id = event.get("agent_id")
            alias_id = event.get("alias_id")
            prompt   = event.get("prompt")

            if not all([agent_id, alias_id, prompt]):
                raise ValueError(
                    f"Campi mancanti per bedrock_agent: "
                    f"agent_id={agent_id}, alias_id={alias_id}, prompt={'presente' if prompt else 'MANCANTE'}"
                )

            testo_risposta = invoke_bedrock_agent(agent_id, alias_id, prompt)
            risposta       = parse_risposta_agente(testo_risposta)

        elif tipo == "lambda":
            function_name = event.get("lambda_function_name")
            payload       = event.get("payload", {})

            if not function_name:
                raise ValueError("Campo 'lambda_function_name' mancante per tipo_invocazione=lambda")

            risposta = invoke_lambda(function_name, payload)

        else:
            raise ValueError(f"tipo_invocazione non riconosciuto: '{tipo}'")

        logger.info(f"Controllo '{controllo_id}' completato — esito: {risposta.get('esito', 'N/A')}")

        return {
            "controllo_id":    controllo_id,
            "macro_categoria": macro_categoria,
            "risposta_agente": risposta,
        }

    except Exception as e:
        logger.error(f"Errore nel wrapper per controllo '{controllo_id}': {e}")
        # Non solleva l'eccezione — restituisce esito NON_VERIFICABILE
        # così il Map State non si blocca e gli altri controlli continuano
        return {
            "controllo_id":    controllo_id,
            "macro_categoria": macro_categoria,
            "risposta_agente": {
                "esito":       "NON_VERIFICABILE",
                "motivazione": f"Errore durante l'esecuzione del controllo: {str(e)}",
            },
        }