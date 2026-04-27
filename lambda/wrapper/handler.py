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
import time
import uuid

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Client AWS ---
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime")
lambda_client         = boto3.client("lambda")

# --- Variabili d'ambiente ---
AWS_REGION       = os.environ.get("AWS_REGION", "eu-central-1")
BEDROCK_MAX_RETRIES = int(os.environ.get("BEDROCK_MAX_RETRIES", "5"))
BEDROCK_RETRY_DELAY_S = float(os.environ.get("BEDROCK_RETRY_DELAY_S", "3"))


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

    # Legge lo stream di completamento gestendo tutti i tipi di eventi
    completion_text = ""
    event_stream = response.get("completion", [])
    for event in event_stream:
        # Chunk di risposta testuale
        chunk = event.get("chunk", {})
        bytes_data = chunk.get("bytes", b"")
        if bytes_data:
            completion_text += bytes_data.decode("utf-8")
            continue

        # Errori restituiti nello stream (non sollevano ClientError)
        for error_key in (
            "internalServerException",
            "modelTimeoutException",
            "throttlingException",
            "accessDeniedException",
            "validationException",
            "badGatewayException",
            "dependencyFailedException",
        ):
            if error_key in event:
                err_msg = event[error_key].get("message", str(event[error_key]))
                raise RuntimeError(
                    f"Errore Bedrock stream [{error_key}] (agent={agent_id}): {err_msg}"
                )

        # returnControl: l'agente ha bisogno di un'azione esterna — non supportato
        if "returnControl" in event:
            raise RuntimeError(
                f"Il Bedrock agent {agent_id} ha restituito 'returnControl' "
                f"(action group return-of-control) — non supportato da questo wrapper"
            )

    if not completion_text:
        raise RuntimeError(
            f"Bedrock agent {agent_id} ha restituito una risposta vuota "
            f"(nessun chunk ricevuto nello stream)"
        )

    logger.info(f"Risposta Bedrock ricevuta ({len(completion_text)} chars): {completion_text[:500]!r}")
    return completion_text


_CTRL_ESCAPES = {'\n': '\\n', '\r': '\\r', '\t': '\\t'}

def _sanitize_json_string_values(s: str) -> str:
    """Escape unescaped control chars (newline, CR, tab) inside JSON string values."""
    result = []
    in_string = False
    escape_next = False
    for ch in s:
        if escape_next:
            escape_next = False
            result.append(ch)
            continue
        if ch == '\\' and in_string:
            escape_next = True
            result.append(ch)
            continue
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            continue
        if in_string and ch in _CTRL_ESCAPES:
            result.append(_CTRL_ESCAPES[ch])
            continue
        result.append(ch)
    return ''.join(result)


def _extract_first_json_object(testo: str) -> dict | None:
    """
    Trova il primo oggetto JSON valido nel testo usando scan a parentesi bilanciate.
    Gestisce stringhe con caratteri di escape; robusto rispetto a testo prima/dopo il JSON.
    """
    idx = 0
    while True:
        start = testo.find("{", idx)
        if start == -1:
            return None
        depth = 0
        in_string = False
        escape_next = False
        end = start
        for i, ch in enumerate(testo[start:], start):
            if escape_next:
                escape_next = False
                continue
            if ch == "\\" and in_string:
                escape_next = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        else:
            # Stream terminato senza chiudere le parentesi
            return None

        candidate = testo[start : end + 1]
        try:
            return json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            try:
                return json.loads(_sanitize_json_string_values(candidate))
            except (json.JSONDecodeError, ValueError):
                idx = start + 1  # prova dal prossimo '{'


def parse_risposta_agente(testo: str) -> dict:
    """
    Estrae un JSON valido dalla risposta testuale dell'agente.

    Strategia:
      1. Parse diretto (risposta già JSON puro)
      2. Blocco ```json ... ```
      3. Scan a parentesi bilanciate — trova il primo oggetto JSON valido nel testo
         (robusto rispetto a testo prima/dopo il JSON, array discordanze, ecc.)
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

    # 3. Scan a parentesi bilanciate
    result = _extract_first_json_object(testo)
    if result is not None:
        return result

    # 4. Fallback
    logger.warning(f"Impossibile parsare la risposta come JSON — restituito testo grezzo : {testo}")
    return {
        "esito":        "NON_VERIFICABILE",
        "motivazione":  "Risposta dell'agente non in formato JSON atteso",
        "testo_grezzo": testo,
    }


# ---------------------------------------------------------------------------
# Retry — invocazione Bedrock con backoff esponenziale
# ---------------------------------------------------------------------------

def invoke_bedrock_agent_with_retry(agent_id: str, alias_id: str, prompt: str) -> dict:
    """
    Chiama invoke_bedrock_agent + parse_risposta_agente con retry.

    Ritenta se:
      - invoke_bedrock_agent solleva RuntimeError (risposta vuota, errore stream)
      - parse_risposta_agente restituisce un fallback con 'testo_grezzo' (risposta non JSON)

    Backoff: BEDROCK_RETRY_DELAY_S * 2^attempt (es. 3s, 6s con MAX_RETRIES=2).
    Dopo tutti i tentativi, propaga l'ultima eccezione o restituisce il fallback.
    """
    last_exc = None
    last_risposta = None

    for attempt in range(BEDROCK_MAX_RETRIES + 1):
        if attempt > 0:
            delay = BEDROCK_RETRY_DELAY_S * (2 ** (attempt - 1))
            logger.warning(
                f"Retry {attempt}/{BEDROCK_MAX_RETRIES} per agent={agent_id} "
                f"(attesa {delay:.1f}s)"
            )
            time.sleep(delay)

        try:
            testo = invoke_bedrock_agent(agent_id, alias_id, prompt)
        except RuntimeError as e:
            last_exc = e
            logger.warning(f"Tentativo {attempt + 1} fallito: {e}")
            continue

        risposta = parse_risposta_agente(testo)

        # Parse riuscito — risposta JSON valida dall'agente
        if "testo_grezzo" not in risposta:
            return risposta

        # Parse fallito — registra e riprova
        last_risposta = risposta
        logger.warning(f"Tentativo {attempt + 1}: risposta non parsabile come JSON, retry.")

    # Tutti i tentativi esauriti
    if last_exc is not None:
        raise last_exc
    return last_risposta


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

            risposta = invoke_bedrock_agent_with_retry(agent_id, alias_id, prompt)

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
            "descrizione":     event.get("descrizione", ""),
            "risposta_agente": risposta,
        }

    except Exception as e:
        logger.error(f"Errore nel wrapper per controllo '{controllo_id}': {e}", exc_info=True)
        # Non solleva l'eccezione — restituisce esito NON_VERIFICABILE
        # così il Map State non si blocca e gli altri controlli continuano
        return {
            "controllo_id":    controllo_id,
            "macro_categoria": macro_categoria,
            "descrizione":     event.get("descrizione", ""),
            "risposta_agente": {
                "esito":       "NON_VERIFICABILE",
                "motivazione": f"Errore durante l'esecuzione del controllo: {str(e)}",
            },
        }