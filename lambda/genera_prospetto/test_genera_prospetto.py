"""
Script di test locale per genera_prospetto.
Simula i dati DynamoDB e genera il prospetto Excel senza AWS.

Uso:
  python test_genera_prospetto.py

Output: prospetto_test.xlsx nella directory corrente.
"""

import io
import os
import sys
import pathlib

# --- stub boto3/botocore prima dell'import del handler ---
import types

_fake_boto3 = types.ModuleType("boto3")
_fake_boto3.resource = lambda *a, **k: None
_fake_boto3.client = lambda *a, **k: None
_fake_dynamo = types.ModuleType("boto3.dynamodb")
_fake_cond = types.ModuleType("boto3.dynamodb.conditions")


class _Key:
    def __init__(self, name):
        self._name = name
    def eq(self, v): return self
    def begins_with(self, v): return self
    def __and__(self, other): return self


_fake_cond.Key = _Key
_fake_botocore = types.ModuleType("botocore")
_fake_exc = types.ModuleType("botocore.exceptions")
_fake_exc.ClientError = Exception
sys.modules.update({
    "boto3": _fake_boto3,
    "boto3.dynamodb": _fake_dynamo,
    "boto3.dynamodb.conditions": _fake_cond,
    "botocore": _fake_botocore,
    "botocore.exceptions": _fake_exc,
})

os.environ.setdefault("TEMPLATE_BUCKET", "fake-bucket")

# --- import handler ---
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from handler import _build_placeholder_map, fill_excel, _normalizza_classe_stipendiale  # noqa: E402

# ---------------------------------------------------------------------------
# Dati simulati (struttura DynamoDB item decreto_ricostruzione)
# ---------------------------------------------------------------------------

FAKE_DECRETO = {
    "PK": "PRATICA#FELICI_MARCO_20260421T143022Z",
    "SK": "DOCUMENTO#decreto_ricostruzione#aabbccdd-1234-5678-abcd-000000000001",
    "tipo_documento": "decreto_ricostruzione",

    # Sezione anagrafica
    "dati_anagrafici": {
        "nome_cognome": "FELICI MARCO",
        "codice_fiscale": "FLCMRC80A01H501U",
        "data_nascita": "01/01/1980",
    },

    # Sezione professionale
    "dati_professionali": {
        "qualifica_funzionale": "Docente scuola superiore diplomato",
        # data_decorrenza_giuridica = data_immissione_in_ruolo
        "data_immissione_in_ruolo": "01/09/2023",
        "data_decorrenza_giuridica": "01/09/2023",
        # data_decorrenza_economica = data_assunzione_in_servizio
        "data_decorrenza_economica": "01/09/2023",
        "data_assunzione_in_servizio": "01/09/2023",
        "data_conferma_in_ruolo": "01/09/2023",
    },

    # Intestazione decreto
    "intestazione": {
        "numero_decreto": "12345",
        "data_decreto": "21/04/2026",
        "istituto_amministrante": "I.C. G. Pascoli - Roma",
    },

    # Art. 2 — variazione stipendi
    "articolo_2": {
        # Classe stipendiale come TESTO (esempio alternativo: "terza posizione stipendiale")
        "classe_stipendiale": "terza posizione",
       # "data_scadenza": "01/09/2027",
        "periodo_totale_fini_giuridici_economici": {
            "anni": 5,
            "mesi": 2,
            "giorni": 15,
        },
        "periodo_totale_soli_fini_economici": {
            "anni": 3,
            "mesi": 0,
            "giorni": 0,
        },
    },

    # Art. 4 — assegni ad personam (può essere dict singolo o lista di assegni)
    "articolo_4": [
        {
            "codice_assegno": "120/SCU",
            "importo_assegno_ad_personam": "523,45",
            "numero_mensilita": "13"
        },
        {
            "codice_assegno": "121/IND",
            "importo_assegno_ad_personam": "350,00",
            "numero_mensilita": "12"
        },
    ],

    # Visti (dict singolo o lista)
    "visti": {
        "numero_visto": "2026/123",
        "data_visto": "30/04/2026",
    },

    # Assenze dal decreto (lista — può essere vuota)
    "assenze": [
        {
            "data_inizio": "01/09/2013",
            "data_fine": "31/08/2014",
            "tipologia_assenza": "aspettativa non retribuita",
        },
        {
            "data_inizio": "15/01/2020",
            "data_fine": "30/06/2020",
            "tipologia_assenza": "malattia",
        },
    ],
}

FAKE_METADATA = {
    "PK": "PRATICA#FELICI_MARCO_20260421T143022Z",
    "SK": "METADATA",
    "id_pratica": "FELICI_MARCO_20260421T143022Z",
    "tipo_servizio": "stipendi",
    "prescrizione": "Sì",
    "data_prescrizione": "01/09/2012",
}

# ---------------------------------------------------------------------------
# Generazione
# ---------------------------------------------------------------------------

TEMPLATE_PATH = pathlib.Path(__file__).parents[2] / "documenti" / "RTS_Prospetto esito Servizio Stipendi_24042026_v1.xlsx"
OUTPUT_PATH = pathlib.Path(__file__).parent / "prospetto_test.xlsx"


def test_normalizza_classe_stipendiale():
    """Test della funzione di normalizzazione classe stipendiale"""
    print("\n" + "="*70)
    print("TEST: Normalizzazione classe stipendiale")
    print("="*70)
    
    test_cases = [
        # (input, expected_output, description)
        ("prima posizione stipendiale", "00", "Prima posizione testuale"),
        ("Prima Posizione Stipendiale", "00", "Prima posizione maiuscole"),
        ("PRIMA POSIZIONE", "00", "Prima posizione abbreviato"),
        ("1a posizione stipendiale", "00", "Prima posizione numerata"),
        ("1° posizione stipendiale", "00", "Prima posizione con simbolo"),
        ("i posizione stipendiale", "00", "Prima posizione romana"),
        
        ("seconda posizione stipendiale", "09", "Seconda posizione testuale"),
        ("2a posizione stipendiale", "09", "Seconda posizione numerata"),
        ("ii posizione stipendiale", "09", "Seconda posizione romana"),
        
        ("terza posizione stipendiale", "15", "Terza posizione testuale"),
        ("3° posizione stipendiale", "15", "Terza posizione con simbolo"),
        
        ("quarta posizione stipendiale", "21", "Quarta posizione testuale"),
        ("4a posizione", "21", "Quarta posizione abbreviato"),
        
        ("quinta posizione stipendiale", "28", "Quinta posizione testuale"),
        ("5° posizione stipendiale", "28", "Quinta posizione con simbolo"),
        
        ("sesta posizione stipendiale", "35", "Sesta posizione testuale"),
        ("6a posizione", "35", "Sesta posizione abbreviato"),
        
        # Valori numerici già normalizzati
        ("0", "00", "Zero senza padding"),
        ("00", "00", "Zero con padding"),
        ("9", "09", "Nove senza padding"),
        ("09", "09", "Nove con padding"),
        ("15", "15", "Quindici"),
        ("21", "21", "Ventuno"),
        ("28", "28", "Ventotto"),
        ("35", "35", "Trentacinque"),
        
        # Edge cases
        ("", "00", "Stringa vuota"),
        (None, "00", "None"),
        ("posizione non valida", "00", "Testo non riconosciuto"),
    ]
    
    failed = 0
    passed = 0
    
    for input_val, expected, description in test_cases:
        result = _normalizza_classe_stipendiale(input_val)
        status = "✓ OK" if result == expected else "✗ FAIL"
        
        if result == expected:
            passed += 1
        else:
            failed += 1
            
        print(f"{status} | {description:40s} | Input: {str(input_val):35s} → Output: {result} (Expected: {expected})")
    
    print("-"*70)
    print(f"Risultati: {passed} passati, {failed} falliti su {len(test_cases)} test")
    print("="*70 + "\n")
    
    return failed == 0


def main():
    # Test della normalizzazione classe stipendiale
    test_ok = test_normalizza_classe_stipendiale()
    if not test_ok:
        print("[ATTENZIONE] Alcuni test di normalizzazione classe stipendiale sono falliti!\n")
    
    if not TEMPLATE_PATH.exists():
        print(f"[ERRORE] Template non trovato: {TEMPLATE_PATH}")
        sys.exit(1)

    print("Costruzione placeholder map...")
    mapping = _build_placeholder_map(FAKE_DECRETO, FAKE_METADATA)

    print("Campi estratti:")
    for k, v in mapping.items():
        if not k.startswith("_"):
            print(f"  {k!r:55s} : {v!r}")

    print(f"\nCaricamento template: {TEMPLATE_PATH.name}")
    template_bytes = TEMPLATE_PATH.read_bytes()

    print("Compilazione Excel...")
    result = fill_excel(template_bytes, mapping)

    OUTPUT_PATH.write_bytes(result)
    print(f"\nProspetto generato: {OUTPUT_PATH} ({len(result):,} bytes)")


if __name__ == "__main__":
    main()
