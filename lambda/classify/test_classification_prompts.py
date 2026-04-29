"""
Test per verificare che i prompt di classificazione siano separati correttamente
tra i flussi "controlli" e "stipendi".
"""

# Mock delle costanti dal handler
COMMON_DOCUMENT_TYPES = [
    "Decreto di ricostruzione della carriera",
    "Istanza ricostruzione di carriera",
    "Certificato di servizi/Dichiarazione di servizi/Autocertificazione dei servizi",
    "Contratto a tempo indeterminato",
    "Decreto superamento anno di formazione e prova",
    "Presa d'atto di conferma in ruolo del personale",
    "Titolo di studio",
    "Foglio di congedo illimitato",
    "Decreto di mancato superamento prova",
    "Osservazione precedente",
]

CONTROLLI_DOCUMENT_TYPES = COMMON_DOCUMENT_TYPES + ["NON_CLASSIFICATO"]

STIPENDI_DOCUMENT_TYPES = [
    "Decreto di ricostruzione della carriera",
    "Visto di controllo",
    "NON_CLASSIFICATO",
]

VALID_DOCUMENT_TYPES = CONTROLLI_DOCUMENT_TYPES


def get_valid_document_types_for_flusso(tipo_flusso: str | None) -> list[str]:
    """
    Restituisce la lista di tipi di documento validi in base al tipo_flusso.
    """
    if tipo_flusso == "controlli":
        return CONTROLLI_DOCUMENT_TYPES
    elif tipo_flusso == "stipendi":
        return STIPENDI_DOCUMENT_TYPES
    else:
        return VALID_DOCUMENT_TYPES


def build_system_prompt(valid_document_types: list[str]) -> str:
    """
    Costruisce il system prompt con la lista specifica di tipi di documento validi.
    """
    document_types_list = "\n".join(f"- {doc_type}" for doc_type in valid_document_types)
    
    return f"""
Sei un classificatore documentale prudente per documenti amministrativi scolastici italiani.

Devi classificare il documento in UNA sola delle seguenti classi:
{document_types_list}

Regole:
- Sii prudente.
- Se gli indizi non sono sufficienti, troppo deboli o ambigui, restituisci NON_CLASSIFICATO.
""".strip()


def test_prompts_separation():
    """Test che i prompt siano diversi tra controlli e stipendi."""
    print("=" * 80)
    print("TEST: Verifica separazione prompt per flussi controlli e stipendi")
    print("=" * 80)
    
    # Test flusso controlli
    print("\n--- FLUSSO: controlli ---")
    controlli_types = get_valid_document_types_for_flusso("controlli")
    print(f"Numero tipi documento: {len(controlli_types)}")
    print(f"Tipi documento: {controlli_types}")
    print(f"'Visto di controllo' presente: {'Visto di controllo' in controlli_types}")
    
    prompt_controlli = build_system_prompt(controlli_types)
    print(f"\nPrompt preview (primi 500 caratteri):\n{prompt_controlli[:500]}")
    
    # Test flusso stipendi
    print("\n--- FLUSSO: stipendi ---")
    stipendi_types = get_valid_document_types_for_flusso("stipendi")
    print(f"Numero tipi documento: {len(stipendi_types)}")
    print(f"Tipi documento: {stipendi_types}")
    print(f"'Visto di controllo' presente: {'Visto di controllo' in stipendi_types}")
    
    prompt_stipendi = build_system_prompt(stipendi_types)
    print(f"\nPrompt preview (primi 500 caratteri):\n{prompt_stipendi[:500]}")
    
    # Test flusso sconosciuto (default)
    print("\n--- FLUSSO: None (default) ---")
    default_types = get_valid_document_types_for_flusso(None)
    print(f"Numero tipi documento: {len(default_types)}")
    print(f"'Visto di controllo' presente: {'Visto di controllo' in default_types}")
    
    # Verifica che i prompt siano effettivamente diversi
    print("\n" + "=" * 80)
    print("VERIFICA: I prompt sono diversi?")
    print("=" * 80)
    print(f"Prompt controlli != Prompt stipendi: {prompt_controlli != prompt_stipendi}")
    print(f"Lunghezza prompt controlli: {len(prompt_controlli)} caratteri")
    print(f"Lunghezza prompt stipendi: {len(prompt_stipendi)} caratteri")
    
    # Verifica contenuto specifico
    print("\n" + "=" * 80)
    print("VERIFICA: 'Visto di controllo' nei prompt")
    print("=" * 80)
    visto_in_controlli = "Visto di controllo" in prompt_controlli
    visto_in_stipendi = "Visto di controllo" in prompt_stipendi
    print(f"'Visto di controllo' nel prompt controlli: {visto_in_controlli}")
    print(f"'Visto di controllo' nel prompt stipendi: {visto_in_stipendi}")
    
    # Test riuscito?
    success = (
        not visto_in_controlli and 
        visto_in_stipendi and 
        prompt_controlli != prompt_stipendi and
        len(controlli_types) == 11 and  # 10 comuni + NON_CLASSIFICATO
        len(stipendi_types) == 3  # Decreto + Visto + NON_CLASSIFICATO
    )
    
    print("\n" + "=" * 80)
    if success:
        print("✅ TEST SUPERATO: I prompt sono correttamente separati!")
    else:
        print("❌ TEST FALLITO: Verifica i prompt!")
    print("=" * 80)
    
    return success


if __name__ == "__main__":
    test_prompts_separation()
