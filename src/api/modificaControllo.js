const API_URL = import.meta.env.VITE_API_AGGIORNA_CONTROLLO_URL;

/**
 * Aggiorna esito e motivazione di un controllo.
 * @param {string} idPratica
 * @param {string} controlloId
 * @param {'superato' | 'non_superato' | 'non_verificabile'} esito
 * @param {string} motivazione
 */
export async function modificaControllo(idPratica, controlloId, esito, motivazione) {
  const res = await fetch(`${API_URL}/${idPratica}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'modifica', controllo_id: controlloId, esito, motivazione }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Errore server (${res.status})`);
  }

  return res.json();
}
