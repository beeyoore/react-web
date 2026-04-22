const API_URL = import.meta.env.VITE_API_CONVALIDA_URL;

/**
 * Imposta convalidato=true per i controlli specificati.
 * @param {string} idPratica
 * @param {'preliminare' | 'amm_contabile'} tipo
 * @param {string[]} controlloIds  – lista di id dei controlli da convalidare
 * @returns {Promise<{ updated: string[] }>}
 */
export async function convalidaControlli(idPratica, tipo, controlloIds) {
  const res = await fetch(`${API_URL}/${idPratica}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tipo, controllo_ids: controlloIds }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Errore server (${res.status})`);
  }

  return res.json();
}
