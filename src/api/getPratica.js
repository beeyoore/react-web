const API_URL = import.meta.env.VITE_API_GET_PRATICA_URL;

export async function getPratica(idPratica) {
  const res = await fetch(`${API_URL}/${idPratica}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Errore server (${res.status})`);
  }
  return res.json();
}
