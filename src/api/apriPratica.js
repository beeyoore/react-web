const API_URL = import.meta.env.VITE_API_APRI_PRATICA_URL;

/**
 * Crea la pratica su DynamoDB e ottiene i presigned URL per l'upload S3.
 * @param {string} userId
 * @param {'controlli'|'stipendi'} tipoServizio
 * @param {string[]} fileNames
 * @returns {Promise<{ id_pratica: string, status: string, presigned_urls: Array }>}
 */
export async function apriPratica(userId, tipoServizio, fileNames) {
  const res = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_id: userId, tipo_servizio: tipoServizio, file_names: fileNames }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Errore server (${res.status})`);
  }

  return res.json();
}

/**
 * Carica i file direttamente su S3 tramite i presigned URL ricevuti dalla Lambda.
 * @param {File[]} files
 * @param {Array<{ nome: string, url: string }>} presignedUrls
 */
export async function uploadDocumenti(files, presignedUrls) {
  await Promise.all(
    files.map(async (file) => {
      const entry = presignedUrls.find((u) => u.nome === file.name);
      if (!entry) throw new Error(`Presigned URL mancante per ${file.name}`);
      const r = await fetch(entry.url, { method: 'PUT', body: file });
      if (!r.ok) throw new Error(`Upload fallito per ${file.name} (${r.status})`);
    })
  );
}
