const API_URL = import.meta.env.VITE_API_APRI_PRATICA_URL;

function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      // result è "data:<mime>;base64,<contenuto>" — estraiamo solo la parte base64
      const base64 = reader.result.split(',')[1];
      resolve(base64);
    };
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

/**
 * Apre una pratica su DynamoDB e carica i documenti su S3.
 * @param {string} userId  - ID utente da userProfile.id
 * @param {File[]} files   - array di oggetti File nativi del browser
 * @returns {Promise<{ id_pratica: string, status: string }>}
 */
export async function apriPratica(userId, files) {
  const documenti = await Promise.all(
    files.map(async (file) => ({
      nome: file.name,
      contenuto: await fileToBase64(file),
    }))
  );

  const res = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_id: userId, documenti }),
  });

  if (!res.ok && res.status !== 207) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Errore server (${res.status})`);
  }

  return res.json();
}
