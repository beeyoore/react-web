import { useState } from 'react';
import userProfile from './data/userProfile.json';
import Homepage from './pages/Homepage';

// Flusso "Operatore Controlli"
import AperturaNuovaPratica from './pages/AperturaNuovaPratica';
import CaricamentoDocumenti from './pages/CaricamentoDocumenti';
import Scr13AperturaNuovaPratica from './pages/Scr13AperturaNuovaPratica';

// Flusso "Stipendi"
import StipAperturaNuovaPratica from './pages/stipendi/AperturaNuovaPratica';
import StipCaricamentoDocumenti from './pages/stipendi/CaricamentoDocumenti';

const userName = `${userProfile.nome} ${userProfile.cognome}`;
const profilo = (userProfile.profilo || '').toLowerCase();

export default function App() {
  const [step, setStep] = useState(0);
  const [praticeData, setPraticeData] = useState(null);
  const [uploadedFiles, setUploadedFiles] = useState([]);

  // Homepage
  if (step === 0) return (
    <Homepage
      userName={userName}
      onNuovaPratica={() => setStep(1)}
    />
  );

  // ── Flusso Stipendi ──────────────────────────────────────────
  if (profilo === 'stipendi') {
    if (step === 1) return (
      <StipAperturaNuovaPratica
        onNext={(data) => { setPraticeData(data); setStep(2); }}
        onCancel={() => setStep(0)}
      />
    );
    return (
      <StipCaricamentoDocumenti
        praticeData={praticeData}
        onNext={(files) => { setUploadedFiles(files); setStep(3); }}
        onCancel={() => setStep(0)}
      />
    );
  }

  // ── Flusso Operatore Controlli (default) ─────────────────────
  if (step === 1) return (
    <AperturaNuovaPratica
      onNext={(data) => { setPraticeData(data); setStep(2); }}
      onCancel={() => setStep(0)}
    />
  );
  if (step === 2) return (
    <CaricamentoDocumenti
      onNext={(files) => { setUploadedFiles(files); setStep(3); }}
      onCancel={() => setStep(0)}
    />
  );
  return (
    <Scr13AperturaNuovaPratica
      praticeData={praticeData}
      uploadedFiles={uploadedFiles}
      onCancel={() => setStep(0)}
    />
  );
}
