import { useState } from 'react';
import userProfile from './data/userProfile.json';
import { getDaysSinceDate, getHomepageVariant } from './utils/dateUtils';
import Homepage from './pages/Homepage';

// Flusso "Operatore Controlli"
import AperturaNuovaPratica from './pages/AperturaNuovaPratica';
import CaricamentoDocumenti from './pages/CaricamentoDocumenti';
import Scr13AperturaNuovaPratica from './pages/Scr13AperturaNuovaPratica';
import DettaglioPratica from './pages/DettaglioPratica';

// Flusso "Stipendi"
import HomepageStipendi from './pages/stipendi/Homepage';
import StipAperturaNuovaPratica from './pages/stipendi/AperturaNuovaPratica';
import StipCaricamentoDocumenti from './pages/stipendi/CaricamentoDocumenti';
import StipRiepilogoPratica from './pages/stipendi/RiepilogoPratica';

const userName = `${userProfile.nome.charAt(0)}${userProfile.cognome.charAt(0)}`;
const displayName = userProfile.nome || userName;
const profilo = (userProfile.profilo || '').toLowerCase();
const genderValue = String(userProfile.genere || userProfile.sesso || userProfile.gender || '').toLowerCase();
const salutation = ['f', 'femmina', 'female'].includes(genderValue) ? 'Benvenuta' : 'Benvenuto';

// Determine homepage variant based on days since first login
const daysSinceFirstLogin = getDaysSinceDate(userProfile.first_login);
const homepageVariant = getHomepageVariant(daysSinceFirstLogin);

export default function App() {
  const [step, setStep] = useState(0);
  const [praticeData, setPraticeData] = useState(null);
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [idPratica, setIdPratica] = useState(null);

  // Homepage - Route to correct variant based on profilo
  if (step === 0) {
    if (profilo === 'stipendi') {
      return (
        <HomepageStipendi
          variant={homepageVariant}
          userName={userName}
          displayName={displayName}
          salutation={salutation}
          onNuovaPratica={() => setStep(1)}
        />
      );
    }
    return (
      <Homepage
        variant={homepageVariant}
        userName={userName}
        displayName={displayName}
        salutation={salutation}
        onNuovaPratica={() => setStep(1)}
      />
    );
  }

  // ── Flusso Stipendi ──────────────────────────────────────────
  if (profilo === 'stipendi') {
    if (step === 1) return (
      <StipAperturaNuovaPratica
        onNext={(data) => { setPraticeData(data); setStep(2); }}
        onCancel={() => setStep(0)}
      />
    );
    if (step === 2) return (
      <StipCaricamentoDocumenti
        praticeData={praticeData}
        onNext={(files) => { setUploadedFiles(files); setStep(3); }}
        onCancel={() => setStep(0)}
      />
    );
    return (
      <StipRiepilogoPratica
        praticeData={praticeData}
        uploadedFiles={uploadedFiles}
        userName={userName}
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
  if (step === 3) return (
    <Scr13AperturaNuovaPratica
      praticeData={praticeData}
      uploadedFiles={uploadedFiles}
      onCancel={() => setStep(0)}
      onDetail={(id) => { setIdPratica(id); setStep(4); }}
    />
  );
  return (
    <DettaglioPratica
      praticeData={praticeData}
      uploadedFiles={uploadedFiles}
      idPratica={idPratica}
      userName={userName}
      onHome={() => setStep(0)}
    />
  );
}
