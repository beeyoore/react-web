import { useState } from 'react';
import AperturaNuovaPratica from './pages/AperturaNuovaPratica';
import CaricamentoDocumenti from './pages/CaricamentoDocumenti';
import Scr13AperturaNuovaPratica from './pages/Scr13AperturaNuovaPratica';

export default function App() {
  const [step, setStep] = useState(1);
  const [praticeData, setPraticeData] = useState(null);
  const [uploadedFiles, setUploadedFiles] = useState([]);

  if (step === 1) return (
    <AperturaNuovaPratica
      onNext={(data) => { setPraticeData(data); setStep(2); }}
      onCancel={() => setStep(1)}
    />
  );
  if (step === 2) return (
    <CaricamentoDocumenti
      onNext={(files) => { setUploadedFiles(files); setStep(3); }}
      onCancel={() => setStep(1)}
    />
  );
  return (
    <Scr13AperturaNuovaPratica
      praticeData={praticeData}
      uploadedFiles={uploadedFiles}
      onCancel={() => setStep(1)}
    />
  );
}
