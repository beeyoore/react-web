import { useState } from 'react';
import AperturaNuovaPratica from './pages/AperturaNuovaPratica';
import CaricamentoDocumenti from './pages/CaricamentoDocumenti';
import Scr13AperturaNuovaPratica from './pages/Scr13AperturaNuovaPratica';

export default function App() {
  const [step, setStep] = useState(1);

  if (step === 1) return <AperturaNuovaPratica onNext={() => setStep(2)} onCancel={() => setStep(1)} />;
  if (step === 2) return <CaricamentoDocumenti onNext={() => setStep(3)} onCancel={() => setStep(1)} />;
  return <Scr13AperturaNuovaPratica onCancel={() => setStep(1)} />;
}
