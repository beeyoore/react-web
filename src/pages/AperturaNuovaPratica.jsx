import { useState } from 'react';
import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import Stepper from '../components/Stepper';
import FormField from '../components/FormField';
import DatePickerField from '../components/DatePickerField';
import PageAlert from '../components/PageAlert';
import NavigationToolbar from '../components/NavigationToolbar';

const STEPS = [
  { label: 'Dati della pratica',    sublabel: 'Da compilare' },
  { label: 'Caricamento documenti', sublabel: 'Non completato' },
  { label: 'Riepilogo',             sublabel: 'Non completato' },
];

function daysSince(date) {
  if (!date) return null;
  return Math.floor((Date.now() - date.getTime()) / 86_400_000);
}

function isValidCF(cf) {
  return /^[A-Za-z0-9]{16}$/.test(cf);
}

const CF_ERROR = 'Il codice fiscale non è valido. Verifica che\nquesto abbia 16 caratteri alfanumerici.';

export default function AperturaNuovaPratica({ onNext, onCancel }) {
  const [protocollo, setProtocollo] = useState('');
  const [dataPec, setDataPec] = useState(null);
  const [codiceFiscale, setCodiceFiscale] = useState('');
  const [cfTouched, setCfTouched] = useState(false);

  const cfValid = isValidCF(codiceFiscale);
  const cfError = cfTouched && codiceFiscale !== '' && !cfValid ? CF_ERROR : undefined;

  const allFilled = protocollo.trim() !== '' && dataPec !== null && cfValid;
  const days = daysSince(dataPec);
  const showAlert = days !== null && days >= 30;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName="MR" />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      <Stepper
        title="Apertura pratica"
        currentStep={1}
        totalSteps={3}
        steps={STEPS}
      />

      <main style={{ padding: '40px var(--margin-xl)', flex: 1 }}>
        <h2 style={{
          fontSize: 32,
          fontWeight: 600,
          letterSpacing: 1,
          lineHeight: '42px',
          color: 'var(--text-main)',
          marginBottom: 24,
          paddingLeft: 'var(--margin-l)',
        }}>
          Dati della pratica
        </h2>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 24, paddingLeft: 'var(--margin-l)' }}>
          <div style={{ display: 'flex', alignItems: 'flex-start', gap: 24 }}>
            <FormField
              id="protocollo"
              label="Numero protocollo del decreto"
              placeholder="Es. 0012345/2026"
              onChange={setProtocollo}
              style={{ flex: '1 0 0', minWidth: 0 }}
            />
            <DatePickerField
              id="data-pec"
              label="Data recezione PEC"
              value={dataPec}
              onChange={setDataPec}
              style={{ width: 183, flexShrink: 0 }}
            />
            <FormField
              id="codice-fiscale"
              label="Codice fiscale"
              placeholder="Es. RSSMRA80A01H501U"
              showInfo
              onChange={setCodiceFiscale}
              onBlur={() => setCfTouched(true)}
              error={cfError}
              style={{ width: 269, flexShrink: 0 }}
            />
          </div>

          {showAlert && (
            <PageAlert
              title="Attenzione:"
              message="sono trascorsi più di 30 giorni dalla ricezione della PEC. Prima di procedere, verifica i termini previsti dalla normativa."
            />
          )}
        </div>
      </main>

      <div style={{ height: 96 }} />

      <NavigationToolbar
        onCancel={onCancel}
        onNext={() => onNext({ protocollo, dataPec, codiceFiscale })}
        nextDisabled={!allFilled}
      />
    </div>
  );
}
