import { useState } from 'react';
import PreHeader from '../../components/PreHeader';
import TopHeader from '../../components/TopHeader';
import SubheaderMenu from '../../components/SubheaderMenu';
import Stepper from '../../components/Stepper';
import FormField from '../../components/FormField';
import DatePickerField from '../../components/DatePickerField';
import RadioGroup from '../../components/RadioGroup';
import NavigationToolbar from '../../components/NavigationToolbar';

const STEPS = [
  { label: 'Dati della pratica',    sublabel: 'Da compilare' },
  { label: 'Caricamento documenti', sublabel: 'Non completato' },
  { label: 'Riepilogo',             sublabel: 'Non completato' },
];

function isValidCF(cf) {
  return /^[A-Za-z0-9]{16}$/.test(cf);
}

const CF_ERROR = 'Il codice fiscale non è valido. Verifica che\nquesto abbia 16 caratteri alfanumerici.';

export default function AperturaNuovaPraticaStipendi({ onNext, onCancel, userName }) {
  const [protocollo, setProtocollo]           = useState('');
  const [codiceFiscale, setCodiceFiscale]     = useState('');
  const [cfTouched, setCfTouched]             = useState(false);
  const [prescrizione, setPrescrizione]       = useState('No');
  const [dataPrescrizione, setDataPrescrizione] = useState(null);

  const cfValid = isValidCF(codiceFiscale);
  const cfError = cfTouched && codiceFiscale !== '' && !cfValid ? CF_ERROR : undefined;

  const prescrizioneOk = prescrizione === 'No' || (prescrizione === 'Sì' && dataPrescrizione !== null);
  const allFilled = protocollo.trim() !== '' && cfValid && prescrizioneOk;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName={userName} />
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

          {/* Riga 1: Protocollo + Codice fiscale */}
          <div style={{ display: 'flex', alignItems: 'flex-start', gap: 24 }}>
            <FormField
              id="protocollo"
              label="Numero protocollo del decreto"
              placeholder="Es. 0012345/2026"
              onChange={setProtocollo}
              style={{ flex: '1 0 0', minWidth: 0 }}
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

          {/* Riga 2: Prescrizione + (condizionale) Data inizio prescrizione */}
          <div style={{ display: 'flex', alignItems: 'flex-end', gap: 24 }}>
            <RadioGroup
              label="Prescrizione"
              options={['Sì', 'No']}
              defaultValue="No"
              onChange={setPrescrizione}
            />
            {prescrizione === 'Sì' && (
              <DatePickerField
                id="data-prescrizione"
                label="Data inizio prescrizione"
                value={dataPrescrizione}
                onChange={setDataPrescrizione}
                style={{ width: 220, flexShrink: 0 }}
              />
            )}
          </div>

        </div>
      </main>

      <div style={{ height: 96 }} />

      <NavigationToolbar
        onCancel={onCancel}
        onNext={() => onNext({ protocollo, codiceFiscale, prescrizione, dataPrescrizione })}
        nextDisabled={!allFilled}
      />
    </div>
  );
}

