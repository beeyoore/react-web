import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import Stepper from '../components/Stepper';
import FormField from '../components/FormField';
import RadioGroup from '../components/RadioGroup';
import NavigationToolbar from '../components/NavigationToolbar';

const STEPS = [
  { label: 'Dati della pratica',    sublabel: 'Da compilare' },
  { label: 'Caricamento documenti', sublabel: 'Non completato' },
  { label: 'Riepilogo',             sublabel: 'Da verificare' },
];

export default function Scr13AperturaNuovaPratica() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      {/* Navigation header (sticky) */}
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName="MR" />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      {/* Stepper — Step 2 di 3, step 1 highlighted as active */}
      <Stepper
        title="Apertura pratica"
        currentStep={1}
        totalSteps={3}
        steps={STEPS}
      />

      {/* Main content */}
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

        {/* Row 1: Numero protocollo + Codice fiscale */}
        <div style={{
          display: 'flex',
          alignItems: 'flex-end',
          gap: 24,
          paddingLeft: 'var(--margin-l)',
          marginBottom: 24,
        }}>
          <FormField
            id="protocollo"
            label="Numero protocollo del decreto"
            placeholder="Es. 0012345/2026"
            style={{ flex: '1 0 0', minWidth: 0 }}
          />
          <FormField
            id="codice-fiscale"
            label="Codice fiscale"
            placeholder="Es. RSSMRA80A01H501U"
            showInfo
            style={{ width: 269, flexShrink: 0 }}
          />
        </div>

        {/* Row 2: Prescrizione radio */}
        <div style={{ paddingLeft: 'var(--margin-l)' }}>
          <RadioGroup
            label="Prescrizione"
            options={['Si', 'No']}
            defaultValue="No"
          />
        </div>
      </main>

      {/* Fixed bottom toolbar spacer */}
      <div style={{ height: 96 }} />

      <NavigationToolbar
        onCancel={() => alert('Annulla')}
        nextDisabled={true}
      />
    </div>
  );
}
