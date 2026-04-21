import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import Stepper from '../components/Stepper';
import FormField from '../components/FormField';
import NavigationToolbar from '../components/NavigationToolbar';

const STEPS = [
  { label: 'Dati della pratica',    sublabel: 'Da compilare' },
  { label: 'Caricamento documenti', sublabel: 'Non completato' },
  { label: 'Riepilogo',             sublabel: 'Non completato' },
];

export default function AperturaNuovaPratica() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      {/* Navigation header (sticky) */}
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName="MR" />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      {/* Stepper */}
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

        <div style={{
          display: 'flex',
          alignItems: 'flex-end',
          gap: 24,
          paddingLeft: 'var(--margin-l)',
        }}>
          <FormField
            id="protocollo"
            label="Numero protocollo del decreto"
            placeholder="Es. 0012345/2026"
            style={{ flex: '1 0 0', minWidth: 0 }}
          />
          <FormField
            id="data-pec"
            label="Data recezione PEC"
            placeholder="gg/mm/aaaa"
            showCalendar
            style={{ width: 183, flexShrink: 0 }}
          />
          <FormField
            id="codice-fiscale"
            label="Codice fiscale"
            placeholder="Es. RSSMRA80A01H501U"
            showInfo
            style={{ width: 269, flexShrink: 0 }}
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
