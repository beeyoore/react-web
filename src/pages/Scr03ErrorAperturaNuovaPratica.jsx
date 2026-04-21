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

const CF_ERROR = 'Il codice fiscale non è valido. Verifica che\nquesto abbia 16 caratteri alfanumerici.';

export default function Scr03ErrorAperturaNuovaPratica() {
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

        <div style={{
          display: 'flex',
          alignItems: 'flex-start',
          gap: 24,
          paddingLeft: 'var(--margin-l)',
        }}>
          <FormField
            id="protocollo"
            label="Numero protocollo del decreto"
            placeholder="Es. 0012345/2026"
            defaultValue="0012345/2026"
            style={{ flex: '1 0 0', minWidth: 0 }}
          />
          <FormField
            id="data-pec"
            label="Data recezione PEC"
            placeholder="gg/mm/aaaa"
            defaultValue="01/10/2026"
            showCalendar
            style={{ width: 183, flexShrink: 0 }}
          />
          <FormField
            id="codice-fiscale"
            label="Codice fiscale"
            placeholder="Es. RSSMRA80A01H501U"
            defaultValue="RSSMRA80A01H501U"
            showInfo
            error={CF_ERROR}
            style={{ width: 269, flexShrink: 0 }}
          />
        </div>
      </main>

      <div style={{ height: 96 }} />

      <NavigationToolbar
        onCancel={() => alert('Annulla')}
        onNext={() => alert('Prosegui')}
        nextDisabled={false}
      />
    </div>
  );
}
