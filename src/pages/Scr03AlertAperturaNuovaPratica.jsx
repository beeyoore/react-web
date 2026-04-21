import { useState } from 'react';
import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import Stepper from '../components/Stepper';
import FormField from '../components/FormField';
import PageAlert from '../components/PageAlert';
import NavigationToolbar from '../components/NavigationToolbar';

const STEPS = [
  { label: 'Dati della pratica',    sublabel: 'Da compilare' },
  { label: 'Caricamento documenti', sublabel: 'Non completato' },
  { label: 'Riepilogo',             sublabel: 'Non completato' },
];

/** Parses dd/mm/yyyy and returns days elapsed since that date, or null if invalid. */
function daysSince(dateStr) {
  const [dd, mm, yyyy] = dateStr.split('/').map(Number);
  if (!dd || !mm || !yyyy) return null;
  const parsed = new Date(yyyy, mm - 1, dd);
  if (isNaN(parsed)) return null;
  return Math.floor((Date.now() - parsed.getTime()) / 86_400_000);
}

export default function Scr03AlertAperturaNuovaPratica() {
  const [pecDate, setPecDate] = useState('03/07/2026');

  const days = daysSince(pecDate);
  const showAlert = days !== null && days > 60;

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
          <div style={{ display: 'flex', alignItems: 'flex-end', gap: 24 }}>
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
              defaultValue={pecDate}
              showCalendar
              onChange={setPecDate}
              style={{ width: 183, flexShrink: 0 }}
            />
            <FormField
              id="codice-fiscale"
              label="Codice fiscale"
              placeholder="Es. RSSMRA80A01H501U"
              defaultValue="RSSMRA80A01H501U"
              showInfo
              style={{ width: 269, flexShrink: 0 }}
            />
          </div>

          {showAlert && (
            <PageAlert
              title="Attenzione:"
              message="sono trascorsi più di 60 giorni dalla ricezione della PEC. Prima di procedere, verifica i termini previsti dalla normativa."
            />
          )}
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
