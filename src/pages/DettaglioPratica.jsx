import { useState } from 'react';
import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';

const MONTHS_IT = [
  'gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
  'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre',
];

function formatDateShort(d) {
  if (!d) return '—';
  return [
    String(d.getDate()).padStart(2, '0'),
    String(d.getMonth() + 1).padStart(2, '0'),
    d.getFullYear(),
  ].join('/');
}

function formatDateLong(d) {
  if (!d) return '';
  return `${d.getDate()} ${MONTHS_IT[d.getMonth()]} ${d.getFullYear()}`;
}

function getExt(name) {
  const parts = name.split('.');
  return parts.length > 1 ? parts.pop().toUpperCase() : '';
}

function formatSize(bytes) {
  const mb = bytes / (1024 * 1024);
  return mb.toFixed(1).replace('.', ',') + 'mb';
}

// ── Icons ──────────────────────────────────────────────────────────────────

function HomeIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <path
        d="M3 9.5L12 2.5L21 9.5V21H15V15H9V21H3V9.5Z"
        stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
      />
    </svg>
  );
}

function FileIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <path
        d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"
        stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
      />
      <polyline
        points="14 2 14 8 20 8"
        stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
      />
    </svg>
  );
}

function BulletDot() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <circle cx="8" cy="8" r="2" fill="#494949" />
    </svg>
  );
}

// Tab icons matching Figma
function IconDashboard() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <rect x="3" y="3" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <rect x="13" y="3" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <rect x="3" y="13" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <rect x="13" y="13" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
    </svg>
  );
}

function IconList() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <rect x="3" y="5" width="4" height="4" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="10" y1="7" x2="21" y2="7" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <rect x="3" y="10" width="4" height="4" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="10" y1="12" x2="21" y2="12" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <rect x="3" y="15" width="4" height="4" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="10" y1="17" x2="21" y2="17" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function IconSliders() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <line x1="4" y1="6" x2="20" y2="6" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="8" cy="6" r="2.5" fill="white" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="4" y1="12" x2="20" y2="12" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="16" cy="12" r="2.5" fill="white" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="4" y1="18" x2="20" y2="18" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="10" cy="18" r="2.5" fill="white" stroke="var(--blue-main)" strokeWidth="1.5" />
    </svg>
  );
}

// ── Status tag ─────────────────────────────────────────────────────────────

const STATUS_STYLES = {
  'IN LAVORAZIONE': { background: '#fff8eb', color: '#494949', border: '1px solid #c25700' },
  'NON AVVIABILI':  { background: '#fafafa', color: '#494949', border: '1px solid #737373' },
  'COMPLETATO':     { background: '#eaf4ea', color: '#2a6b2a', border: '1px solid #82c282' },
  'ERRORE':         { background: '#fdecea', color: '#b71c1c', border: '1px solid #f44336' },
};

function StatusTag({ label }) {
  const style = STATUS_STYLES[label] || STATUS_STYLES['NON AVVIABILI'];
  return (
    <span style={{
      display: 'inline-block',
      padding: '4px 8px',
      borderRadius: 8,
      fontSize: 12,
      fontWeight: 500,
      letterSpacing: 1,
      textTransform: 'uppercase',
      whiteSpace: 'nowrap',
      flexShrink: 0,
      ...style,
    }}>
      {label}
    </span>
  );
}

// ── Read-only field ────────────────────────────────────────────────────────

function ReadOnlyField({ label, value, style }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, ...style }}>
      <p style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', margin: 0, whiteSpace: 'nowrap' }}>
        {label}
      </p>
      <p style={{ fontSize: 18, fontWeight: 400, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
        {value || '—'}
      </p>
    </div>
  );
}

// ── Document card ──────────────────────────────────────────────────────────

function DocumentCard({ file }) {
  const today = new Date();
  return (
    <div style={{
      flex: '1 0 0', minWidth: 0,
      background: 'var(--white)',
      border: '1px solid var(--grey-border)',
      borderRadius: 8,
      padding: 16,
      display: 'flex', gap: 16, alignItems: 'flex-start',
    }}>
      <FileIcon />
      <div style={{ flex: '1 0 0', minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4 }}>
        <p style={{
          fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px',
          color: 'var(--blue-main)', textDecoration: 'underline', margin: 0,
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>
          {file.name}
        </p>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 12, fontWeight: 300, letterSpacing: 1, lineHeight: '16px', color: '#494949', whiteSpace: 'nowrap' }}>
            {formatDateLong(today)}
          </span>
          <BulletDot />
          <span style={{ fontSize: 12, fontWeight: 300, letterSpacing: 1, lineHeight: '16px', color: '#494949', whiteSpace: 'nowrap' }}>
            {getExt(file.name)} - {formatSize(file.size)}
          </span>
        </div>
      </div>
    </div>
  );
}

// ── Left tab menu ──────────────────────────────────────────────────────────

const TABS = [
  { label: 'Informazioni sulla pratica',        Icon: IconDashboard },
  { label: 'Controlli preliminari',              Icon: IconList },
  { label: 'Controlli amministrativo-contabili', Icon: IconSliders },
];

function TabMenu({ activeTab, onTabChange }) {
  return (
    <nav style={{
      width: 323,
      flexShrink: 0,
      display: 'flex',
      flexDirection: 'column',
      gap: 8,
      alignSelf: 'flex-start',
      position: 'sticky',
      top: 180,
    }}>
      {TABS.map(({ label, Icon }) => {
        const isActive = label === activeTab;
        return (
          <button
            key={label}
            onClick={() => onTabChange(label)}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              width: '100%',
              textAlign: 'left',
              padding: '12px 16px',
              border: 'none',
              borderRadius: 8,
              background: isActive ? '#f5f9ff' : 'transparent',
              cursor: 'pointer',
              fontFamily: 'var(--font)',
              fontSize: 18,
              fontWeight: isActive ? 600 : 300,
              letterSpacing: 1,
              lineHeight: '28px',
              color: 'var(--blue-main)',
            }}
          >
            <Icon />
            {label}
          </button>
        );
      })}
    </nav>
  );
}

// ── Info card ──────────────────────────────────────────────────────────────

function InfoPraticaCard({ praticeData, uploadedFiles }) {
  const {
    protocollo = '',
    dataPec = null,
    codiceFiscale = '',
  } = praticeData || {};

  const rows = [];
  for (let i = 0; i < uploadedFiles.length; i += 2) {
    rows.push(uploadedFiles.slice(i, i + 2));
  }

  return (
    <div style={{
      background: '#fbfcff',
      border: '1px solid var(--grey-border)',
      borderRadius: 8,
      padding: 16,
      display: 'flex',
      flexDirection: 'column',
      gap: 24,
    }}>
      <h4 style={{ fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px', color: 'var(--text-main)', margin: 0 }}>
        Informazioni sulla pratica
      </h4>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <p style={{ fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
          Dati della pratica
        </p>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
          <ReadOnlyField label="Numero di protocollo del decreto" value={protocollo} style={{ width: 264 }} />
          <ReadOnlyField label="Data di recezione della PEC" value={formatDateShort(dataPec)} style={{ width: 264 }} />
          <ReadOnlyField label="Codice fiscale" value={codiceFiscale} style={{ width: 264 }} />
        </div>
      </div>

      {uploadedFiles.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <p style={{ fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
            Documenti caricati
          </p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {rows.map((row, rowIdx) => (
              <div key={rowIdx} style={{ display: 'flex', gap: 24 }}>
                {row.map((file, colIdx) => (
                  <DocumentCard key={rowIdx * 2 + colIdx} file={file} />
                ))}
                {row.length === 1 && <div style={{ flex: '1 0 0' }} />}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ControlliCard({ title, status, description }) {
  return (
    <div style={{
      background: '#fbfcff',
      border: '1px solid var(--grey-border)',
      borderRadius: 8,
      padding: 16,
      display: 'flex',
      flexDirection: 'column',
      gap: 8,
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
        <p style={{ fontSize: 20, fontWeight: 600, letterSpacing: 1, lineHeight: '26px', color: 'var(--text-main)', margin: 0, whiteSpace: 'nowrap' }}>
          {title}
        </p>
        <StatusTag label={status} />
      </div>
      <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
        {description}
      </p>
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────

export default function DettaglioPratica({ praticeData, uploadedFiles = [], onHome, userName }) {
  const [activeTab, setActiveTab] = useState(TABS[0].label);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName={userName} />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      {/* Breadcrumb */}
      <div style={{
        height: 60,
        borderBottom: '1px solid var(--grey-border)',
        display: 'flex',
        alignItems: 'center',
        padding: '0 40px',
        gap: 8,
      }}>
        <button
          onClick={onHome}
          style={{
            display: 'flex', alignItems: 'center', gap: 4,
            background: 'none', border: 'none', cursor: 'pointer',
            fontFamily: 'var(--font)', padding: 0,
          }}
        >
          <HomeIcon />
        </button>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--blue-main)' }}>/</span>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--blue-main)' }}>
          Dettaglio pratica
        </span>
      </div>

      <main style={{ padding: '32px 40px 80px', flex: 1, display: 'flex', flexDirection: 'column', gap: 32 }}>
        {/* Page title */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <h2 style={{ fontSize: 32, fontWeight: 600, letterSpacing: 1, lineHeight: '42px', color: 'var(--text-main)', margin: 0 }}>
            Dettaglio pratica
          </h2>
          <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
            Consulta le informazioni relative alla pratica e lo stato di avanzamento dei controlli preliminari e amministrativo-contabili. Se i controlli sono ancora in lavorazione, riprova tra qualche minuto per visualizzarne gli esiti.
          </p>
        </div>

        {/* Two-column layout */}
        <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start' }}>
          <TabMenu activeTab={activeTab} onTabChange={setActiveTab} />

          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 40 }}>
            {activeTab === 'Informazioni sulla pratica' && (
              <InfoPraticaCard praticeData={praticeData} uploadedFiles={uploadedFiles} />
            )}

            {activeTab === 'Controlli preliminari' && (
              <ControlliCard
                title="Controlli preliminari"
                status="IN LAVORAZIONE"
                description="Controlli preliminari in corso. L'esito sarà disponibile al termine della verifica."
              />
            )}

            {activeTab === 'Controlli amministrativo-contabili' && (
              <ControlliCard
                title="Controlli amministrativo-contabili"
                status="NON AVVIABILI"
                description="Controlli automatici non avviati. È necessario attendere il completamento dei controlli preliminari."
              />
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
