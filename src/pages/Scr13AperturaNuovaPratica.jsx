import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import Stepper from '../components/Stepper';
import NavigationToolbar from '../components/NavigationToolbar';

const STEPS = [
  { label: 'Dati della pratica',    sublabel: 'Completato' },
  { label: 'Caricamento documenti', sublabel: 'Completato' },
  { label: 'Riepilogo',             sublabel: 'Da verificare' },
];

const MONTHS_IT = [
  'gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
  'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre',
];

function formatDateShort(d) {
  if (!d) return '';
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

function ReadOnlyField({ label, value, style }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, ...style }}>
      <p style={{
        fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px',
        color: 'var(--text-main)', margin: 0, whiteSpace: 'nowrap',
      }}>
        {label}
      </p>
      <p style={{
        fontSize: 18, fontWeight: 400, letterSpacing: 1, lineHeight: '28px',
        color: 'var(--text-main)', margin: 0,
      }}>
        {value}
      </p>
    </div>
  );
}

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
          <span style={{
            fontSize: 12, fontWeight: 300, letterSpacing: 1, lineHeight: '16px',
            color: '#494949', whiteSpace: 'nowrap',
          }}>
            {formatDateLong(today)}
          </span>
          <BulletDot />
          <span style={{
            fontSize: 12, fontWeight: 300, letterSpacing: 1, lineHeight: '16px',
            color: '#494949', whiteSpace: 'nowrap',
          }}>
            {getExt(file.name)} - {formatSize(file.size)}
          </span>
        </div>
      </div>
    </div>
  );
}

export default function Scr13AperturaNuovaPratica({ praticeData, uploadedFiles = [], onCancel }) {
  const { protocollo = '', dataPec = null, codiceFiscale = '' } = praticeData || {};

  const rows = [];
  for (let i = 0; i < uploadedFiles.length; i += 2) {
    rows.push(uploadedFiles.slice(i, i + 2));
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName="MR" />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      <Stepper title="Apertura pratica" currentStep={3} totalSteps={3} steps={STEPS} />

      <main style={{ padding: '40px var(--margin-xl)', flex: 1 }}>
        {/* Dati della pratica */}
        <div style={{ paddingLeft: 'var(--margin-l)', marginBottom: 32 }}>
          <h2 style={{
            fontSize: 32, fontWeight: 600, letterSpacing: 1, lineHeight: '42px',
            color: 'var(--text-main)', marginBottom: 24, margin: '0 0 24px',
          }}>
            Dati della pratica
          </h2>
          <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start' }}>
            <ReadOnlyField
              label="Numero di protocollo del decreto"
              value={protocollo}
              style={{ flex: '1 0 0', minWidth: 0 }}
            />
            <ReadOnlyField
              label="Data di recezione della PEC"
              value={formatDateShort(dataPec)}
              style={{ flexShrink: 0 }}
            />
            <ReadOnlyField
              label="Codice fiscale"
              value={codiceFiscale}
              style={{ flexShrink: 0 }}
            />
          </div>
        </div>

        {/* Documenti caricati */}
        {uploadedFiles.length > 0 && (
          <div style={{ paddingLeft: 'var(--margin-l)' }}>
            <h3 style={{
              fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px',
              color: 'var(--text-main)', margin: '0 0 24px',
            }}>
              Documenti caricati
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              {rows.map((row, rowIdx) => (
                <div key={rowIdx} style={{ display: 'flex', gap: 16 }}>
                  {row.map((file, colIdx) => (
                    <DocumentCard key={rowIdx * 2 + colIdx} file={file} />
                  ))}
                  {row.length === 1 && <div style={{ flex: '1 0 0' }} />}
                </div>
              ))}
            </div>
          </div>
        )}
      </main>

      <div style={{ height: 96 }} />

      <NavigationToolbar
        onCancel={onCancel}
        onNext={() => {}}
        nextDisabled={false}
        nextLabel="Crea pratica e avvia i controlli"
      />
    </div>
  );
}
