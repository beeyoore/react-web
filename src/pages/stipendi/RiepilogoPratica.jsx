import { useState } from 'react';
import PreHeader from '../../components/PreHeader';
import TopHeader from '../../components/TopHeader';
import SubheaderMenu from '../../components/SubheaderMenu';
import Stepper from '../../components/Stepper';
import NavigationToolbar from '../../components/NavigationToolbar';
import userProfile from '../../data/userProfile.json';
import { apriPratica } from '../../api/apriPratica';

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

function CheckCircleIcon() {
  return (
    <svg width="64" height="64" viewBox="0 0 64 64" fill="none" aria-hidden="true">
      <circle cx="32" cy="32" r="32" fill="#468041" />
      <path d="M20 32L28 40L44 24" stroke="white" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

// ── Success modal ──────────────────────────────────────────────────────────

function SuccessModal({ onHome, onDetail }) {
  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="success-modal-title"
      style={{
        position: 'fixed', inset: 0, zIndex: 300,
        background: 'rgba(0,0,0,0.5)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}
    >
      <div style={{
        background: 'var(--white)',
        borderRadius: 12,
        padding: '40px 48px',
        width: 727,
        display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16,
        boxShadow: '0 8px 32px rgba(0,0,0,0.2)',
      }}>
        <CheckCircleIcon />

        <h2 id="success-modal-title" style={{
          fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px',
          color: 'var(--text-main)', textAlign: 'center', margin: 0,
        }}>
          Pratica aperta con successo!
        </h2>

        <p style={{
          fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px',
          color: 'var(--text-main)', textAlign: 'center', margin: 0,
        }}>
          La pratica è stata aperta correttamente.<br />
          Gli esiti dei controlli saranno disponibili<br />
          in Homepage appena completati.
        </p>

        <div style={{ display: 'flex', gap: 16, marginTop: 8, width: '100%' }}>
          <button
            onClick={onHome}
            style={{
              flex: 1, height: 48, padding: '12px 16px',
              border: '1px solid var(--blue-main)',
              borderRadius: 'var(--radius-btn)',
              background: 'var(--white)', cursor: 'pointer',
              fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
              letterSpacing: 1, color: 'var(--blue-main)',
            }}
          >
            Torna alla Homepage
          </button>
          <button
            onClick={onDetail}
            style={{
              flex: 1, height: 48, padding: '12px 16px',
              border: 'none',
              borderRadius: 'var(--radius-btn)',
              background: 'var(--blue-main)', cursor: 'pointer',
              fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
              letterSpacing: 1, color: 'var(--white)',
            }}
          >
            Vai al dettaglio pratica
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Read-only field ────────────────────────────────────────────────────────

function ReadOnlyField({ label, value, style }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, ...style }}>
      <p style={{
        fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px',
        color: 'var(--text-main)', margin: 0,
      }}>
        {label}
      </p>
      <p style={{
        fontSize: 18, fontWeight: 400, letterSpacing: 1, lineHeight: '28px',
        color: 'var(--text-main)', margin: 0,
      }}>
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

// ── Page ───────────────────────────────────────────────────────────────────

export default function RiepilogoPraticaStipendi({ praticeData, uploadedFiles = [], onCancel, userName }) {
  const [showModal, setShowModal] = useState(false);
  const [loading, setLoading] = useState(false);
  const [apiError, setApiError] = useState(null);

  async function handleApriPratica() {
    setApiError(null);
    setLoading(true);
    try {
      await apriPratica(userProfile.id, uploadedFiles);
      setShowModal(true);
    } catch (err) {
      setApiError(err.message || 'Errore durante l\'apertura della pratica');
    } finally {
      setLoading(false);
    }
  }

  const {
    protocollo = '',
    codiceFiscale = '',
    prescrizione = 'No',
    dataPrescrizione = null,
  } = praticeData || {};

  const rows = [];
  for (let i = 0; i < uploadedFiles.length; i += 2) {
    rows.push(uploadedFiles.slice(i, i + 2));
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName={userName} />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      <Stepper title="Apertura pratica" currentStep={3} totalSteps={3} steps={STEPS} />

      <main style={{ padding: '40px var(--margin-xl)', flex: 1 }}>

        {/* Dati della pratica */}
        <div style={{ paddingLeft: 'var(--margin-l)', marginBottom: 40 }}>
          <h2 style={{
            fontSize: 32, fontWeight: 600, letterSpacing: 1, lineHeight: '42px',
            color: 'var(--text-main)', margin: '0 0 24px',
          }}>
            Dati della pratica
          </h2>

          {/* Riga 1: Protocollo + Codice fiscale */}
          <div style={{ display: 'flex', gap: 48, alignItems: 'flex-start', marginBottom: 24 }}>
            <ReadOnlyField
              label="Numero di protocollo del decreto"
              value={protocollo}
              style={{ flex: '1 0 0', minWidth: 0 }}
            />
            <ReadOnlyField
              label="Codice fiscale"
              value={codiceFiscale}
              style={{ flex: '1 0 0', minWidth: 0 }}
            />
          </div>

          {/* Riga 2: Prescrizione + (condizionale) Data prescrizione */}
          <div style={{ display: 'flex', gap: 48, alignItems: 'flex-start' }}>
            <ReadOnlyField
              label="Prescrizione"
              value={prescrizione}
              style={{ flex: '1 0 0', minWidth: 0 }}
            />
            {prescrizione === 'Sì' && (
              <ReadOnlyField
                label="Data prescrizione"
                value={formatDateShort(dataPrescrizione)}
                style={{ flex: '1 0 0', minWidth: 0 }}
              />
            )}
            {prescrizione !== 'Sì' && <div style={{ flex: '1 0 0' }} />}
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

      {apiError && (
        <div role="alert" style={{
          margin: '0 var(--margin-xl) 16px',
          padding: '12px 16px',
          background: '#fdecea',
          border: '1px solid #f44336',
          borderRadius: 4,
          color: '#b71c1c',
          fontSize: 14,
        }}>
          {apiError}
        </div>
      )}

      <NavigationToolbar
        onCancel={onCancel}
        onNext={handleApriPratica}
        nextDisabled={loading}
        nextLabel={loading ? 'Apertura in corso...' : 'Apri pratica e avvia i controlli'}
      />

      {showModal && (
        <SuccessModal
          onHome={onCancel}
          onDetail={onCancel}
        />
      )}
    </div>
  );
}
