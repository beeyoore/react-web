import { useRef, useState } from 'react';
import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import Stepper from '../components/Stepper';
import NavigationToolbar from '../components/NavigationToolbar';

const STEPS = [
  { label: 'Dati della pratica',    sublabel: 'Completato' },
  { label: 'Caricamento documenti', sublabel: 'Da compilare' },
  { label: 'Riepilogo',             sublabel: 'Da verificare' },
];

const ACCEPTED_EXTS = ['.pdf', '.p7m'];

function isAccepted(file) {
  return ACCEPTED_EXTS.some(ext => file.name.toLowerCase().endsWith(ext));
}

function formatSize(bytes) {
  const mb = bytes / (1024 * 1024);
  return mb.toFixed(1).replace('.', ',') + ' MB';
}

function getExt(name) {
  const parts = name.split('.');
  return parts.length > 1 ? parts.pop().toUpperCase() : '';
}

function getBaseName(name) {
  const parts = name.split('.');
  if (parts.length > 1) parts.pop();
  return parts.join('.');
}

// ── Icons ──────────────────────────────────────────────────────────────────

function UploadIcon() {
  return (
    <svg width="80" height="80" viewBox="0 0 80 80" fill="none" aria-hidden="true">
      <circle cx="40" cy="40" r="39" stroke="var(--grey-border)" strokeWidth="2" />
      <path
        d="M40 52V30M40 30L32 38M40 30L48 38"
        stroke="var(--blue-main)" strokeWidth="2.5"
        strokeLinecap="round" strokeLinejoin="round"
      />
    </svg>
  );
}

function CloseCircleIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <circle cx="12" cy="12" r="11" stroke="var(--blue-main)" strokeWidth="1.5" />
      <path d="M15 9L9 15M9 9L15 15" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

// ── File cards ─────────────────────────────────────────────────────────────

function AcceptedFileCard({ file, onRemove }) {
  return (
    <div style={{ flex: '1 0 0', minWidth: 0, background: '#e5f2ff', borderRadius: 8 }}>
      <div style={{
        background: 'var(--grey-bg)', borderRadius: 8,
        padding: '16px 24px',
        display: 'flex', alignItems: 'center', gap: 24,
      }}>
        <div style={{ flex: '1 0 0', minWidth: 0 }}>
          <p style={{
            fontSize: 22, fontWeight: 600, letterSpacing: 1, lineHeight: '28px',
            color: 'var(--text-main)', marginBottom: 4,
            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>
            {file.name}
          </p>
          <p style={{
            fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px',
            color: 'var(--text-main)', whiteSpace: 'nowrap',
          }}>
            {formatSize(file.size)} - {getExt(file.name)}
          </p>
        </div>
        <button
          onClick={onRemove}
          aria-label={`Rimuovi ${file.name}`}
          style={{
            background: 'none', border: 'none', cursor: 'pointer',
            padding: 8, display: 'flex', alignItems: 'center', justifyContent: 'center',
            flexShrink: 0,
          }}
        >
          <CloseCircleIcon />
        </button>
      </div>
    </div>
  );
}

function RejectedFileCard({ file }) {
  const ext = getExt(file.name);
  const baseName = getBaseName(file.name);

  return (
    <div style={{ flex: '1 0 0', minWidth: 0, background: '#e5f2ff', borderRadius: 8 }}>
      <div style={{ background: '#fff6f6', borderRadius: 8, overflow: 'hidden' }}>
        {/* Red progress bar */}
        <div style={{ height: 8, background: '#eb0000', width: '100%' }} />

        <div style={{ padding: '16px 24px' }}>
          <p style={{
            fontSize: 22, fontWeight: 300, letterSpacing: 1, lineHeight: '28px',
            color: 'var(--text-main)', marginBottom: 4,
            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
          }}>
            <strong style={{ fontWeight: 600 }}>Rifiutato: </strong>
            {baseName}<span>.{ext}</span>
          </p>
          <p style={{
            fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px',
            color: 'var(--text-main)',
          }}>
            Il formato del documento non è supportato.
          </p>
        </div>
      </div>
    </div>
  );
}

// ── Delete confirmation modal ──────────────────────────────────────────────

function ConfirmDeleteModal({ onConfirm, onCancel }) {
  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
      style={{
        position: 'fixed', inset: 0, zIndex: 300,
        background: 'rgba(0,0,0,0.5)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}
      onClick={e => { if (e.target === e.currentTarget) onCancel(); }}
    >
      <div style={{
        background: 'var(--white)',
        borderRadius: 8,
        padding: '40px 48px',
        width: 480,
        display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16,
      }}>
        <h2 id="modal-title" style={{
          fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px',
          color: 'var(--text-main)', textAlign: 'center', margin: 0,
        }}>
          Vuoi eliminare il documento?
        </h2>
        <p style={{
          fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px',
          color: 'var(--text-main)', textAlign: 'center', margin: 0,
        }}>
          Se confermi, il documento non sarà più disponibile.
        </p>
        <div style={{ display: 'flex', gap: 16, marginTop: 8 }}>
          <button
            onClick={onCancel}
            style={{
              height: 48, minWidth: 160, padding: '12px 24px',
              border: '1px solid var(--blue-main)', borderRadius: 'var(--radius-btn)',
              background: 'var(--white)', cursor: 'pointer',
              fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
              letterSpacing: 1, color: 'var(--blue-main)',
            }}
          >
            Annulla
          </button>
          <button
            onClick={onConfirm}
            style={{
              height: 48, minWidth: 160, padding: '12px 24px',
              border: 'none', borderRadius: 'var(--radius-btn)',
              background: 'var(--blue-main)', cursor: 'pointer',
              fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
              letterSpacing: 1, color: 'var(--white)',
            }}
          >
            Conferma
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────

export default function CaricamentoDocumenti({ onNext, onCancel }) {
  const inputRef = useRef(null);
  const [isDragOver, setIsDragOver] = useState(false);
  // Each entry: { file, accepted: boolean }
  const [entries, setEntries] = useState([]);
  const [pendingDeleteIdx, setPendingDeleteIdx] = useState(null);

  function handleFiles(newFiles) {
    const incoming = Array.from(newFiles).map(file => ({ file, accepted: isAccepted(file) }));
    setEntries(prev => [...prev, ...incoming]);
  }

  function handleDrop(e) {
    e.preventDefault();
    setIsDragOver(false);
    handleFiles(e.dataTransfer.files);
  }

  function handleRemoveAccepted(idx) {
    setPendingDeleteIdx(idx);
  }

  function confirmDelete() {
    setEntries(prev => prev.filter((_, i) => i !== pendingDeleteIdx));
    setPendingDeleteIdx(null);
  }

  const hasAccepted = entries.some(e => e.accepted);
  const hasEntries  = entries.length > 0;

  // Group all entries into rows of 2 for the grid
  const rows = [];
  for (let i = 0; i < entries.length; i += 2) {
    rows.push(entries.slice(i, i + 2));
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName="MR" />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      <Stepper title="Apertura pratica" currentStep={2} totalSteps={3} steps={STEPS} />

      <main style={{ padding: '40px var(--margin-xl)', flex: 1 }}>
        {/* Title block */}
        <div style={{ paddingLeft: 'var(--margin-l)', marginBottom: 24 }}>
          <h2 style={{
            fontSize: 32, fontWeight: 600, letterSpacing: 1, lineHeight: '42px',
            color: 'var(--text-main)', marginBottom: 8,
          }}>
            Caricamento documenti
          </h2>
          <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)' }}>
            Carica tutti i documenti necessari alla lavorazione della pratica.
          </p>
        </div>

        {/* Drop zone */}
        <div
          role="button" tabIndex={0}
          aria-label="Area caricamento documenti"
          onClick={() => inputRef.current?.click()}
          onKeyDown={e => e.key === 'Enter' && inputRef.current?.click()}
          onDragOver={e => { e.preventDefault(); setIsDragOver(true); }}
          onDragLeave={() => setIsDragOver(false)}
          onDrop={handleDrop}
          style={{
            marginLeft: 'var(--margin-l)',
            border: `1px dashed ${isDragOver ? 'var(--blue-main)' : 'var(--grey-border)'}`,
            borderRadius: 'var(--radius-input)',
            background: isDragOver ? '#f0f7ff' : 'var(--white)',
            display: 'flex', flexDirection: 'column', alignItems: 'center',
            justifyContent: 'center', gap: 24, padding: '32px 24px',
            cursor: 'pointer', transition: 'border-color 0.15s, background 0.15s',
          }}
        >
          <UploadIcon />
          <div style={{ textAlign: 'center' }}>
            <p style={{
              fontSize: 22, fontWeight: 600, letterSpacing: 1, lineHeight: '28px',
              color: 'var(--text-main)', marginBottom: 4,
            }}>
              Trascina qui i tuoi documenti oppure{' '}
              <span style={{ color: 'var(--blue-main)', textDecoration: 'underline' }}>selezionali</span>
              {' '}dal tuo computer
            </p>
            <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)' }}>
              Formati accettati: PDF, P7M (max xMb)
            </p>
          </div>
        </div>

        <input
          ref={inputRef} type="file" accept="*" multiple
          style={{ display: 'none' }}
          onChange={e => { handleFiles(e.target.files); e.target.value = ''; }}
        />

        {/* Files grid */}
        {hasEntries && (
          <div style={{ marginLeft: 'var(--margin-l)', marginTop: 32 }}>
            <h3 style={{
              fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px',
              color: 'var(--text-main)', marginBottom: 16,
            }}>
              Documenti caricati
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              {rows.map((row, rowIdx) => (
                <div key={rowIdx} style={{ display: 'flex', gap: 16, alignItems: 'stretch' }}>
                  {row.map((entry, colIdx) => {
                    const globalIdx = rowIdx * 2 + colIdx;
                    return entry.accepted
                      ? (
                        <AcceptedFileCard
                          key={globalIdx}
                          file={entry.file}
                          onRemove={e => { e.stopPropagation(); handleRemoveAccepted(globalIdx); }}
                        />
                      ) : (
                        <RejectedFileCard key={globalIdx} file={entry.file} />
                      );
                  })}
                  {/* Spacer so single items stay half-width */}
                  {row.length === 1 && <div style={{ flex: '1 0 0' }} />}
                </div>
              ))}
            </div>
          </div>
        )}
      </main>

      {pendingDeleteIdx !== null && (
        <ConfirmDeleteModal
          onConfirm={confirmDelete}
          onCancel={() => setPendingDeleteIdx(null)}
        />
      )}

      <div style={{ height: 96 }} />

      <NavigationToolbar
        onCancel={onCancel}
        onNext={() => onNext(entries.filter(e => e.accepted).map(e => e.file))}
        nextDisabled={!hasAccepted}
      />
    </div>
  );
}
