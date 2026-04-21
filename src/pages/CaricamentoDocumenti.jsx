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

function UploadIcon() {
  return (
    <svg width="80" height="80" viewBox="0 0 80 80" fill="none" aria-hidden="true">
      <circle cx="40" cy="40" r="39" stroke="var(--grey-border)" strokeWidth="2" />
      <path
        d="M40 52V30M40 30L32 38M40 30L48 38"
        stroke="var(--blue-main)"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export default function CaricamentoDocumenti({ onNext, onCancel }) {
  const inputRef = useRef(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const [files, setFiles] = useState([]);

  function handleFiles(newFiles) {
    const accepted = Array.from(newFiles).filter(f =>
      f.name.toLowerCase().endsWith('.pdf') || f.name.toLowerCase().endsWith('.p7m')
    );
    setFiles(prev => [...prev, ...accepted]);
  }

  function handleDrop(e) {
    e.preventDefault();
    setIsDragOver(false);
    handleFiles(e.dataTransfer.files);
  }

  function handleRemove(index) {
    setFiles(prev => prev.filter((_, i) => i !== index));
  }

  const hasFiles = files.length > 0;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName="MR" />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      <Stepper
        title="Apertura pratica"
        currentStep={2}
        totalSteps={3}
        steps={STEPS}
      />

      <main style={{ padding: '40px var(--margin-xl)', flex: 1 }}>
        {/* Title block */}
        <div style={{ paddingLeft: 'var(--margin-l)', marginBottom: 24 }}>
          <h2 style={{
            fontSize: 32,
            fontWeight: 600,
            letterSpacing: 1,
            lineHeight: '42px',
            color: 'var(--text-main)',
            marginBottom: 8,
          }}>
            Caricamento documenti
          </h2>
          <p style={{
            fontSize: 18,
            fontWeight: 300,
            letterSpacing: 1,
            lineHeight: '28px',
            color: 'var(--text-main)',
          }}>
            Carica tutti i documenti necessari alla lavorazione della pratica.
          </p>
        </div>

        {/* Upload drop zone */}
        <div
          role="button"
          tabIndex={0}
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
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 24,
            padding: '32px 24px',
            cursor: 'pointer',
            transition: 'border-color 0.15s, background 0.15s',
          }}
        >
          <UploadIcon />
          <div style={{ textAlign: 'center' }}>
            <p style={{
              fontSize: 22,
              fontWeight: 600,
              letterSpacing: 1,
              lineHeight: '28px',
              color: 'var(--text-main)',
              marginBottom: 4,
            }}>
              Trascina qui i tuoi documenti oppure{' '}
              <span style={{ color: 'var(--blue-main)', textDecoration: 'underline' }}>
                selezionali
              </span>
              {' '}dal tuo computer
            </p>
            <p style={{
              fontSize: 18,
              fontWeight: 300,
              letterSpacing: 1,
              lineHeight: '28px',
              color: 'var(--text-main)',
            }}>
              Formati accettati: <span style={{ fontWeight: 300 }}>PDF, P7M (max xMb)</span>
            </p>
          </div>
        </div>

        <input
          ref={inputRef}
          type="file"
          accept=".pdf,.p7m"
          multiple
          style={{ display: 'none' }}
          onChange={e => { handleFiles(e.target.files); e.target.value = ''; }}
        />

        {/* File list */}
        {hasFiles && (
          <ul style={{
            marginLeft: 'var(--margin-l)',
            marginTop: 16,
            listStyle: 'none',
            display: 'flex',
            flexDirection: 'column',
            gap: 8,
          }}>
            {files.map((file, i) => (
              <li key={i} style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: '10px 16px',
                border: '1px solid var(--grey-border)',
                borderRadius: 'var(--radius-input)',
                background: 'var(--grey-bg)',
              }}>
                <span style={{ fontSize: 16, fontWeight: 400, letterSpacing: 1, color: 'var(--text-main)' }}>
                  {file.name}
                </span>
                <button
                  onClick={e => { e.stopPropagation(); handleRemove(i); }}
                  aria-label={`Rimuovi ${file.name}`}
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    color: 'var(--grey-text)',
                    fontSize: 18,
                    lineHeight: 1,
                    padding: '0 4px',
                  }}
                >
                  ✕
                </button>
              </li>
            ))}
          </ul>
        )}
      </main>

      <div style={{ height: 96 }} />

      <NavigationToolbar
        onCancel={onCancel}
        onNext={onNext}
        nextDisabled={!hasFiles}
      />
    </div>
  );
}
