import { useState } from 'react';

function ArrowRightIcon({ color }) {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
         stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="5" y1="12" x2="19" y2="12" />
      <polyline points="12 5 19 12 12 19" />
    </svg>
  );
}

function WarningIcon() {
  return (
    <svg width="56" height="56" viewBox="0 0 56 56" fill="none" aria-hidden="true">
      <circle cx="28" cy="28" r="28" fill="#f5a623" />
      <path d="M28 16V30" stroke="white" strokeWidth="3" strokeLinecap="round" />
      <circle cx="28" cy="38" r="2" fill="white" />
    </svg>
  );
}

function CancelModal({ onConfirm, onDismiss }) {
  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="cancel-modal-title"
      style={{
        position: 'fixed', inset: 0, zIndex: 400,
        background: 'rgba(0,0,0,0.5)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}
    >
      <div style={{
        background: 'var(--white)',
        borderRadius: 12,
        padding: '40px 48px',
        width: 'min(727px, calc(100vw - 32px))',
        display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16,
        boxShadow: '0 8px 32px rgba(0,0,0,0.2)',
      }}>
        <WarningIcon />

        <h2 id="cancel-modal-title" style={{
          fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px',
          color: 'var(--text-main)', textAlign: 'center', margin: 0,
        }}>
          Vuoi annullare l'inserimento della pratica?
        </h2>

        <p style={{
          fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px',
          color: 'var(--text-main)', textAlign: 'center', margin: 0,
        }}>
          Se annulli, i dati inseriti andranno persi.
        </p>

        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, marginTop: 8, width: '100%' }}>
          <button
            onClick={onConfirm}
            style={{
              flex: 1,
              minWidth: 0,
              minHeight: 48, padding: '12px 16px',
              border: '1px solid var(--blue-main)',
              borderRadius: 'var(--radius-btn)',
              background: 'var(--white)', cursor: 'pointer',
              fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
              letterSpacing: 1, lineHeight: '24px', color: 'var(--blue-main)',
              whiteSpace: 'normal', textAlign: 'center',
            }}
          >
            Annulla e torna alla Homepage
          </button>
          <button
            onClick={onDismiss}
            style={{
              flex: 1,
              minWidth: 0,
              minHeight: 48, padding: '12px 16px',
              border: 'none',
              borderRadius: 'var(--radius-btn)',
              background: 'var(--blue-main)', cursor: 'pointer',
              fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
              letterSpacing: 1, lineHeight: '24px', color: 'var(--white)',
              whiteSpace: 'normal', textAlign: 'center',
            }}
          >
            Rimani sulla pratica
          </button>
        </div>
      </div>
    </div>
  );
}

export default function NavigationToolbar({ onCancel, onNext, nextDisabled = true, nextLabel = 'Prosegui' }) {
  const [showCancelModal, setShowCancelModal] = useState(false);

  return (
    <>
      <footer style={{
        position: 'fixed',
        bottom: 0, left: 0, right: 0,
        borderTop: '1px solid var(--grey-border)',
        background: 'var(--white)',
        height: 96,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '24px var(--margin-xl)',
        gap: 24,
        zIndex: 200,
      }}>
        <button
          onClick={() => setShowCancelModal(true)}
          style={{
            background: 'none',
            border: 'none',
            cursor: 'pointer',
            fontFamily: 'var(--font)',
            fontSize: 18,
            fontWeight: 500,
            color: 'var(--blue-main)',
            letterSpacing: 1,
            padding: 0,
          }}
        >
          Annulla
        </button>

        <button
          onClick={!nextDisabled ? onNext : undefined}
          disabled={nextDisabled}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 8,
            height: 48,
            minWidth: 180,
            padding: '12px 16px',
            borderRadius: 'var(--radius-btn)',
            border: 'none',
            fontFamily: 'var(--font)',
            fontSize: 18,
            fontWeight: 500,
            letterSpacing: 1,
            cursor: nextDisabled ? 'not-allowed' : 'pointer',
            background: nextDisabled ? 'var(--grey-border)' : 'var(--blue-main)',
            color: nextDisabled ? 'var(--grey-dim)' : 'var(--white)',
            transition: 'background 0.15s, color 0.15s',
          }}
        >
          {nextLabel}
          <ArrowRightIcon color={nextDisabled ? 'var(--grey-dim)' : 'var(--white)'} />
        </button>
      </footer>

      {showCancelModal && (
        <CancelModal
          onConfirm={() => { setShowCancelModal(false); onCancel(); }}
          onDismiss={() => setShowCancelModal(false)}
        />
      )}
    </>
  );
}
