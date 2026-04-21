function ArrowRightIcon({ color }) {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
         stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="5" y1="12" x2="19" y2="12" />
      <polyline points="12 5 19 12 12 19" />
    </svg>
  );
}

export default function NavigationToolbar({ onCancel, onNext, nextDisabled = true }) {
  return (
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
    }}>
      <button
        onClick={onCancel}
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
        Prosegui
        <ArrowRightIcon color={nextDisabled ? 'var(--grey-dim)' : 'var(--white)'} />
      </button>
    </footer>
  );
}
