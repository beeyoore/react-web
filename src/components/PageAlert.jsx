function WarningIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
         stroke="#b45309" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
         style={{ flexShrink: 0, marginTop: 2 }}>
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="8" x2="12" y2="12" />
      <line x1="12" y1="16" x2="12.01" y2="16" />
    </svg>
  );
}

export default function PageAlert({ title, message }) {
  return (
    <div role="alert" style={{
      background: '#fff8eb',
      display: 'flex',
      alignItems: 'flex-start',
      gap: 8,
      padding: '8px 40px',
      width: '100%',
    }}>
      <WarningIcon />
      <p style={{
        fontSize: 18,
        fontWeight: 300,
        letterSpacing: 1,
        lineHeight: '28px',
        color: '#000',
        flex: 1,
      }}>
        {title && (
          <strong style={{ fontWeight: 700 }}>{title} </strong>
        )}
        {message}
      </p>
    </div>
  );
}
