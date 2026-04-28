const BRAND_ICON = 'https://www.figma.com/api/mcp/asset/29929529-992d-47be-b60f-e117188ae908';

function ChevronDown() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"
         stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="4 6 8 10 12 6" />
    </svg>
  );
}

function HelpIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <circle cx="8" cy="8" r="6.5" stroke="white" strokeWidth="1.2" opacity="0.9" />
      <path d="M6.9 6.1a1.4 1.4 0 1 1 2.2 1.1c-.6.4-1 .7-1 1.4"
        stroke="white" strokeWidth="1.2" strokeLinecap="round" />
      <circle cx="8" cy="11.3" r="0.8" fill="white" />
    </svg>
  );
}

export default function TopHeader({ userName = 'MR' }) {
  return (
    <div style={{
      background: 'var(--blue-main)',
      height: 56,
      display: 'flex',
      alignItems: 'center',
      padding: '0 20px',
      gap: 18,
    }}>
      {/* Brand */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <img src={BRAND_ICON} alt="" style={{ width: 20, height: 20, objectFit: 'contain' }} />
        <span style={{
          fontSize: 30,
          fontWeight: 700,
          color: 'var(--white)',
          letterSpacing: 0.15,
          whiteSpace: 'nowrap',
        }}>
          RTS placeholder
        </span>
      </div>

      <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 10 }}>
        <a href="#" style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 4,
          color: 'var(--white)',
          textDecoration: 'none',
          fontSize: 16,
          fontWeight: 500,
          opacity: 0.95,
        }}>
          <HelpIcon />
          Assistenza
        </a>

        <span style={{ width: 1, height: 18, background: 'rgba(255,255,255,0.5)' }} />

        <div style={{
          width: 24,
          height: 24,
          borderRadius: 1000,
          background: 'var(--teal)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: 12,
          fontWeight: 600,
          color: 'var(--teal-dark)',
          letterSpacing: 0.2,
          flexShrink: 0,
          cursor: 'pointer',
        }}>
          {userName}
        </div>
        <ChevronDown />
      </div>
    </div>
  );
}
