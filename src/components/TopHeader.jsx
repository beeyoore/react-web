const BRAND_ICON = 'https://www.figma.com/api/mcp/asset/29929529-992d-47be-b60f-e117188ae908';

function ChevronDown() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"
         stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="4 6 8 10 12 6" />
    </svg>
  );
}

export default function TopHeader({ userName = 'MR' }) {
  return (
    <div style={{
      background: 'var(--blue-main)',
      height: 72,
      display: 'flex',
      alignItems: 'center',
      padding: '0 var(--margin-l)',
      gap: 24,
    }}>
      {/* Brand */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        <img src={BRAND_ICON} alt="" style={{ width: 32, height: 32, objectFit: 'contain' }} />
        <span style={{
          fontSize: 24,
          fontWeight: 700,
          color: 'var(--white)',
          letterSpacing: 0.15,
          whiteSpace: 'nowrap',
        }}>
          RTS placeholder
        </span>
      </div>

      {/* Avatar */}
      <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8 }}>
        <div style={{
          width: 40, height: 40,
          borderRadius: 1000,
          background: 'var(--teal)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: 18,
          fontWeight: 600,
          color: 'var(--teal-dark)',
          letterSpacing: 1,
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
