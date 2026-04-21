const MEF_LOGO = 'https://www.figma.com/api/mcp/asset/1fc57417-9796-4351-b8db-3b81823fba63';

export default function PreHeader() {
  return (
    <div style={{
      background: 'var(--blue-dark)',
      height: 32,
      display: 'flex',
      alignItems: 'center',
      padding: '4px var(--margin-l)',
      gap: 6,
    }}>
      <img src={MEF_LOGO} alt="MEF" style={{ width: 20, height: 18, objectFit: 'contain' }} />
      <span style={{
        fontSize: 13.5,
        fontWeight: 400,
        color: 'var(--white)',
        letterSpacing: 1,
        whiteSpace: 'nowrap',
      }}>
        Ragioneria Generale dello Stato
      </span>
    </div>
  );
}
