export default function SubheaderMenu({ items = ['Home', 'Assistenza'], active = 'Home' }) {
  return (
    <>
      <nav style={{
        background: 'var(--blue-main)',
        height: 48,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '0 var(--margin-l)',
        fontSize: 18,
        fontWeight: 500,
        color: 'var(--white)',
        letterSpacing: 1,
        whiteSpace: 'nowrap',
      }}>
        {items.map((item) => (
          <span key={item} style={{ cursor: 'pointer' }}>{item}</span>
        ))}
      </nav>
      <div style={{ height: 1, background: 'var(--blue-light)' }} />
    </>
  );
}
