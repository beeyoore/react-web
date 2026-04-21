import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';

function ArrowRightIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path
        d="M4 10h12M12 6l4 4-4 4"
        stroke="currentColor" strokeWidth="1.75"
        strokeLinecap="round" strokeLinejoin="round"
      />
    </svg>
  );
}

export default function Homepage({ userName, onNuovaPratica }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName={userName} />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      <main style={{
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '80px var(--margin-xl)',
        gap: 40,
      }}>
        {/* Welcome card */}
        <div style={{
          background: 'var(--white)',
          border: '1px solid var(--grey-border)',
          borderRadius: 12,
          padding: '48px 64px',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 32,
          maxWidth: 640,
          width: '100%',
          boxShadow: '0 2px 12px rgba(0,0,0,0.07)',
        }}>
          <div style={{ textAlign: 'center' }}>
            <h1 style={{
              fontSize: 36,
              fontWeight: 700,
              letterSpacing: 0.5,
              lineHeight: '46px',
              color: 'var(--text-main)',
              margin: '0 0 12px',
            }}>
              Benvenuto, {userName}!
            </h1>
            <p style={{
              fontSize: 18,
              fontWeight: 300,
              letterSpacing: 1,
              lineHeight: '28px',
              color: 'var(--text-main)',
              margin: 0,
            }}>
              Seleziona un'operazione per iniziare.
            </p>
          </div>

          <button
            onClick={onNuovaPratica}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 10,
              height: 52,
              padding: '0 28px',
              border: 'none',
              borderRadius: 'var(--radius-btn)',
              background: 'var(--blue-main)',
              cursor: 'pointer',
              fontFamily: 'var(--font)',
              fontSize: 18,
              fontWeight: 500,
              letterSpacing: 1,
              color: 'var(--white)',
              transition: 'opacity 0.15s',
            }}
            onMouseEnter={e => (e.currentTarget.style.opacity = '0.88')}
            onMouseLeave={e => (e.currentTarget.style.opacity = '1')}
          >
            Apri nuova pratica
            <ArrowRightIcon />
          </button>
        </div>
      </main>
    </div>
  );
}
