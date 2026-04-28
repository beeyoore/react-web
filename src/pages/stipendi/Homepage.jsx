import PreHeader from '../../components/PreHeader';
import TopHeader from '../../components/TopHeader';
import SubheaderMenu from '../../components/SubheaderMenu';

const MOCK_PRACTICES = [
  {
    protocollo: '0012345/2026',
    codiceFiscale: 'RSSMRA80A01H501U',
    dataApertura: '02/04/2026',
    stato: 'Aperta',
    esitoPreliminari: 'superati',
    esitoAmministrativi: 'non_superati_oss',
  },
  {
    protocollo: '0091734/2026',
    codiceFiscale: 'VRDLGI90A15F205X',
    dataApertura: '28/03/2026',
    stato: 'Chiusa',
    esitoPreliminari: 'non_superati_int',
    esitoAmministrativi: 'non_superati_int',
  },
  {
    protocollo: '0026409/2026',
    codiceFiscale: 'BNCGP77C10D612W',
    dataApertura: '25/03/2026',
    stato: 'Aperta',
    esitoPreliminari: 'in_lavorazione',
    esitoAmministrativi: 'in_lavorazione',
  },
  {
    protocollo: '0078912/2026',
    codiceFiscale: 'FRRPLA92T41G273Y',
    dataApertura: '11/03/2026',
    stato: 'Aperta',
    esitoPreliminari: 'non_superati_oss',
    esitoAmministrativi: 'non_superati_oss',
  },
  {
    protocollo: '0053267/2026',
    codiceFiscale: 'LMBCRL88E22F205V',
    dataApertura: '26/02/2026',
    stato: 'Aperta',
    esitoPreliminari: 'superati',
    esitoAmministrativi: 'superati',
  },
];

const STATUS_CHIP = {
  superati: {
    label: 'SUPERATI',
    background: '#eaf4ea',
    border: '#8ec58e',
    color: '#3a6f3a',
  },
  in_lavorazione: {
    label: 'IN LAVORAZIONE',
    background: '#fff8eb',
    border: '#d09a4a',
    color: '#8f6018',
  },
  non_superati_int: {
    label: 'NON SUPERATI - INTEGRAZIONE',
    background: '#fff6f6',
    border: '#f19a9a',
    color: '#b74646',
  },
  non_superati_oss: {
    label: 'NON SUPERATI - OSSERVAZIONE',
    background: '#fff6f6',
    border: '#f19a9a',
    color: '#b74646',
  },
};

function ArrowRightIcon({ small = false }) {
  const size = small ? 16 : 20;
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path
        d="M4 10h12M12 6l4 4-4 4"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function DocumentIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"
        stroke="var(--blue-main)"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <polyline
        points="14 2 14 8 20 8"
        stroke="var(--blue-main)"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function StatusChip({ value }) {
  const cfg = STATUS_CHIP[value] || STATUS_CHIP.in_lavorazione;

  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        borderRadius: 4,
        border: `1px solid ${cfg.border}`,
        background: cfg.background,
        color: cfg.color,
        fontSize: 9,
        lineHeight: '12px',
        letterSpacing: 0.3,
        padding: '2px 6px',
        fontWeight: 700,
        textTransform: 'uppercase',
        whiteSpace: 'nowrap',
      }}
    >
      {cfg.label}
    </span>
  );
}

function FooterBar() {
  return (
    <footer style={{ marginTop: 'auto', background: 'var(--blue-main)' }}>
      <div
        style={{
          maxWidth: 1260,
          margin: '0 auto',
          padding: '18px 24px 12px',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          color: 'var(--white)',
        }}
      >
        <div
          style={{
            width: 20,
            height: 20,
            border: '1px solid rgba(255,255,255,0.8)',
            borderRadius: 2,
            display: 'grid',
            placeItems: 'center',
            fontSize: 12,
            lineHeight: '12px',
            fontWeight: 700,
          }}
        >
          x
        </div>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
          <span style={{ fontSize: 22, fontWeight: 700, lineHeight: '24px' }}>RTS</span>
          <span style={{ fontSize: 11, fontWeight: 500, letterSpacing: 0.4 }}>PLACEHOLDER</span>
        </div>
      </div>

      <div
        style={{
          maxWidth: 1260,
          margin: '0 auto',
          borderTop: '1px solid rgba(255,255,255,0.32)',
          padding: '10px 24px 14px',
          display: 'flex',
          alignItems: 'center',
          gap: 24,
          flexWrap: 'wrap',
        }}
      >
        {['Termini e condizioni', 'Informativa privacy', 'Dichiarazione di accessibilita'].map((item) => (
          <a
            key={item}
            href="#"
            style={{
              color: 'var(--white)',
              fontSize: 12,
              textDecoration: 'underline',
              textUnderlineOffset: 2,
            }}
          >
            {item}
          </a>
        ))}
      </div>
    </footer>
  );
}

// Variant-specific content for Stipendi
const VARIANT_CONTENT = {
  T0: {
    title: (salutation, displayName, userName) => `${salutation} ${displayName || userName},`,
    description: 'RTS (TDB nome) è la piattaforma che utilizza l\'intelligenza artificiale per supportarti nella gestione degli stipendi e delle pratiche correlate. Carica la documentazione necessaria, verifica le anomalie segnalate e consulta l\'esito proposto dal sistema. Gestisci facilmente tutte le tue pratiche in un unico posto.',
  },
  T1: {
    title: (salutation, displayName, userName) => {
      // For T1, replace greeting with "Bentornata/o"
      const genderSuffix = salutation === 'Benvenuta' ? 'a' : 'o';
      return `Bentornat${genderSuffix} ${displayName || userName},`;
    },
    description: 'Apri una nuova pratica per avviare i controlli amministrativi oppure monitora lo stato delle pratiche per verificare gli esiti dei controlli effettuati. Scopri più dettagli!',
  },
};

export default function HomepageStipendi({ variant = 'T0', userName, displayName, salutation = 'Benvenuto', onNuovaPratica }) {
  const recentPractices = MOCK_PRACTICES.slice(0, 2);
  const hasPractices = MOCK_PRACTICES.length > 0;
  
  const content = VARIANT_CONTENT[variant] || VARIANT_CONTENT.T0;
  const titleText = content.title(salutation, displayName, userName);
  const descriptionText = content.description;

  return (
    <div className="homepage-stipendi" style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh', background: '#f5f6f8' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName={userName} />
        <SubheaderMenu items={['Home']} />
      </header>

      <main style={{ flex: 1 }}>
        <section
          className="homepage-hero"
          style={{
            maxWidth: 1260,
            margin: '0 auto',
            minHeight: 300,
            background: 'linear-gradient(120deg, #f4f4f5 0%, #f4f4f5 35%, #eceef1 35%, #f1f2f4 100%)',
            display: 'grid',
            gridTemplateColumns: '1.05fr 1fr',
            alignItems: 'center',
            gap: 28,
            padding: '28px 70px 24px',
          }}
        >
          <div>
            <h1
              className="homepage-title"
              style={{
                margin: '0 0 14px',
                color: '#202124',
                fontSize: 44,
                lineHeight: '52px',
                fontWeight: 700,
              }}
            >
              {titleText}
            </h1>
            <p
              className="homepage-description"
              style={{
                margin: '0 0 20px',
                color: '#2f3136',
                fontSize: 20,
                lineHeight: '30px',
                maxWidth: 640,
              }}
            >
              {descriptionText}
            </p>

            <button
              onClick={onNuovaPratica}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 8,
                height: 44,
                padding: '0 16px',
                border: 'none',
                borderRadius: 6,
                background: 'var(--blue-main)',
                cursor: 'pointer',
                fontFamily: 'var(--font)',
                fontSize: 16,
                fontWeight: 600,
                color: 'var(--white)',
              }}
            >
              Apri una nuova pratica
            </button>
          </div>

          <div
            style={{
              justifySelf: 'end',
              width: '100%',
              maxWidth: 520,
              minHeight: 220,
              borderRadius: 12,
              background:
                'radial-gradient(circle at 12% 22%, rgba(27,135,255,0.12) 0, rgba(27,135,255,0) 34%), radial-gradient(circle at 88% 75%, rgba(0,110,230,0.12) 0, rgba(0,110,230,0) 35%), linear-gradient(160deg, #f7f8fa 0%, #f0f2f5 100%)',
              border: '1px solid #d8dde5',
              display: 'grid',
              placeItems: 'center',
              padding: 20,
            }}
          >
            <img
              src="/image.png"
              alt="Illustrazione team al lavoro"
              style={{ width: '100%', maxWidth: 420, height: 'auto', objectFit: 'contain' }}
            />
          </div>
        </section>

        {hasPractices ? (
          <>
            <section className="homepage-section" style={{ maxWidth: 1260, margin: '0 auto', background: '#e8edf5', padding: '22px 70px' }}>
              <h2 className="homepage-heading" style={{ fontSize: 36, lineHeight: '44px', fontWeight: 700, margin: '0 0 16px', color: '#2b2e33' }}>
                Ultime pratiche lavorate
              </h2>

              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
                  gap: 14,
                }}
              >
                {recentPractices.map((practice, index) => (
                  <article
                    key={practice.protocollo + index}
                    style={{
                      background: '#fff',
                      border: '1px solid #d7dce5',
                      borderRadius: 8,
                      padding: '14px 16px',
                      display: 'grid',
                      gridTemplateColumns: 'auto 1fr auto',
                      gap: 12,
                      alignItems: 'start',
                    }}
                  >
                    <DocumentIcon />
                    <div>
                      <p style={{ fontSize: 10, lineHeight: '14px', color: '#80858d', margin: '0 0 6px' }}>
                        ID {practice.protocollo} | 02 aprile 2026
                      </p>
                      <h3 style={{ margin: '0 0 2px', fontSize: 26, lineHeight: '32px', fontWeight: 700, color: '#31343a' }}>
                        {practice.stato === 'Chiusa' ? 'Pratica chiusa' : 'Pratica aperta'}
                      </h3>
                      <p style={{ margin: 0, fontSize: 24, lineHeight: '32px', color: '#5c616a' }}>
                        Protocollo: {practice.protocollo}
                      </p>
                    </div>

                    <a
                      href="#"
                      style={{
                        marginTop: 10,
                        color: 'var(--blue-main)',
                        fontSize: 20,
                        lineHeight: '24px',
                        fontWeight: 700,
                        textDecoration: 'none',
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 4,
                      }}
                    >
                      Vai al dettaglio
                      <ArrowRightIcon small />
                    </a>
                  </article>
                ))}
              </div>
            </section>

            <section className="homepage-section" style={{ maxWidth: 1260, margin: '0 auto', background: '#f4f5f7', padding: '22px 70px 32px' }}>
              <h2 className="homepage-heading" style={{ fontSize: 38, lineHeight: '44px', fontWeight: 700, margin: '0 0 16px', color: '#2b2e33' }}>
                Lista pratiche
              </h2>

              <div
                style={{
                  border: '1px solid #c9d1de',
                  borderRadius: 8,
                  background: '#fff',
                  overflowX: 'auto',
                }}
              >
                <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 1080 }}>
                  <thead>
                    <tr style={{ background: '#f8f9fc', borderBottom: '1px solid #c9d1de' }}>
                      {[
                        'Protocollo',
                        'Codice fiscale',
                        'Data apertura',
                        'Stato',
                        'Esito controlli preliminari',
                        'Esito controlli amministrativi',
                        'Azioni',
                      ].map((head) => (
                        <th
                          key={head}
                          style={{
                            textAlign: 'left',
                            padding: '10px 12px',
                            fontSize: 12,
                            lineHeight: '16px',
                            fontWeight: 600,
                            color: '#4f5560',
                          }}
                        >
                          {head}
                        </th>
                      ))}
                    </tr>
                  </thead>

                  <tbody>
                    {MOCK_PRACTICES.map((practice, index) => (
                      <tr key={practice.protocollo} style={{ background: index % 2 ? '#f6f8fd' : '#fff' }}>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: '#5f6570', borderTop: '1px solid #e0e5ef' }}>
                          {practice.protocollo}
                        </td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: '#5f6570', borderTop: '1px solid #e0e5ef' }}>
                          {practice.codiceFiscale}
                        </td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: '#5f6570', borderTop: '1px solid #e0e5ef' }}>
                          {practice.dataApertura}
                        </td>
                        <td style={{ padding: '10px 12px', fontSize: 12, color: '#5f6570', borderTop: '1px solid #e0e5ef' }}>
                          {practice.stato}
                        </td>
                        <td style={{ padding: '10px 12px', borderTop: '1px solid #e0e5ef' }}>
                          <StatusChip value={practice.esitoPreliminari} />
                        </td>
                        <td style={{ padding: '10px 12px', borderTop: '1px solid #e0e5ef' }}>
                          <StatusChip value={practice.esitoAmministrativi} />
                        </td>
                        <td style={{ padding: '10px 12px', borderTop: '1px solid #e0e5ef' }}>
                          <a href="#" style={{ color: 'var(--blue-main)', fontSize: 13, fontWeight: 700, textDecoration: 'none' }}>
                            Vai al dettaglio
                          </a>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  gap: 8,
                  paddingTop: 16,
                  color: '#5d6470',
                }}
              >
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: 12 }}>
                  <button
                    style={{ border: 'none', background: 'transparent', color: '#a2a7b1', fontSize: 18, cursor: 'pointer' }}
                    aria-label="Pagina precedente"
                  >
                    &#8249;
                  </button>
                  <span
                    style={{
                      width: 28,
                      height: 28,
                      borderRadius: 6,
                      display: 'grid',
                      placeItems: 'center',
                      background: 'var(--blue-main)',
                      color: '#fff',
                      fontSize: 12,
                      fontWeight: 700,
                    }}
                  >
                    01
                  </span>
                  <span style={{ color: 'var(--blue-main)', fontSize: 13, fontWeight: 600 }}>02</span>
                  <span style={{ color: 'var(--blue-main)', fontSize: 13, fontWeight: 600 }}>... 05</span>
                  <button
                    style={{ border: 'none', background: 'transparent', color: 'var(--blue-main)', fontSize: 18, cursor: 'pointer' }}
                    aria-label="Pagina successiva"
                  >
                    &#8250;
                  </button>
                </div>
                <p style={{ margin: 0, fontSize: 12 }}>05 di 23 risultati</p>
              </div>
            </section>
          </>
        ) : (
          <section className="homepage-section" style={{ maxWidth: 1260, margin: '0 auto', padding: '62px 24px 52px' }}>
            <p style={{ textAlign: 'center', fontSize: 14, color: '#2f3035', margin: '0 0 6px' }}>
              Non sono presenti pratiche
            </p>
            <p style={{ textAlign: 'center', fontSize: 14, color: '#6f7580', margin: 0 }}>
              Quando aprirai delle pratiche le troverai qui.
            </p>
          </section>
        )}
      </main>

      <style>{`
        @media (max-width: 980px) {
          .homepage-stipendi .homepage-hero {
            grid-template-columns: 1fr !important;
            padding: 20px !important;
          }
          .homepage-stipendi .homepage-section {
            padding-left: 16px !important;
            padding-right: 16px !important;
          }
          .homepage-stipendi .homepage-title {
            font-size: 32px !important;
            line-height: 40px !important;
          }
          .homepage-stipendi .homepage-description {
            font-size: 16px !important;
            line-height: 24px !important;
          }
          .homepage-stipendi .homepage-heading {
            font-size: 28px !important;
            line-height: 34px !important;
          }
        }
      `}</style>

      <FooterBar />
    </div>
  );
}
