import { useEffect, useRef, useState } from 'react';
import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import { getPratica } from '../api/getPratica';
import { convalidaControlli } from '../api/convalidaControlli';

const POLL_INTERVAL = 10_000;
const TERMINAL = new Set(['superato', 'non_superato', 'errore']);

const MONTHS_IT = [
  'gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
  'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre',
];

function formatDateShort(d) {
  if (!d) return '—';
  return [
    String(d.getDate()).padStart(2, '0'),
    String(d.getMonth() + 1).padStart(2, '0'),
    d.getFullYear(),
  ].join('/');
}

function formatDateLong(d) {
  if (!d) return '';
  return `${d.getDate()} ${MONTHS_IT[d.getMonth()]} ${d.getFullYear()}`;
}

function getExt(name) {
  const parts = name.split('.');
  return parts.length > 1 ? parts.pop().toUpperCase() : '';
}

function formatSize(bytes) {
  const mb = bytes / (1024 * 1024);
  return mb.toFixed(1).replace('.', ',') + 'mb';
}

function allTerminal(controls) {
  return controls.length > 0 && controls.every(c => TERMINAL.has(c.esito));
}

function deriveControlliStatus(controls) {
  if (!controls || controls.length === 0) return 'in_lavorazione';
  if (!allTerminal(controls)) return 'in_lavorazione';
  if (controls.every(c => c.esito === 'superato')) return 'superati';
  return 'non_superati';
}

function deriveAmmStatus(controls) {
  if (!controls || controls.length === 0) return 'non_avviati';
  if (!allTerminal(controls)) return 'in_lavorazione';
  if (controls.every(c => c.esito === 'superato')) return 'superati';
  return 'non_superati';
}

// ── Icons ──────────────────────────────────────────────────────────────────

function HomeIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <path d="M3 9.5L12 2.5L21 9.5V21H15V15H9V21H3V9.5Z"
        stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function FileIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"
        stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      <polyline points="14 2 14 8 20 8"
        stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function BulletDot() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <circle cx="8" cy="8" r="2" fill="#494949" />
    </svg>
  );
}

function IconDashboard() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <rect x="3" y="3" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <rect x="13" y="3" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <rect x="3" y="13" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <rect x="13" y="13" width="8" height="8" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
    </svg>
  );
}

function IconList() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <rect x="3" y="5" width="4" height="4" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="10" y1="7" x2="21" y2="7" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <rect x="3" y="10" width="4" height="4" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="10" y1="12" x2="21" y2="12" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <rect x="3" y="15" width="4" height="4" rx="1" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="10" y1="17" x2="21" y2="17" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function IconSliders() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <line x1="4" y1="6" x2="20" y2="6" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="8" cy="6" r="2.5" fill="white" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="4" y1="12" x2="20" y2="12" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="16" cy="12" r="2.5" fill="white" stroke="var(--blue-main)" strokeWidth="1.5" />
      <line x1="4" y1="18" x2="20" y2="18" stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="10" cy="18" r="2.5" fill="white" stroke="var(--blue-main)" strokeWidth="1.5" />
    </svg>
  );
}

// X circle icon for "Non superato" chip
function IconXCircle() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <circle cx="12" cy="12" r="9" fill="#fff6f6" stroke="#f66f6f" strokeWidth="1.5" />
      <path d="M15 9L9 15M9 9l6 6" stroke="#e60000" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

// ── Status tag ─────────────────────────────────────────────────────────────

const TAG_STYLES = {
  non_avviati:   { background: '#fafafa',  color: '#494949', border: '1px solid #737373' },
  in_lavorazione:{ background: '#fff8eb',  color: '#494949', border: '1px solid #c25700' },
  superati:      { background: '#eaf4ea',  color: '#2a6b2a', border: '1px solid #82c282' },
  non_superati:  { background: '#fff6f6',  color: '#e60000', border: '1px solid #e60000' },
};

const TAG_LABELS = {
  non_avviati:    'NON AVVIATI',
  in_lavorazione: 'IN LAVORAZIONE',
  superati:       'SUPERATI',
  non_superati:   'NON SUPERATI - INTEGRAZIONE',
};

function StatusTag({ status }) {
  const style = TAG_STYLES[status] || TAG_STYLES.non_avviati;
  return (
    <span style={{
      display: 'inline-block', padding: '4px 8px', borderRadius: 8,
      fontSize: 12, fontWeight: 500, letterSpacing: 1, textTransform: 'uppercase',
      whiteSpace: 'nowrap', flexShrink: 0, ...style,
    }}>
      {TAG_LABELS[status]}
    </span>
  );
}

// ── Esito chip ─────────────────────────────────────────────────────────────

const ESITO_CONFIG = {
  superato:           { bg: '#f0fff4', border: '#82c282', text: '#2a6b2a', label: 'Superato' },
  non_superato:       { bg: '#fff6f6', border: '#f66f6f', text: '#202124', label: 'Non superato', icon: 'x' },
  errore:             { bg: '#fdecea', border: '#f44336', text: '#b71c1c', label: 'Errore', icon: 'x' },
  in_lavorazione:     { bg: '#fff8eb', border: '#c25700', text: '#a05c00', label: 'In lavorazione' },
  in_attesa_categoria:{ bg: '#fafafa', border: '#737373', text: '#494949', label: 'In attesa' },
};

function EsitoChip({ esito }) {
  const cfg = ESITO_CONFIG[esito] || ESITO_CONFIG.in_lavorazione;
  return (
    <div style={{
      display: 'inline-flex', alignItems: 'center', gap: 0,
      height: 32, paddingLeft: cfg.icon ? 4 : 8, paddingRight: 8,
      borderRadius: 1000, border: `1px solid ${cfg.border}`,
      background: cfg.bg, flexShrink: 0,
    }}>
      {cfg.icon === 'x' && <IconXCircle />}
      <span style={{
        paddingLeft: 4, fontSize: 16, fontWeight: 300,
        letterSpacing: 1, lineHeight: '24px', color: cfg.text, whiteSpace: 'nowrap',
      }}>
        {cfg.label}
      </span>
    </div>
  );
}

// ── Read-only field ────────────────────────────────────────────────────────

function ReadOnlyField({ label, value, style }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, ...style }}>
      <p style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', margin: 0, whiteSpace: 'nowrap' }}>
        {label}
      </p>
      <p style={{ fontSize: 18, fontWeight: 400, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
        {value || '—'}
      </p>
    </div>
  );
}

// ── Document card ──────────────────────────────────────────────────────────

function DocumentCard({ file }) {
  const today = new Date();
  return (
    <div style={{
      flex: '1 0 0', minWidth: 0, background: 'var(--white)',
      border: '1px solid var(--grey-border)', borderRadius: 8,
      padding: 16, display: 'flex', gap: 16, alignItems: 'flex-start',
    }}>
      <FileIcon />
      <div style={{ flex: '1 0 0', minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4 }}>
        <p style={{
          fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px',
          color: 'var(--blue-main)', textDecoration: 'underline', margin: 0,
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>
          {file.name}
        </p>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 12, fontWeight: 300, letterSpacing: 1, lineHeight: '16px', color: '#494949', whiteSpace: 'nowrap' }}>
            {formatDateLong(today)}
          </span>
          <BulletDot />
          <span style={{ fontSize: 12, fontWeight: 300, letterSpacing: 1, lineHeight: '16px', color: '#494949', whiteSpace: 'nowrap' }}>
            {getExt(file.name)} - {formatSize(file.size)}
          </span>
        </div>
      </div>
    </div>
  );
}

// ── Tabs ───────────────────────────────────────────────────────────────────

const TABS = [
  { label: 'Informazioni sulla pratica',        Icon: IconDashboard },
  { label: 'Controlli preliminari',              Icon: IconList },
  { label: 'Controlli amministrativo-contabili', Icon: IconSliders },
];

function TabMenu({ activeTab, onTabChange }) {
  return (
    <nav style={{
      width: 323, flexShrink: 0, display: 'flex', flexDirection: 'column', gap: 8,
      alignSelf: 'flex-start', position: 'sticky', top: 160,
    }}>
      {TABS.map(({ label, Icon }) => {
        const isActive = label === activeTab;
        return (
          <button key={label} onClick={() => onTabChange(label)} style={{
            display: 'flex', alignItems: 'center', gap: 8, width: '100%',
            textAlign: 'left', padding: '12px 16px', border: 'none', borderRadius: 8,
            background: isActive ? '#f5f9ff' : 'transparent', cursor: 'pointer',
            fontFamily: 'var(--font)', fontSize: 18, fontWeight: isActive ? 600 : 300,
            letterSpacing: 1, lineHeight: '28px', color: 'var(--blue-main)',
          }}>
            <Icon />
            {label}
          </button>
        );
      })}
    </nav>
  );
}

// ── Info card ──────────────────────────────────────────────────────────────

function InfoPraticaCard({ praticeData, uploadedFiles }) {
  const { protocollo = '', dataPec = null, codiceFiscale = '' } = praticeData || {};
  const rows = [];
  for (let i = 0; i < uploadedFiles.length; i += 2) rows.push(uploadedFiles.slice(i, i + 2));

  return (
    <div style={{
      background: '#fbfcff', border: '1px solid var(--grey-border)', borderRadius: 8,
      padding: 16, display: 'flex', flexDirection: 'column', gap: 24,
    }}>
      <h4 style={{ fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px', color: 'var(--text-main)', margin: 0 }}>
        Informazioni sulla pratica
      </h4>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <p style={{ fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
          Dati della pratica
        </p>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
          <ReadOnlyField label="Numero di protocollo del decreto" value={protocollo} style={{ width: 264 }} />
          <ReadOnlyField label="Data di recezione della PEC" value={formatDateShort(dataPec)} style={{ width: 264 }} />
          <ReadOnlyField label="Codice fiscale" value={codiceFiscale} style={{ width: 264 }} />
        </div>
      </div>
      {uploadedFiles.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <p style={{ fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
            Documenti caricati
          </p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {rows.map((row, rowIdx) => (
              <div key={rowIdx} style={{ display: 'flex', gap: 24 }}>
                {row.map((file, colIdx) => <DocumentCard key={rowIdx * 2 + colIdx} file={file} />)}
                {row.length === 1 && <div style={{ flex: '1 0 0' }} />}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Controlli table ────────────────────────────────────────────────────────

const COL_WIDTHS = { checkbox: 48, esito: 166, convalidato: 136, azioni: 170 };

const HEADER_STYLE = {
  background: 'white', borderBottom: '1px solid #bbc5d7',
  height: 64, display: 'flex', alignItems: 'center', padding: 12,
  fontSize: 16, fontWeight: 500, letterSpacing: 1, color: 'var(--text-main)',
};

function TableRow({ ctrl, index, checked, onCheck }) {
  const isEven = index % 2 === 1;
  const cellBg = isEven ? '#eef1f8' : '#fbfcff';
  const cellStyle = {
    background: cellBg, borderBottom: '1px solid #bbc5d7',
    height: 64, display: 'flex', alignItems: 'center', padding: 12,
  };
  const isDisabled = !!ctrl.convalidato;
  return (
    <>
      {/* Checkbox */}
      <div style={{ ...cellStyle, width: COL_WIDTHS.checkbox, justifyContent: 'center', flexShrink: 0 }}>
        <div
          role="checkbox"
          aria-checked={checked}
          aria-disabled={isDisabled}
          tabIndex={isDisabled ? -1 : 0}
          onClick={() => !isDisabled && onCheck(!checked)}
          onKeyDown={e => !isDisabled && e.key === ' ' && onCheck(!checked)}
          style={{
            width: 24, height: 24, borderRadius: 4, flexShrink: 0,
            cursor: isDisabled ? 'not-allowed' : 'pointer',
            border: `1px solid ${isDisabled ? '#bdbdbd' : 'var(--blue-main)'}`,
            background: isDisabled ? '#f5f5f5' : checked ? 'var(--blue-main)' : 'transparent',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}
        >
          {checked && !isDisabled && (
            <svg width="14" height="10" viewBox="0 0 14 10" fill="none">
              <path d="M1 5L5 9L13 1" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          )}
        </div>
      </div>
      {/* Controllo */}
      <div style={{ ...cellStyle, flex: '1 0 0', minWidth: 0 }}>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)' }}>
          {ctrl.nome}
        </span>
      </div>
      {/* Esito */}
      <div style={{ ...cellStyle, width: COL_WIDTHS.esito, flexShrink: 0 }}>
        <EsitoChip esito={ctrl.esito} />
      </div>
      {/* Convalidato */}
      <div style={{ ...cellStyle, width: COL_WIDTHS.convalidato, flexShrink: 0 }}>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)' }}>
          {ctrl.convalidato ? 'Sì' : 'No'}
        </span>
      </div>
      {/* Azioni */}
      <div style={{ ...cellStyle, width: COL_WIDTHS.azioni, flexShrink: 0 }}>
        <button style={{
          background: 'none', border: 'none', cursor: 'pointer', padding: 0,
          fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
          letterSpacing: 1, color: 'var(--blue-main)', whiteSpace: 'nowrap',
        }}>
          Vai al dettaglio
        </button>
      </div>
    </>
  );
}

function ControlliTable({ controls, checked, onCheckAll, onCheckRow }) {
  const allChecked = controls.length > 0 && controls.every((_, i) => checked[i]);

  return (
    <div style={{ border: '1px solid #bbc5d7', borderRadius: 8, overflow: 'hidden', width: '100%' }}>
      {/* Header */}
      <div style={{ display: 'flex' }}>
        <div style={{ ...HEADER_STYLE, width: COL_WIDTHS.checkbox, justifyContent: 'center', flexShrink: 0 }}>
          <div
            role="checkbox"
            aria-checked={allChecked}
            tabIndex={0}
            onClick={() => onCheckAll(!allChecked)}
            onKeyDown={e => e.key === ' ' && onCheckAll(!allChecked)}
            style={{
              width: 24, height: 24, borderRadius: 4, flexShrink: 0, cursor: 'pointer',
              border: '1px solid var(--blue-main)',
              background: allChecked ? 'var(--blue-main)' : 'transparent',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}
          >
            {allChecked && (
              <svg width="14" height="10" viewBox="0 0 14 10" fill="none">
                <path d="M1 5L5 9L13 1" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            )}
          </div>
        </div>
        <div style={{ ...HEADER_STYLE, flex: '1 0 0', minWidth: 0 }}>Controllo</div>
        <div style={{ ...HEADER_STYLE, width: COL_WIDTHS.esito, flexShrink: 0 }}>Esito</div>
        <div style={{ ...HEADER_STYLE, width: COL_WIDTHS.convalidato, flexShrink: 0 }}>Convalidato</div>
        <div style={{ ...HEADER_STYLE, width: COL_WIDTHS.azioni, flexShrink: 0 }}>Azioni</div>
      </div>
      {/* Rows */}
      {controls.map((ctrl, i) => (
        <div key={ctrl.id} style={{ display: 'flex' }}>
          <TableRow
            ctrl={ctrl}
            index={i}
            checked={!!checked[i]}
            onCheck={(val) => onCheckRow(i, val)}
          />
        </div>
      ))}
    </div>
  );
}

// ── Modals ─────────────────────────────────────────────────────────────────

function ModalOverlay({ children }) {
  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 1000,
      background: 'rgba(0,0,0,0.45)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
    }}>
      {children}
    </div>
  );
}

function ConvalidaAlertModal({ onAnnulla, onConferma }) {
  return (
    <ModalOverlay>
      <div style={{
        background: 'white', borderRadius: 12, padding: '40px 48px',
        maxWidth: 500, width: '90%', display: 'flex', flexDirection: 'column',
        alignItems: 'center', gap: 16, boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
      }}>
        {/* Warning icon */}
        <div style={{
          width: 56, height: 56, borderRadius: '50%', background: '#fff8eb',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
            <path d="M14 4L25 23H3L14 4Z" stroke="#c25700" strokeWidth="2" strokeLinejoin="round" fill="#fff8eb" />
            <line x1="14" y1="12" x2="14" y2="18" stroke="#c25700" strokeWidth="2" strokeLinecap="round" />
            <circle cx="14" cy="21" r="1" fill="#c25700" />
          </svg>
        </div>
        <h3 style={{ fontSize: 22, fontWeight: 600, letterSpacing: 1, lineHeight: '30px', color: 'var(--text-main)', margin: 0, textAlign: 'center' }}>
          Sei sicuro di voler convalidare<br />i controlli preliminari?
        </h3>
        <p style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', margin: 0, textAlign: 'center' }}>
          Se convalidi, i controlli preliminari non saranno più modificabili.
        </p>
        <div style={{ display: 'flex', gap: 16, marginTop: 8, width: '100%', justifyContent: 'center' }}>
          <button
            onClick={onAnnulla}
            style={{
              flex: 1, height: 44, borderRadius: 8, border: '2px solid var(--blue-main)',
              background: 'white', cursor: 'pointer', fontFamily: 'var(--font)',
              fontSize: 16, fontWeight: 500, letterSpacing: 1, color: 'var(--blue-main)',
            }}
          >
            Annulla
          </button>
          <button
            onClick={onConferma}
            style={{
              flex: 1, height: 44, borderRadius: 8, border: 'none',
              background: 'var(--blue-main)', cursor: 'pointer', fontFamily: 'var(--font)',
              fontSize: 16, fontWeight: 500, letterSpacing: 1, color: 'white',
            }}
          >
            Conferma e convalida
          </button>
        </div>
      </div>
    </ModalOverlay>
  );
}

function ConvalidaSuccessModal({ onHome, onRimani }) {
  return (
    <ModalOverlay>
      <div style={{
        background: 'white', borderRadius: 12, padding: '40px 48px',
        maxWidth: 500, width: '90%', display: 'flex', flexDirection: 'column',
        alignItems: 'center', gap: 16, boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
      }}>
        {/* Green checkmark */}
        <div style={{
          width: 56, height: 56, borderRadius: '50%', background: '#eaf4ea',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
            <path d="M5 14L11 20L23 8" stroke="#2a6b2a" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>
        <h3 style={{ fontSize: 22, fontWeight: 600, letterSpacing: 1, lineHeight: '30px', color: 'var(--text-main)', margin: 0, textAlign: 'center' }}>
          I controlli preliminari sono stati<br />convalidati con successo!
        </h3>
        <p style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', margin: 0, textAlign: 'center' }}>
          Non verranno effettuati ulteriori controlli, in quanto quelli preliminari non sono stati superati
        </p>
        <div style={{ display: 'flex', gap: 16, marginTop: 8, width: '100%', justifyContent: 'center' }}>
          <button
            onClick={onHome}
            style={{
              flex: 1, height: 44, borderRadius: 8, border: '2px solid var(--blue-main)',
              background: 'white', cursor: 'pointer', fontFamily: 'var(--font)',
              fontSize: 16, fontWeight: 500, letterSpacing: 1, color: 'var(--blue-main)',
            }}
          >
            Torna alla Homepage
          </button>
          <button
            onClick={onRimani}
            style={{
              flex: 1, height: 44, borderRadius: 8, border: 'none',
              background: 'var(--blue-main)', cursor: 'pointer', fontFamily: 'var(--font)',
              fontSize: 16, fontWeight: 500, letterSpacing: 1, color: 'white',
            }}
          >
            Rimani sulla pratica
          </button>
        </div>
      </div>
    </ModalOverlay>
  );
}

// ── Controlli card ─────────────────────────────────────────────────────────

const DESCRIZIONE_STATUS = {
  non_avviati:    'I controlli preliminari non sono ancora stati avviati.',
  in_lavorazione: 'I controlli preliminari sono in corso di elaborazione. Gli esiti saranno disponibili al completamento.',
  superati:       'Tutti i controlli preliminari sono stati superati.',
  non_superati:   'Controlli non superati. Il sistema propone una richiesta di integrazione e blocca i controlli successivi fino al superamento dei controlli preliminari.',
};

function ControlliPreliminaryCard({ controls, idPratica, onRefresh, onHome }) {
  const status = deriveControlliStatus(controls);
  const [checked, setChecked] = useState({});
  const [showAlert, setShowAlert] = useState(false);
  const [showSuccess, setShowSuccess] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState(null);

  // ids of controls already convalidated (derived from server data)
  const convalidatiSet = new Set(
    controls.map((c, i) => (c.convalidato ? i : null)).filter(i => i !== null)
  );

  const checkedIndices = Object.entries(checked).filter(([, v]) => v).map(([k]) => Number(k));
  const anyChecked = checkedIndices.length > 0;
  const allChecked = controls.length > 0 && checkedIndices.length === controls.length;
  const buttonLabel = allChecked ? 'Convalida tutti' : 'Convalida';
  const allDone = controls.length > 0 && controls.every(c => c.convalidato);

  function handleCheckAll(val) {
    const next = {};
    controls.forEach((_, i) => { next[i] = val; });
    setChecked(next);
  }

  function handleCheckRow(i, val) {
    setChecked(prev => ({ ...prev, [i]: val }));
  }

  function handleConvalidaClick() {
    setSaveError(null);
    setShowAlert(true);
  }

  function handleAnnulla() {
    setShowAlert(false);
  }

  async function handleConferma() {
    setShowAlert(false);
    setSaving(true);
    setSaveError(null);
    try {
      const selectedIds = checkedIndices.map(i => controls[i].id);
      await convalidaControlli(idPratica, 'preliminare', selectedIds);
      setChecked({});
      // Refresh data from server so the new convalidato=true is persisted and visible
      if (onRefresh) await onRefresh();
      setShowSuccess(true);
    } catch (err) {
      setSaveError(err.message || 'Errore durante la convalida. Riprova.');
    } finally {
      setSaving(false);
    }
  }

  function handleRimani() {
    setShowSuccess(false);
  }

  function handleHome() {
    setShowSuccess(false);
    if (onHome) onHome();
  }

  // displayControls: use server data directly (convalidato already reflects DB state after refresh)
  const displayControls = controls;

  return (
    <>
      {showAlert && (
        <ConvalidaAlertModal onAnnulla={handleAnnulla} onConferma={handleConferma} />
      )}
      {showSuccess && (
        <ConvalidaSuccessModal onHome={handleHome} onRimani={handleRimani} />
      )}

      <div style={{
        background: '#fbfcff', border: '1px solid var(--grey-border)', borderRadius: 8,
        padding: 16, display: 'flex', flexDirection: 'column', gap: 24,
      }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
          <h4 style={{ fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px', color: 'var(--text-main)', margin: 0, whiteSpace: 'nowrap' }}>
            Controlli preliminari
          </h4>
          <StatusTag status={status} />
        </div>

        {controls.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <p style={{ fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
              Esiti per ciascun controllo
            </p>
            <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
              {DESCRIZIONE_STATUS[status]}
            </p>

            {/* Convalida button */}
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 8 }}>
              {saveError && (
                <p style={{ fontSize: 14, color: '#e60000', margin: 0, letterSpacing: 1 }}>{saveError}</p>
              )}
              <button
                disabled={!anyChecked || saving}
                onClick={handleConvalidaClick}
                style={{
                  height: 40, minWidth: 160, padding: '8px 12px', borderRadius: 8, border: 'none',
                  cursor: (anyChecked && !saving) ? 'pointer' : 'default',
                  background: (anyChecked && !saving) ? 'var(--blue-main)' : '#d9d9d9',
                  fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
                  letterSpacing: 1, lineHeight: '24px',
                  color: (anyChecked && !saving) ? 'white' : '#a6a6a6',
                }}
              >
                {saving ? 'Salvataggio…' : buttonLabel}
              </button>
            </div>

            <ControlliTable
              controls={displayControls}
              checked={checked}
              onCheckAll={handleCheckAll}
              onCheckRow={handleCheckRow}
            />

            {/* Post-convalida banner */}
            {allDone && (
              <div style={{
                display: 'flex', alignItems: 'center', gap: 12,
                padding: '12px 16px', borderRadius: 8,
                background: '#eaf4ea', border: '1px solid #82c282',
              }}>
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" style={{ flexShrink: 0 }}>
                  <circle cx="12" cy="12" r="9" fill="#eaf4ea" stroke="#2a6b2a" strokeWidth="1.5" />
                  <path d="M7 12L10.5 15.5L17 9" stroke="#2a6b2a" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                <span style={{ fontSize: 16, fontWeight: 400, letterSpacing: 1, lineHeight: '24px', color: '#2a6b2a' }}>
                  Esiti convalidati. La lavorazione della pratica si conclude poiché non tutti i controlli sono stati superati.
                </span>
              </div>
            )}
          </div>
        )}

        {controls.length === 0 && (
          <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
            {DESCRIZIONE_STATUS[status]}
          </p>
        )}
      </div>
    </>
  );
}

function ControlliAmmCard({ prelStatus, ammStatus }) {
  const descrizione = prelStatus === 'non_superati'
    ? 'Controlli automatici non avviabili. È stato rilevato un errore bloccante nei controlli preliminari.'
    : 'I controlli amministrativo-contabili non possono essere avviati fino al completamento dei controlli preliminari.';

  return (
    <div style={{
      background: '#fbfcff', border: '1px solid var(--grey-border)', borderRadius: 8,
      padding: 16, display: 'flex', flexDirection: 'column', gap: 8,
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
        <p style={{ fontSize: 20, fontWeight: 600, letterSpacing: 1, lineHeight: '26px', color: 'var(--text-main)', margin: 0, whiteSpace: 'nowrap' }}>
          Controlli amministrativo-contabili
        </p>
        <StatusTag status={ammStatus} />
      </div>
      <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
        {descrizione}
      </p>
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────

export default function DettaglioPratica({ praticeData, uploadedFiles = [], idPratica, onHome, userName }) {
  const [activeTab, setActiveTab] = useState(TABS[0].label);
  const [controlli, setControlli] = useState({ preliminari: [], amm_contabili: [] });
  const intervalRef = useRef(null);

  function stopPolling() {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }

  async function fetchControlli() {
    if (!idPratica) return;
    try {
      const data = await getPratica(idPratica);
      const prel = data.controlli_preliminari || [];
      const amm  = data.controlli_amm_contabili || [];
      setControlli({ preliminari: prel, amm_contabili: amm });
      // Stop se i preliminari sono tutti terminali E almeno uno è non_superato
      // (gli AMM contabili non partiranno mai), oppure se anche gli AMM sono tutti terminali.
      const prelAllTerminal = allTerminal(prel);
      const prelHasFailure = prel.some(c => c.esito === 'non_superato' || c.esito === 'errore');
      const ammAllTerminal = allTerminal(amm);
      if (prelAllTerminal && (prelHasFailure || ammAllTerminal)) stopPolling();
    } catch {
      // silently retry on next tick
    }
  }

  useEffect(() => {
    if (!idPratica) return;
    fetchControlli();
    intervalRef.current = setInterval(fetchControlli, POLL_INTERVAL);
    return stopPolling;
  }, [idPratica]);

  const prelStatus = deriveControlliStatus(controlli.preliminari);
  const ammStatus = deriveAmmStatus(controlli.amm_contabili);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <header style={{ position: 'sticky', top: 0, zIndex: 100 }}>
        <PreHeader />
        <TopHeader userName={userName} />
        <SubheaderMenu items={['Home', 'Assistenza']} />
      </header>

      {/* Breadcrumb */}
      <div style={{
        height: 60, borderBottom: '1px solid var(--grey-border)',
        display: 'flex', alignItems: 'center', padding: '0 40px', gap: 8,
      }}>
        <button onClick={onHome} style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
          <HomeIcon />
        </button>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--blue-main)' }}>/</span>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--blue-main)' }}>
          Dettaglio pratica
        </span>
      </div>

      <main style={{ padding: '32px 40px 80px', flex: 1, display: 'flex', flexDirection: 'column', gap: 32 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <h2 style={{ fontSize: 32, fontWeight: 600, letterSpacing: 1, lineHeight: '42px', color: 'var(--text-main)', margin: 0 }}>
            Dettaglio pratica
          </h2>
          <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
            Consulta le informazioni relative alla pratica e lo stato di avanzamento dei controlli preliminari e amministrativo-contabili.
            {' '}Se i controlli sono ancora in lavorazione, riprova tra qualche minuto per visualizzarne gli esiti.
          </p>
        </div>

        <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start' }}>
          <TabMenu activeTab={activeTab} onTabChange={setActiveTab} />

          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 40 }}>
            {activeTab === 'Informazioni sulla pratica' && (
              <InfoPraticaCard praticeData={praticeData} uploadedFiles={uploadedFiles} />
            )}
            {activeTab === 'Controlli preliminari' && (
              <ControlliPreliminaryCard
                controls={controlli.preliminari}
                idPratica={idPratica}
                onRefresh={fetchControlli}
                onHome={onHome}
              />
            )}
            {activeTab === 'Controlli amministrativo-contabili' && (
              <ControlliAmmCard prelStatus={prelStatus} ammStatus={ammStatus} />
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
