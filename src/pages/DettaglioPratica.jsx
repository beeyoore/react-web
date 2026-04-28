import { useEffect, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import PreHeader from '../components/PreHeader';
import TopHeader from '../components/TopHeader';
import SubheaderMenu from '../components/SubheaderMenu';
import { getPratica } from '../api/getPratica';
import { convalidaControlli } from '../api/convalidaControlli';
import { modificaControllo } from '../api/modificaControllo';

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

function WarningTriangleIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <path d="M8 2.5L14 13.5H2L8 2.5Z" stroke="#c25700" strokeWidth="1.5" strokeLinejoin="round" fill="#fff8eb" />
      <line x1="8" y1="7" x2="8" y2="10" stroke="#c25700" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="8" cy="12" r="0.75" fill="#c25700" />
    </svg>
  );
}

function InfoTooltipIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}>
      <circle cx="9" cy="9" r="8" stroke="#737373" strokeWidth="1.5" />
      <line x1="9" y1="8" x2="9" y2="13" stroke="#737373" strokeWidth="1.5" strokeLinecap="round" />
      <circle cx="9" cy="6" r="0.75" fill="#737373" />
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

function StatusTag({ status, labelOverride }) {
  const style = TAG_STYLES[status] || TAG_STYLES.non_avviati;
  return (
    <span style={{
      display: 'inline-block', padding: '4px 8px', borderRadius: 8,
      fontSize: 12, fontWeight: 500, letterSpacing: 1, textTransform: 'uppercase',
      whiteSpace: 'nowrap', flexShrink: 0, ...style,
    }}>
      {labelOverride || TAG_LABELS[status]}
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
const AMM_COL_WIDTHS = { checkbox: 48, esito: 166, convalidato: 136, modificato: 136, azioni: 170 };

const HEADER_STYLE = {
  background: 'white', borderBottom: '1px solid #bbc5d7',
  height: 64, display: 'flex', alignItems: 'center', padding: 12,
  fontSize: 16, fontWeight: 500, letterSpacing: 1, color: 'var(--text-main)',
};

function TableRow({ ctrl, index, checked, onCheck, onDettaglio }) {
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
        <button
          onClick={() => onDettaglio(ctrl)}
          style={{
            background: 'none', border: 'none', cursor: 'pointer', padding: 0,
            fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
            letterSpacing: 1, color: 'var(--blue-main)', whiteSpace: 'nowrap',
          }}
        >
          Vai al dettaglio
        </button>
      </div>
    </>
  );
}

function ControlliTable({ controls, checked, onCheckAll, onCheckRow, onDettaglioRow }) {
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
            onDettaglio={onDettaglioRow}
          />
        </div>
      ))}
    </div>
  );
}

function AmmTableRow({ ctrl, index, checked, onCheck, onDettaglio }) {
  const isEven = index % 2 === 1;
  const cellBg = isEven ? '#eef1f8' : '#fbfcff';
  const cellStyle = {
    background: cellBg, borderBottom: '1px solid #bbc5d7',
    height: 64, display: 'flex', alignItems: 'center', padding: 12,
  };
  const isDisabled = !!ctrl.convalidato;
  return (
    <>
      <div style={{ ...cellStyle, width: AMM_COL_WIDTHS.checkbox, justifyContent: 'center', flexShrink: 0 }}>
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
      <div style={{ ...cellStyle, flex: '1 0 0', minWidth: 0 }}>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)' }}>
          {ctrl.descrizione}
        </span>
      </div>
      <div style={{ ...cellStyle, width: AMM_COL_WIDTHS.esito, flexShrink: 0 }}>
        <EsitoChip esito={ctrl.esito} />
      </div>
      <div style={{ ...cellStyle, width: AMM_COL_WIDTHS.convalidato, flexShrink: 0 }}>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)' }}>
          {ctrl.convalidato ? 'Sì' : 'No'}
        </span>
      </div>
      <div style={{ ...cellStyle, width: AMM_COL_WIDTHS.modificato, flexShrink: 0 }}>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)' }}>
          {ctrl.modificato ? 'Sì' : 'No'}
        </span>
      </div>
      <div style={{ ...cellStyle, width: AMM_COL_WIDTHS.azioni, flexShrink: 0 }}>
        <button
          onClick={() => onDettaglio(ctrl)}
          style={{
            background: 'none', border: 'none', cursor: 'pointer', padding: 0,
            fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
            letterSpacing: 1, color: 'var(--blue-main)', whiteSpace: 'nowrap',
          }}
        >
          Vai al dettaglio
        </button>
      </div>
    </>
  );
}

function AmmControlliTable({ controls, checked, onCheckAll, onCheckRow, onDettaglioRow }) {
  const allChecked = controls.length > 0 && controls.every((_, i) => checked[i]);
  return (
    <div style={{ border: '1px solid #bbc5d7', borderRadius: 8, overflow: 'hidden', width: '100%' }}>
      <div style={{ display: 'flex' }}>
        <div style={{ ...HEADER_STYLE, width: AMM_COL_WIDTHS.checkbox, justifyContent: 'center', flexShrink: 0 }}>
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
        <div style={{ ...HEADER_STYLE, width: AMM_COL_WIDTHS.esito, flexShrink: 0 }}>Esito</div>
        <div style={{ ...HEADER_STYLE, width: AMM_COL_WIDTHS.convalidato, flexShrink: 0 }}>Convalidato</div>
        <div style={{ ...HEADER_STYLE, width: AMM_COL_WIDTHS.modificato, flexShrink: 0 }}>Modificato</div>
        <div style={{ ...HEADER_STYLE, width: AMM_COL_WIDTHS.azioni, flexShrink: 0 }}>Azioni</div>
      </div>
      {controls.map((ctrl, i) => (
        <div key={ctrl.id} style={{ display: 'flex' }}>
          <AmmTableRow
            ctrl={ctrl}
            index={i}
            checked={!!checked[i]}
            onCheck={(val) => onCheckRow(i, val)}
            onDettaglio={onDettaglioRow}
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

function ConvalidaAlertModal({ onAnnulla, onConferma, allChecked, tipoLabel = 'controlli preliminari' }) {
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
          {allChecked
            ? <>Sei sicuro di voler convalidare<br />i {tipoLabel}?</>
            : <>Sei sicuro di voler convalidare<br />i {tipoLabel} selezionati?</>
          }
        </h3>
        <p style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', margin: 0, textAlign: 'center' }}>
          Se convalidi, i {tipoLabel} non saranno più modificabili.
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

function ConvalidaSuccessModal({ remaining, onHome, onRimani, tipoLabel = 'controlli preliminari', messaggioCompletato }) {
  const isParziale = remaining > 0;
  const defaultMsg = messaggioCompletato || 'Non verranno effettuati ulteriori controlli, in quanto quelli preliminari non sono stati superati.';
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
          {isParziale
            ? <>I {tipoLabel} selezionati<br />sono stati convalidati con successo!</>
            : <>I {tipoLabel} sono stati<br />convalidati con successo!</>
          }
        </h3>
        <p style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', margin: 0, textAlign: 'center' }}>
          {isParziale
            ? `Sono rimasti ${remaining} ${remaining === 1 ? 'controllo' : 'controlli'} in attesa di convalida.`
            : defaultMsg
          }
        </p>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, marginTop: 8, width: '100%', justifyContent: 'center' }}>
          {!isParziale && (
            <button
              onClick={onHome}
              style={{
                flex: 1, minWidth: 0, minHeight: 44, padding: '10px 16px', borderRadius: 8, border: '2px solid var(--blue-main)',
                background: 'white', cursor: 'pointer', fontFamily: 'var(--font)',
                fontSize: 16, fontWeight: 500, letterSpacing: 1, lineHeight: '22px', color: 'var(--blue-main)',
                whiteSpace: 'normal', textAlign: 'center',
              }}
            >
              Torna alla Homepage
            </button>
          )}
          <button
            onClick={onRimani}
            style={{
              flex: isParziale ? 0 : 1, minWidth: 0, minHeight: 44, padding: '10px 24px', borderRadius: 8, border: 'none',
              background: 'var(--blue-main)', cursor: 'pointer', fontFamily: 'var(--font)',
              fontSize: 16, fontWeight: 500, letterSpacing: 1, lineHeight: '22px', color: 'white',
              whiteSpace: 'normal', textAlign: 'center',
            }}
          >
            {isParziale ? 'Chiudi' : 'Rimani sulla pratica'}
          </button>
        </div>
      </div>
    </ModalOverlay>
  );
}

const MAX_MOTIVAZIONE = 12000;

function DettaglioControlloModal({ ctrl, onClose, canModify = false, onSave }) {
  const [isEditing, setIsEditing] = useState(false);
  const [nuovoEsito, setNuovoEsito] = useState('superato');
  const [nuovaMotivazione, setNuovaMotivazione] = useState('');
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState(null);

  const canSave = nuovaMotivazione.trim().length > 0 && !saving;

  async function handleSave() {
    if (!canSave || !onSave) return;
    setSaving(true);
    setSaveError(null);
    try {
      await onSave(ctrl, nuovoEsito, nuovaMotivazione.trim());
      onClose();
    } catch (err) {
      setSaveError(err.message || 'Errore durante il salvataggio. Riprova.');
      setSaving(false);
    }
  }

  return (
    <ModalOverlay>
      <div style={{
        background: 'white', borderRadius: 12, padding: '32px 40px',
        maxWidth: 580, width: '90%', maxHeight: '90vh', overflowY: 'auto',
        display: 'flex', flexDirection: 'column', gap: 24,
        boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
      }}>
        {/* Title */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <span style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)' }}>
            Dettaglio controllo:
          </span>
          <h3 style={{ fontSize: 22, fontWeight: 600, letterSpacing: 1, lineHeight: '30px', color: 'var(--text-main)', margin: 0 }}>
            {ctrl.nome}
          </h3>
          <p style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', margin: '8px 0 0' }}>
            Consulta l&apos;esito del controllo e se necessario, procedi alla modifica.
          </p>
        </div>

        {/* Esito controllo */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <p style={{ fontSize: 16, fontWeight: 400, letterSpacing: 1, color: 'var(--text-main)', margin: 0 }}>
            Esito controllo
          </p>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <EsitoChip esito={ctrl.esito} />
            {canModify && (
              <button
                onClick={() => !isEditing && setIsEditing(true)}
                style={{
                  background: 'none', border: 'none', padding: 0,
                  cursor: isEditing ? 'default' : 'pointer',
                  display: 'flex', alignItems: 'center', gap: 4,
                  fontFamily: 'var(--font)', fontSize: 16, fontWeight: 400, letterSpacing: 1,
                  color: isEditing ? '#bdbdbd' : 'var(--blue-main)',
                }}
              >
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true">
                  <path d="M11 2L14 5L5 14H2V11L11 2Z"
                    stroke={isEditing ? '#bdbdbd' : 'var(--blue-main)'}
                    strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                Modifica
              </button>
            )}
          </div>
        </div>

        {/* Motivazione */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <p style={{ fontSize: 16, fontWeight: 400, letterSpacing: 1, color: 'var(--text-main)', margin: 0 }}>
            Motivazione dell&apos;esito
          </p>
          <div style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)' }}>
            {ctrl.motivazione
              ? <ReactMarkdown components={{
                  p:      ({ children }) => <p style={{ margin: '0 0 8px 0' }}>{children}</p>,
                  ul:     ({ children }) => <ul style={{ margin: '4px 0', paddingLeft: 20 }}>{children}</ul>,
                  ol:     ({ children }) => <ol style={{ margin: '4px 0', paddingLeft: 20 }}>{children}</ol>,
                  li:     ({ children }) => <li style={{ margin: '2px 0' }}>{children}</li>,
                  strong: ({ children }) => <strong style={{ fontWeight: 600 }}>{children}</strong>,
                }}>{ctrl.motivazione}</ReactMarkdown>
              : '—'}
          </div>
        </div>

        {/* Edit form */}
        {isEditing && (
          <div style={{
            border: '1px solid var(--grey-border)', borderRadius: 8,
            padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: 16,
          }}>
            <p style={{ fontSize: 16, fontWeight: 600, letterSpacing: 1, color: 'var(--text-main)', margin: 0 }}>
              Modifica esito e fornisci una motivazione
            </p>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ fontSize: 16, fontWeight: 400, letterSpacing: 1, color: 'var(--text-main)' }}>Esito</span>
                <InfoTooltipIcon />
              </div>
              <div style={{ display: 'flex', gap: 24 }}>
                {['superato', 'non_superato'].map(val => (
                  <label key={val} style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
                    <input
                      type="radio"
                      name="nuovoEsito"
                      value={val}
                      checked={nuovoEsito === val}
                      onChange={() => setNuovoEsito(val)}
                      style={{ accentColor: 'var(--blue-main)', width: 18, height: 18, cursor: 'pointer' }}
                    />
                    <span style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, color: 'var(--text-main)' }}>
                      {val === 'superato' ? 'Superato' : 'Non superato'}
                    </span>
                  </label>
                ))}
              </div>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <span style={{ fontSize: 16, fontWeight: 400, letterSpacing: 1, color: 'var(--text-main)' }}>
                Motivazione della modifica dell&apos;esito
              </span>
              <div style={{ position: 'relative' }}>
                <textarea
                  value={nuovaMotivazione}
                  onChange={e => setNuovaMotivazione(e.target.value.slice(0, MAX_MOTIVAZIONE))}
                  placeholder="Es. I dati corrispondono correttamente."
                  rows={4}
                  style={{
                    width: '100%', boxSizing: 'border-box',
                    padding: '10px 12px 28px', borderRadius: 6,
                    border: '1px solid var(--grey-border)', resize: 'vertical',
                    fontFamily: 'var(--font)', fontSize: 16, fontWeight: 300,
                    letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)',
                    outline: 'none',
                  }}
                />
                <span style={{
                  position: 'absolute', bottom: 8, right: 10,
                  fontSize: 12, fontWeight: 300, letterSpacing: 1, color: '#737373',
                  pointerEvents: 'none',
                }}>
                  {nuovaMotivazione.length}/{MAX_MOTIVAZIONE}
                </span>
              </div>
            </div>

            {saveError && (
              <p style={{ fontSize: 14, color: '#e60000', margin: 0, letterSpacing: 1 }}>{saveError}</p>
            )}
          </div>
        )}

        {/* Footer */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
          <button
            onClick={onClose}
            style={{
              height: 44, padding: '0 20px', borderRadius: 8,
              border: '2px solid var(--blue-main)', background: 'white',
              cursor: 'pointer', fontFamily: 'var(--font)',
              fontSize: 16, fontWeight: 500, letterSpacing: 1, color: 'var(--blue-main)',
            }}
          >
            Chiudi
          </button>
          {canModify && (
            <button
              disabled={!canSave}
              onClick={handleSave}
              style={{
                height: 44, padding: '0 20px', borderRadius: 8, border: 'none',
                cursor: canSave ? 'pointer' : 'default',
                background: canSave ? 'var(--blue-main)' : '#d9d9d9',
                fontFamily: 'var(--font)', fontSize: 16, fontWeight: 500,
                letterSpacing: 1, color: canSave ? 'white' : '#a6a6a6',
              }}
            >
              {saving ? 'Salvataggio…' : 'Salva modifiche'}
            </button>
          )}
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
  const [remainingAfterVal, setRemainingAfterVal] = useState(0);
  const [dettaglioCtrl, setDettaglioCtrl] = useState(null);

  // ids of controls already convalidated (derived from server data)
  const convalidatiSet = new Set(
    controls.map((c, i) => (c.convalidato ? i : null)).filter(i => i !== null)
  );

  // Solo gli indici selezionabili (non ancora convalidati)
  const selectableIndices = controls.map((_, i) => i).filter(i => !convalidatiSet.has(i));
  // Solo quelli effettivamente spuntati E non convalidati
  const checkedIndices = Object.entries(checked)
    .filter(([k, v]) => v && !convalidatiSet.has(Number(k)))
    .map(([k]) => Number(k));
  const anyChecked = checkedIndices.length > 0;
  const allChecked = selectableIndices.length > 0 && checkedIndices.length === selectableIndices.length;
  const buttonLabel = allChecked ? 'Convalida tutti' : `Convalida (${checkedIndices.length})`;
  const allDone = controls.length > 0 && controls.every(c => c.convalidato);

  function handleCheckAll(val) {
    const next = {};
    // Seleziona/deseleziona solo i controlli non ancora convalidati
    selectableIndices.forEach(i => { next[i] = val; });
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
      const notYetValidated = controls.filter(c => !c.convalidato).length;
      setRemainingAfterVal(Math.max(0, notYetValidated - checkedIndices.length));
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

  function handleOpenDettaglio(ctrl) {
    setDettaglioCtrl(ctrl);
  }

  function handleCloseDettaglio() {
    setDettaglioCtrl(null);
  }

  // displayControls: use server data directly (convalidato already reflects DB state after refresh)
  const displayControls = controls;

  return (
    <>
      {showAlert && (
        <ConvalidaAlertModal onAnnulla={handleAnnulla} onConferma={handleConferma} allChecked={allChecked} />
      )}
      {showSuccess && (
        <ConvalidaSuccessModal remaining={remainingAfterVal} onHome={handleHome} onRimani={handleRimani} />
      )}
      {dettaglioCtrl && (
        <DettaglioControlloModal
          ctrl={dettaglioCtrl}
          onClose={handleCloseDettaglio}
        />
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
              onDettaglioRow={handleOpenDettaglio}
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

const CATEGORY_ORDER = [
  'Corrispondenza dei dati anagrafici',
  'Corrispondenza dei dati professionali',
  'Riconoscibilità dei servizi e delle assenze',
  "Verifica dell'anzianità maturata",
  'Ulteriori controlli',
];

const DESC_AMM_STATUS = {
  non_avviati:    'I controlli amministrativo-contabili non possono essere avviati fino al completamento dei controlli preliminari.',
  in_lavorazione: 'I controlli amministrativo-contabili sono in corso di elaborazione.',
  superati:       'Tutti i controlli amministrativo-contabili sono stati superati.',
  non_superati:   'I controlli amministrativo-contabili sono stati completati e i relativi esiti sono disponibili per la consultazione. Verifica i risultati proposti dal sistema e procedi con la convalida dell\'esito di tutti i controlli primari. Una volta completati si abiliterà la modifica dei controlli secondari non bloccanti.',
};

function ControlliAmmCard({ controls, idPratica, onRefresh, onHome, prelStatus }) {
  const status = deriveAmmStatus(controls);

  const CATEGORY_ORDER_SET = new Set(CATEGORY_ORDER);
  const primari   = controls.filter(c => CATEGORY_ORDER_SET.has(c.macro_categoria || ''));
  const secondari = controls.filter(c => !CATEGORY_ORDER_SET.has(c.macro_categoria || ''));

  const availableCats = new Set(primari.map(c => c.macro_categoria || 'Generale'));
  const categories = [
    ...CATEGORY_ORDER.filter(c => availableCats.has(c)),
    ...[...availableCats].filter(c => !CATEGORY_ORDER.includes(c)),
  ];
  const [activeCategory, setActiveCategory] = useState('');

  const displayCategory = categories.includes(activeCategory) ? activeCategory : (categories[0] || '');

  const filteredPrimari = primari.filter(c => (c.macro_categoria || 'Generale') === displayCategory);

  const [checkedPrimari,   setCheckedPrimari]   = useState({});
  const [checkedSecondari, setCheckedSecondari] = useState({});
  const [showAlertPrimari,   setShowAlertPrimari]   = useState(false);
  const [showAlertSecondari, setShowAlertSecondari] = useState(false);
  const [showSuccess,  setShowSuccess]  = useState(false);
  const [saving,       setSaving]       = useState(false);
  const [saveError,    setSaveError]    = useState(null);
  const [remaining,    setRemaining]    = useState(0);
  const [pendingIds,   setPendingIds]   = useState([]);
  const [pendingTipo,  setPendingTipo]  = useState('');
  const [dettaglioCtrl, setDettaglioCtrl] = useState(null);

  const primariConvalidatiSet   = new Set(filteredPrimari.map((c, i) => c.convalidato ? i : null).filter(i => i !== null));
  const secondariConvalidatiSet = new Set(secondari.map((c, i) => c.convalidato ? i : null).filter(i => i !== null));

  const primariSelectable   = filteredPrimari.map((_, i) => i).filter(i => !primariConvalidatiSet.has(i));
  const secondariSelectable = secondari.map((_, i) => i).filter(i => !secondariConvalidatiSet.has(i));

  const primariChecked   = Object.entries(checkedPrimari).filter(([k, v]) => v && !primariConvalidatiSet.has(Number(k))).map(([k]) => Number(k));
  const secondariChecked = Object.entries(checkedSecondari).filter(([k, v]) => v && !secondariConvalidatiSet.has(Number(k))).map(([k]) => Number(k));

  const anyPrimariChecked   = primariChecked.length > 0;
  const anySecondariChecked = secondariChecked.length > 0;
  const allPrimariChecked   = primariSelectable.length > 0 && primariChecked.length === primariSelectable.length;
  const allSecondariChecked = secondariSelectable.length > 0 && secondariChecked.length === secondariSelectable.length;

  const allPrimariConvalidati = primari.length > 0 && primari.every(c => c.convalidato);
  const allSecondariDone      = secondari.length > 0 && secondari.every(c => c.convalidato);

  const primariLabel   = allPrimariChecked   ? 'Convalida tutti' : `Convalida (${primariChecked.length})`;
  const secondariLabel = allSecondariChecked ? 'Convalida tutti' : `Convalida (${secondariChecked.length})`;

  function handleCheckAllPrimari(val) {
    const next = {};
    primariSelectable.forEach(i => { next[i] = val; });
    setCheckedPrimari(next);
  }

  function handleCheckAllSecondari(val) {
    const next = {};
    secondariSelectable.forEach(i => { next[i] = val; });
    setCheckedSecondari(next);
  }

  function openConvalida(ids, tipo) {
    setSaveError(null);
    setPendingIds(ids);
    setPendingTipo(tipo);
    if (tipo === 'primari') setShowAlertPrimari(true);
    else setShowAlertSecondari(true);
  }

  async function executeConvalida() {
    setSaving(true);
    setSaveError(null);
    const pool = pendingTipo === 'primari' ? primari : secondari;
    setRemaining(Math.max(0, pool.filter(c => !c.convalidato).length - pendingIds.length));
    try {
      await convalidaControlli(idPratica, 'amm_contabile', pendingIds);
      if (pendingTipo === 'primari') setCheckedPrimari({});
      else setCheckedSecondari({});
      if (onRefresh) await onRefresh();
      setShowSuccess(true);
    } catch (err) {
      setSaveError(err.message || 'Errore durante la convalida. Riprova.');
    } finally {
      setSaving(false);
    }
  }

  const ammLabelOverride = status === 'non_superati' ? 'NON SUPERATI · OSSERVAZIONE' : undefined;

  return (
    <>
      {showAlertPrimari && (
        <ConvalidaAlertModal
          tipoLabel="controlli primari"
          allChecked={allPrimariChecked}
          onAnnulla={() => setShowAlertPrimari(false)}
          onConferma={() => { setShowAlertPrimari(false); executeConvalida(); }}
        />
      )}
      {showAlertSecondari && (
        <ConvalidaAlertModal
          tipoLabel="controlli secondari"
          allChecked={allSecondariChecked}
          onAnnulla={() => setShowAlertSecondari(false)}
          onConferma={() => { setShowAlertSecondari(false); executeConvalida(); }}
        />
      )}
      {showSuccess && (
        <ConvalidaSuccessModal
          remaining={remaining}
          tipoLabel={`controlli ${pendingTipo}`}
          messaggioCompletato="La convalida dei controlli è stata completata."
          onHome={() => { setShowSuccess(false); if (onHome) onHome(); }}
          onRimani={() => setShowSuccess(false)}
        />
      )}
      {dettaglioCtrl && (
        <DettaglioControlloModal
          ctrl={dettaglioCtrl}
          onClose={() => setDettaglioCtrl(null)}
          canModify={true}
          onSave={async (ctrl, nuovoEsito, nuovaMotivazione) => {
            await modificaControllo(idPratica, ctrl.id, nuovoEsito, nuovaMotivazione);
            if (onRefresh) await onRefresh();
          }}
        />
      )}

      <div style={{
        background: '#fbfcff', border: '1px solid var(--grey-border)', borderRadius: 8,
        padding: 16, display: 'flex', flexDirection: 'column', gap: 24,
      }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
          <h4 style={{ fontSize: 26, fontWeight: 600, letterSpacing: 1, lineHeight: '34px', color: 'var(--text-main)', margin: 0, whiteSpace: 'nowrap' }}>
            Controlli amministrativo-contabili
          </h4>
          <StatusTag status={status} labelOverride={ammLabelOverride} />
        </div>

        <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
          {prelStatus === 'non_superati' && controls.length === 0
            ? 'Controlli automatici non avviabili. È stato rilevato un errore bloccante nei controlli preliminari.'
            : DESC_AMM_STATUS[status]}
        </p>

        {controls.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
            {/* Category tabs */}
            {categories.length > 0 && (
              <div style={{ display: 'flex', borderBottom: '2px solid #bbc5d7', overflowX: 'auto' }}>
                {categories.map(cat => {
                  const isActive = cat === displayCategory;
                  const catControls = primari.filter(c => (c.macro_categoria || 'Generale') === cat);
                  const catDone = catControls.length > 0 && catControls.every(c => c.convalidato);
                  return (
                    <button
                      key={cat}
                      onClick={() => { setActiveCategory(cat); setCheckedPrimari({}); }}
                      style={{
                        padding: '10px 16px', background: 'none', border: 'none',
                        borderBottom: isActive ? '2px solid var(--blue-main)' : '2px solid transparent',
                        marginBottom: -2,
                        cursor: 'pointer', fontFamily: 'var(--font)', fontSize: 16,
                        fontWeight: isActive ? 600 : 300, color: 'var(--text-main)',
                        display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap',
                      }}
                    >
                      {catDone
                        ? <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true" style={{ flexShrink: 0 }}><circle cx="8" cy="8" r="7" fill="#eaf4ea" stroke="#2a6b2a" strokeWidth="1.5"/><path d="M4.5 8L6.5 10L11.5 5.5" stroke="#2a6b2a" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
                        : <WarningTriangleIcon />
                      }
                      {cat}
                    </button>
                  );
                })}
              </div>
            )}

            {/* Convalida button – primary controls */}
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 8 }}>
              {saveError && (
                <p style={{ fontSize: 14, color: '#e60000', margin: 0, letterSpacing: 1 }}>{saveError}</p>
              )}
              <button
                disabled={!anyPrimariChecked || saving}
                onClick={() => openConvalida(primariChecked.map(i => filteredPrimari[i].id), 'primari')}
                style={{
                  height: 40, minWidth: 160, padding: '8px 12px', borderRadius: 8, border: 'none',
                  cursor: (anyPrimariChecked && !saving) ? 'pointer' : 'default',
                  background: (anyPrimariChecked && !saving) ? 'var(--blue-main)' : '#d9d9d9',
                  fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
                  letterSpacing: 1, lineHeight: '24px',
                  color: (anyPrimariChecked && !saving) ? 'white' : '#a6a6a6',
                }}
              >
                {saving && pendingTipo === 'primari' ? 'Salvataggio…' : primariLabel}
              </button>
            </div>

            {/* Primary controls table */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <p style={{ fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
                  Controlli primari per categoria
                </p>
                <InfoTooltipIcon />
              </div>
              <AmmControlliTable
                controls={filteredPrimari}
                checked={checkedPrimari}
                onCheckAll={handleCheckAllPrimari}
                onCheckRow={(i, val) => setCheckedPrimari(prev => ({ ...prev, [i]: val }))}
                onDettaglioRow={setDettaglioCtrl}
              />
              {allPrimariConvalidati && (
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
                    Tutti i controlli primari sono stati convalidati.
                  </span>
                </div>
              )}
            </div>

            {/* Secondary controls */}
            {secondari.length > 0 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <p style={{ fontSize: 18, fontWeight: 600, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
                      Controlli secondari
                    </p>
                    <InfoTooltipIcon />
                  </div>
                  <button
                    disabled={!anySecondariChecked || saving || !allPrimariConvalidati}
                    onClick={() => openConvalida(secondariChecked.map(i => secondari[i].id), 'secondari')}
                    style={{
                      height: 40, minWidth: 160, padding: '8px 12px', borderRadius: 8, border: 'none',
                      cursor: (anySecondariChecked && !saving && allPrimariConvalidati) ? 'pointer' : 'default',
                      background: (anySecondariChecked && !saving && allPrimariConvalidati) ? 'var(--blue-main)' : '#d9d9d9',
                      fontFamily: 'var(--font)', fontSize: 18, fontWeight: 500,
                      letterSpacing: 1, lineHeight: '24px',
                      color: (anySecondariChecked && !saving && allPrimariConvalidati) ? 'white' : '#a6a6a6',
                    }}
                  >
                    {saving && pendingTipo === 'secondari' ? 'Salvataggio…' : secondariLabel}
                  </button>
                </div>
                <p style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', margin: 0 }}>
                  Convalida i controlli secondari solo dopo aver completato la convalida di quelli primari.
                </p>
                <AmmControlliTable
                  controls={secondari}
                  checked={checkedSecondari}
                  onCheckAll={handleCheckAllSecondari}
                  onCheckRow={(i, val) => setCheckedSecondari(prev => ({ ...prev, [i]: val }))}
                  onDettaglioRow={setDettaglioCtrl}
                />
                {allSecondariDone && (
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
                      Tutti i controlli secondari sono stati convalidati.
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </>
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
      // Ferma il polling se:
      // - i preliminari sono tutti terminali E almeno uno non superato (gli AMM non partiranno)
      // - oppure se l'aggregator ha segnato checklist_elaborata=true (tutti gli AMM scritti)
      const prelAllTerminal = allTerminal(prel);
      const prelHasFailure  = prel.some(c => c.esito === 'non_superato' || c.esito === 'errore');
      const checklistElaborata = data.checklist_elaborata === true;
      if (prelAllTerminal && (prelHasFailure || checklistElaborata)) stopPolling();
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
              <ControlliAmmCard
                controls={controlli.amm_contabili}
                idPratica={idPratica}
                onRefresh={fetchControlli}
                onHome={onHome}
                prelStatus={prelStatus}
              />
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
