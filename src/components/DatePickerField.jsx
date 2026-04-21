import { useState, useRef, useEffect } from 'react';

const MONTHS_IT = [
  'Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno',
  'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre',
];
const DAYS_IT = ['L', 'M', 'M', 'G', 'V', 'S', 'D'];

function buildGrid(year, month) {
  const firstDow = new Date(year, month, 1).getDay();
  const startOffset = firstDow === 0 ? 6 : firstDow - 1; // Monday-first

  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const daysInPrevMonth = new Date(year, month, 0).getDate();

  const cells = [];
  for (let i = startOffset - 1; i >= 0; i--)
    cells.push({ day: daysInPrevMonth - i, inMonth: false });
  for (let d = 1; d <= daysInMonth; d++)
    cells.push({ day: d, inMonth: true });
  let next = 1;
  while (cells.length % 7 !== 0)
    cells.push({ day: next++, inMonth: false });

  const weeks = [];
  for (let i = 0; i < cells.length; i += 7)
    weeks.push(cells.slice(i, i + 7));
  return weeks;
}

function isSameDay(a, b) {
  return a && b && a.getFullYear() === b.getFullYear()
    && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

function formatDate(d) {
  if (!d) return '';
  return [
    String(d.getDate()).padStart(2, '0'),
    String(d.getMonth() + 1).padStart(2, '0'),
    d.getFullYear(),
  ].join('/');
}

function CalendarIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
         stroke="var(--grey-input)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
         style={{ flexShrink: 0 }}>
      <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
      <line x1="16" y1="2" x2="16" y2="6" />
      <line x1="8" y1="2" x2="8" y2="6" />
      <line x1="3" y1="10" x2="21" y2="10" />
    </svg>
  );
}

function ChevronLeft() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"
         stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="10 4 6 8 10 12" />
    </svg>
  );
}

function ChevronRight() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none"
         stroke="var(--blue-main)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="6 4 10 8 6 12" />
    </svg>
  );
}

function CalendarPopup({ value, onConfirm, onCancel }) {
  const today = new Date();
  const initial = value || today;
  const [viewYear, setViewYear] = useState(initial.getFullYear());
  const [viewMonth, setViewMonth] = useState(initial.getMonth());
  const [pending, setPending] = useState(value || null);

  function prevMonth() {
    if (viewMonth === 0) { setViewMonth(11); setViewYear(y => y - 1); }
    else setViewMonth(m => m - 1);
  }
  function nextMonth() {
    if (viewMonth === 11) { setViewMonth(0); setViewYear(y => y + 1); }
    else setViewMonth(m => m + 1);
  }

  const weeks = buildGrid(viewYear, viewMonth);

  return (
    <div style={{
      background: 'var(--white)',
      borderRadius: 8,
      boxShadow: '0px 4px 24px 0px rgba(0,0,0,0.1), 0px 0px 1px 0px rgba(0,0,0,0.7)',
      width: 360,
      overflow: 'hidden',
    }}>
      {/* Header */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        height: 48,
        padding: '12px 8px',
        borderBottom: '1px solid var(--grey-border)',
      }}>
        <button onClick={prevMonth} style={{
          width: 32, height: 32, flexShrink: 0,
          border: '1px solid var(--grey-border)',
          borderRadius: 8,
          background: 'none',
          cursor: 'pointer',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <ChevronLeft />
        </button>
        <span style={{
          flex: 1, textAlign: 'right',
          fontSize: 18, fontWeight: 500, letterSpacing: 1,
          color: 'var(--blue-main)', cursor: 'default',
        }}>
          {MONTHS_IT[viewMonth]}
        </span>
        <span style={{
          flex: 1,
          fontSize: 18, fontWeight: 500, letterSpacing: 1,
          color: 'var(--blue-main)', cursor: 'default',
        }}>
          {viewYear}
        </span>
        <button onClick={nextMonth} style={{
          width: 32, height: 32, flexShrink: 0,
          border: '1px solid var(--grey-border)',
          borderRadius: 8,
          background: 'none',
          cursor: 'pointer',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <ChevronRight />
        </button>
      </div>

      {/* Calendar grid */}
      <div style={{ padding: '4px 12px' }}>
        {/* Day-of-week headers */}
        <div style={{ display: 'flex' }}>
          {DAYS_IT.map((d, i) => (
            <div key={i} style={{
              width: 48, height: 48, flexShrink: 0,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: 16, fontWeight: 500, letterSpacing: 1,
              color: 'var(--text-main)',
            }}>
              {d}
            </div>
          ))}
        </div>

        {/* Weeks */}
        {weeks.map((week, wi) => (
          <div key={wi} style={{ display: 'flex' }}>
            {week.map((cell, ci) => {
              const cellDate = new Date(viewYear, viewMonth + (cell.inMonth ? 0 : ci < 3 ? -1 : 1), cell.day);
              const isToday = isSameDay(cellDate, today);
              const isSelected = isSameDay(cellDate, pending);

              let bgColor = 'transparent';
              let textColor = cell.inMonth ? 'var(--text-main)' : '#bfbfbf';
              let fontWeight = cell.inMonth ? 300 : 300;
              let border = 'none';
              let borderRadius = 8;

              if (isSelected) {
                bgColor = 'var(--blue-main)';
                textColor = 'var(--white)';
                fontWeight = 500;
              } else if (isToday && cell.inMonth) {
                border = '1px solid #80bcff';
                fontWeight = 500;
              }

              return (
                <div key={ci} style={{ width: 48, height: 48, flexShrink: 0, padding: 4 }}>
                  <button
                    onClick={() => cell.inMonth && setPending(cellDate)}
                    disabled={!cell.inMonth}
                    style={{
                      width: 40, height: 40, borderRadius,
                      background: bgColor, border,
                      cursor: cell.inMonth ? 'pointer' : 'default',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      fontSize: 16, fontWeight, letterSpacing: 1,
                      color: textColor,
                      fontFamily: 'var(--font)',
                    }}
                  >
                    {cell.day}
                  </button>
                </div>
              );
            })}
          </div>
        ))}
      </div>

      {/* Footer */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        padding: '16px 16px 16px 24px',
        borderTop: '1px solid var(--grey-border)',
      }}>
        <button
          onClick={() => setPending(null)}
          disabled={!pending}
          style={{
            flex: 1, background: 'none', border: 'none',
            cursor: pending ? 'pointer' : 'default',
            fontSize: 18, fontWeight: 500, letterSpacing: 1,
            color: pending ? 'var(--blue-main)' : '#bfbfbf',
            fontFamily: 'var(--font)',
            textAlign: 'left',
          }}
        >
          Reset
        </button>
        <button onClick={onCancel} style={{
          height: 40, padding: '8px 12px',
          border: '1px solid var(--blue-main)',
          borderRadius: 8, background: 'none',
          cursor: 'pointer',
          fontSize: 18, fontWeight: 500, letterSpacing: 1,
          color: 'var(--blue-main)',
          fontFamily: 'var(--font)',
          whiteSpace: 'nowrap',
        }}>
          Annulla
        </button>
        <button onClick={() => onConfirm(pending)} style={{
          height: 40, padding: '8px 12px',
          border: 'none', borderRadius: 8,
          background: 'var(--blue-main)',
          cursor: 'pointer',
          fontSize: 18, fontWeight: 500, letterSpacing: 1,
          color: 'var(--white)',
          fontFamily: 'var(--font)',
          whiteSpace: 'nowrap',
        }}>
          Conferma
        </button>
      </div>
    </div>
  );
}

export default function DatePickerField({ id, label, value, onChange, style }) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef(null);

  useEffect(() => {
    function handleClickOutside(e) {
      if (containerRef.current && !containerRef.current.contains(e.target))
        setOpen(false);
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const filled = !!value;
  const borderColor = open ? 'var(--blue-main)' : filled ? 'var(--grey-text)' : 'var(--grey-input)';

  return (
    <div ref={containerRef} style={{ display: 'flex', flexDirection: 'column', gap: 4, position: 'relative', ...style }}>
      <label htmlFor={id} style={{
        fontSize: 16, fontWeight: 300, letterSpacing: 1,
        color: 'var(--text-main)', lineHeight: '24px',
      }}>
        {label}
      </label>

      <div
        style={{
          position: 'relative', display: 'flex',
          alignItems: 'center',
          height: 48,
          border: `1px solid ${borderColor}`,
          borderRadius: 'var(--radius-input)',
          background: 'var(--white)',
          padding: '0 12px 0 16px',
          cursor: 'pointer',
          transition: 'border-color 0.15s',
          gap: 8,
        }}
        onClick={() => setOpen(o => !o)}
        role="button"
        aria-haspopup="true"
        aria-expanded={open}
      >
        <span style={{
          flex: 1,
          fontSize: 18, fontWeight: 300, letterSpacing: 1,
          color: filled ? 'var(--text-main)' : 'var(--grey-text)',
          lineHeight: '28px',
          whiteSpace: 'nowrap',
        }}>
          {filled ? formatDate(value) : 'gg/mm/aaaa'}
        </span>
        <CalendarIcon />
      </div>

      {open && (
        <div style={{
          position: 'absolute',
          top: 'calc(100% + 4px)',
          left: 0,
          zIndex: 200,
        }}>
          <CalendarPopup
            value={value}
            onConfirm={(date) => { onChange(date); setOpen(false); }}
            onCancel={() => setOpen(false)}
          />
        </div>
      )}
    </div>
  );
}
