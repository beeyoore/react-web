import { useState } from 'react';

function InfoIcon() {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
         stroke="var(--grey-input)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
         style={{ cursor: 'pointer', flexShrink: 0 }} title="Informazioni">
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="16" x2="12" y2="12" />
      <line x1="12" y1="8" x2="12.01" y2="8" />
    </svg>
  );
}

function CalendarIcon({ filled }) {
  return (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
         stroke={filled ? 'var(--grey-text)' : 'var(--grey-input)'}
         strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
         style={{ position: 'absolute', right: 12, pointerEvents: 'none' }}>
      <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
      <line x1="16" y1="2" x2="16" y2="6" />
      <line x1="8" y1="2" x2="8" y2="6" />
      <line x1="3" y1="10" x2="21" y2="10" />
    </svg>
  );
}

export default function FormField({
  id,
  label,
  placeholder,
  defaultValue = '',
  type = 'text',
  showInfo = false,
  showCalendar = false,
  error,
  onChange,
  style,
}) {
  const [value, setValue] = useState(defaultValue);
  const [focused, setFocused] = useState(false);

  const filled = value.length > 0;

  const borderColor = error
    ? 'var(--red)'
    : focused
      ? 'var(--blue-main)'
      : filled
        ? 'var(--grey-text)'
        : 'var(--grey-input)';

  const textColor = filled ? 'var(--text-main)' : 'var(--grey-text)';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, ...style }}>
      <label htmlFor={id} style={{
        display: 'flex',
        alignItems: 'center',
        gap: 4,
        fontSize: 16,
        fontWeight: 300,
        letterSpacing: 1,
        color: 'var(--text-main)',
        lineHeight: '24px',
      }}>
        {label}
        {showInfo && <InfoIcon />}
      </label>

      <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
        <input
          id={id}
          type={type}
          placeholder={placeholder}
          value={value}
          onChange={e => { setValue(e.target.value); onChange?.(e.target.value); }}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          aria-invalid={!!error}
          aria-describedby={error ? `${id}-error` : undefined}
          style={{
            height: 48,
            border: `1px solid ${borderColor}`,
            borderRadius: 'var(--radius-input)',
            background: 'var(--white)',
            padding: `0 ${showCalendar ? 44 : 16}px 0 16px`,
            fontFamily: 'var(--font)',
            fontSize: 18,
            fontWeight: 300,
            color: textColor,
            letterSpacing: 1,
            width: '100%',
            outline: 'none',
            transition: 'border-color 0.15s',
          }}
        />
        {showCalendar && <CalendarIcon filled={filled} />}
      </div>

      {error && (
        <p id={`${id}-error`} role="alert" style={{
          fontSize: 12,
          fontWeight: 300,
          letterSpacing: 1,
          lineHeight: '16px',
          color: 'var(--red)',
          whiteSpace: 'pre-wrap',
        }}>
          {error}
        </p>
      )}
    </div>
  );
}
