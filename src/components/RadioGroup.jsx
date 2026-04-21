import { useState } from 'react';

function RadioButton({ id, name, label, checked, onChange }) {
  return (
    <label
      htmlFor={id}
      style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', flexShrink: 0 }}
    >
      <div
        role="radio"
        aria-checked={checked}
        onClick={() => onChange(label)}
        style={{
          width: 24, height: 24,
          borderRadius: 1000,
          border: '1px solid var(--blue-main)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          cursor: 'pointer',
          flexShrink: 0,
          padding: checked ? 4 : 0,
        }}
      >
        {checked && (
          <div style={{ width: 16, height: 16, borderRadius: 1000, background: 'var(--blue-main)' }} />
        )}
      </div>
      <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1, lineHeight: '28px', color: 'var(--text-main)', whiteSpace: 'nowrap' }}>
        {label}
      </span>
      <input id={id} name={name} type="radio" checked={checked} onChange={() => onChange(label)} style={{ display: 'none' }} />
    </label>
  );
}

export default function RadioGroup({ label, options = [], defaultValue, style }) {
  const [selected, setSelected] = useState(defaultValue ?? options[0]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4, ...style }}>
      <span style={{ fontSize: 16, fontWeight: 300, letterSpacing: 1, lineHeight: '24px', color: 'var(--text-main)', whiteSpace: 'pre' }}>
        {label}
      </span>
      <div style={{ display: 'flex', alignItems: 'center', gap: 24, height: 48 }}>
        {options.map((opt) => (
          <RadioButton
            key={opt}
            id={`radio-${label}-${opt}`}
            name={label}
            label={opt}
            checked={selected === opt}
            onChange={setSelected}
          />
        ))}
      </div>
    </div>
  );
}
