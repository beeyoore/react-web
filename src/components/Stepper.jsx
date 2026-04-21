function StepBubble({ number, active, completed }) {
  if (completed) {
    return (
      <div style={{
        width: 24, height: 24, borderRadius: 1000, flexShrink: 0,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        background: '#468041',
      }}>
        <svg width="13" height="10" viewBox="0 0 13 10" fill="none">
          <path d="M1.5 5L5 8.5L11.5 1.5" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </div>
    );
  }
  return (
    <div style={{
      width: 24, height: 24,
      borderRadius: 1000,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      fontSize: 12,
      fontWeight: 300,
      letterSpacing: 1,
      lineHeight: '16px',
      flexShrink: 0,
      ...(active
        ? { background: 'var(--blue-main)', color: 'var(--white)' }
        : { background: 'var(--grey-bg)', border: '1px solid var(--grey-border)', color: 'var(--grey-text)' }
      ),
    }}>
      {number}
    </div>
  );
}

function StepItem({ number, label, sublabel, active, completed, showDivider = true }) {
  return (
    <div style={{ display: 'flex', alignItems: 'flex-start', gap: 16, flexShrink: 0 }}>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, width: 24, flexShrink: 0 }}>
        <StepBubble number={number} active={active} completed={completed} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 2, whiteSpace: 'nowrap' }}>
        <span style={{
          fontSize: 16,
          fontWeight: 500,
          letterSpacing: 1,
          lineHeight: '24px',
          ...(active || completed
            ? { color: 'var(--blue-main)', textDecoration: 'underline' }
            : { color: 'var(--text-main)', opacity: 0.65 }
          ),
        }}>
          {label}
        </span>
        <span style={{ fontSize: 12, fontWeight: 300, letterSpacing: 1, lineHeight: '16px', color: 'var(--grey-text)' }}>
          {sublabel}
        </span>
      </div>

      {showDivider && (
        <div style={{ display: 'flex', alignItems: 'center', alignSelf: 'stretch', flexShrink: 0 }}>
          <div style={{ height: 1, width: 40, background: 'var(--grey-border)' }} />
        </div>
      )}
    </div>
  );
}

export default function Stepper({ currentStep = 1, totalSteps = 3, title = 'Apertura pratica', steps = [] }) {
  return (
    <section style={{
      background: 'var(--grey-bg)',
      borderBottom: '1px solid var(--grey-border)',
      padding: '0 var(--margin-xl) 16px',
    }}>
      {/* Top row */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        height: 64,
        padding: '24px var(--margin-l)',
      }}>
        <span style={{ fontSize: 20, fontWeight: 600, letterSpacing: 1 }}>{title}</span>
        <span style={{ fontSize: 18, fontWeight: 300, letterSpacing: 1 }}>
          Step {currentStep} di {totalSteps}
        </span>
      </div>

      {/* Step bar */}
      <div style={{ display: 'flex', alignItems: 'center', padding: '0 var(--margin-l)' }}>
        {steps.map((step, i) => (
          <StepItem
            key={step.label}
            number={i + 1}
            label={step.label}
            sublabel={step.sublabel}
            active={i + 1 === currentStep}
            completed={i + 1 < currentStep}
            showDivider={i < steps.length - 1}
          />
        ))}
      </div>
    </section>
  );
}
