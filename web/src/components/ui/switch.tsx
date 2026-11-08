import React from 'react';
import { cn } from '@/lib/utils';

interface ToggleSwitchProps {
  checked: boolean;
  onChange: () => void;
  disabled?: boolean;
  /** Extra classes layered onto the pill (e.g. `mt-0.5` for top alignment). */
  className?: string;
  /** Accessible name — the visual label beside the pill isn't associated with it. */
  ariaLabel?: string;
  /** Lets a `<label htmlFor>` beside the pill toggle it too. */
  id?: string;
  /** The id of the line that describes what the switch does. */
  ariaDescribedBy?: string;
}

/**
 * Hand-rolled pill toggle used across Settings (voice input, experiments).
 * Deliberately a plain button rather than the Radix switch — it preserves the
 * exact markup and theme-var styling both call sites already shipped.
 */
export function ToggleSwitch({ checked, onChange, disabled, className, ariaLabel, id, ariaDescribedBy }: ToggleSwitchProps): React.ReactElement {
  return (
    <button
      id={id}
      type="button"
      role="switch"
      data-slot="switch"
      aria-checked={checked}
      aria-label={ariaLabel}
      aria-describedby={ariaDescribedBy}
      onClick={onChange}
      disabled={disabled}
      // Off, the open card styles draw it as an open box with an inked thumb,
      // as every other open control is: an elevated fill on a white page was
      // a near-white track under a white thumb. On, they draw the fill alone,
      // its edge as clear as their fill is.
      className={cn(
        'tap-target relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-[var(--cs-radius-small,calc(infinity*1px))] border-2 transition-colors',
        className,
      )}
      style={{
        backgroundColor: checked ? 'var(--color-accent-primary)' : 'var(--color-shell-fill, var(--color-bg-elevated))',
        borderColor: checked
          ? 'var(--color-shell-fill, var(--color-border-muted))'
          : 'var(--color-shell-edge, var(--color-border-muted))',
      }}
    >
      <span
        data-slot="switch-thumb"
        className={cn(
          'pointer-events-none inline-block h-4 w-4 transform rounded-[var(--cs-radius-mark,calc(infinity*1px))] shadow-sm ring-0 transition',
          checked ? 'bg-white' : 'bg-[var(--color-shell-edge-hover,#fff)]',
        )}
        style={{
          transform: checked ? 'translateX(16px)' : 'translateX(0)',
        }}
      />
    </button>
  );
}
