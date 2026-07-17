import React from 'react';
import { cn } from '@/lib/utils';

interface ToggleSwitchProps {
  checked: boolean;
  onChange: () => void;
  disabled?: boolean;
  /** Extra classes layered onto the pill (e.g. `mt-0.5` for top alignment). */
  className?: string;
}

/**
 * Hand-rolled pill toggle used across Settings (voice input, experiments).
 * Deliberately a plain button rather than the Radix switch — it preserves the
 * exact markup and theme-var styling both call sites already shipped.
 */
export function ToggleSwitch({ checked, onChange, disabled, className }: ToggleSwitchProps): React.ReactElement {
  return (
    <button
      type="button"
      onClick={onChange}
      disabled={disabled}
      className={cn(
        'relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none',
        className,
      )}
      style={{
        backgroundColor: checked ? 'var(--color-accent-primary)' : 'var(--color-bg-elevated)',
        borderColor: 'var(--color-border-muted)',
      }}
    >
      <span
        className="pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out"
        style={{
          transform: checked ? 'translateX(16px)' : 'translateX(0)',
        }}
      />
    </button>
  );
}
