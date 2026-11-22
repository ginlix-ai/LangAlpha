import { useId, type ReactNode } from 'react';
import { ToggleSwitch } from '@/components/ui/switch';

/**
 * A label and a line of help beside a switch. The switch is left out when the
 * viewer can only read the setting, so the help line has to carry its state.
 */
export function SwitchRow({ id, label, description, checked, onChange, disabled, showSwitch = true, divided }: {
  id: string;
  label: ReactNode;
  description: ReactNode;
  checked: boolean;
  onChange: () => void;
  disabled?: boolean;
  showSwitch?: boolean;
  /** A rule above the row, for a setting that follows another block. */
  divided?: boolean;
}) {
  const helpId = useId();
  return (
    <div
      className={divided ? 'flex items-start justify-between gap-3 border-t pt-4' : 'flex items-start justify-between gap-3'}
      style={divided ? { borderColor: 'var(--color-border-muted)' } : undefined}
    >
      <label htmlFor={id} className="flex min-w-0 flex-col gap-0.5 text-sm">
        <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>{label}</span>
        <span id={helpId} style={{ color: 'var(--color-text-secondary)' }}>{description}</span>
      </label>
      {showSwitch && (
        <ToggleSwitch
          id={id}
          checked={checked}
          disabled={disabled}
          ariaDescribedBy={helpId}
          onChange={onChange}
          className="mt-0.5"
        />
      )}
    </div>
  );
}
