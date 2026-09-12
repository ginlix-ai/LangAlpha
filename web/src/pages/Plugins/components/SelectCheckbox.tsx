import { Check, Minus } from 'lucide-react';
import type { CheckState } from './toolSelection';

/**
 * The selection box on a tool row and on the header that stands for a whole
 * group. A button rather than an `<input>`: the mixed state is a real answer
 * here (some of the group is picked) and `aria-checked="mixed"` says so, while
 * a native checkbox can only carry it through an imperative property no test
 * or screen reader reads off the markup.
 *
 * No focus styling of its own, deliberately. The ring is the sheet's
 * (`styles/tokens.css`, `@layer focus-defaults`), which already holds itself
 * off while the record says a pointer placed the focus.
 */
export function SelectCheckbox({
  state,
  label,
  disabled = false,
  onToggle,
}: {
  state: CheckState;
  /** Names what this box selects, in the tool's or the group's own words. */
  label: string;
  disabled?: boolean;
  onToggle: () => void;
}) {
  const on = state !== 'none';
  return (
    <button
      type="button"
      role="checkbox"
      aria-checked={state === 'some' ? 'mixed' : state === 'all'}
      aria-label={label}
      disabled={disabled}
      onClick={onToggle}
      className="flex-shrink-0 inline-flex h-3.5 w-3.5 items-center justify-center rounded-sm p-0 transition-colors disabled:opacity-40"
      style={{
        border: on ? 'none' : '1px solid var(--color-border-muted)',
        backgroundColor: on ? 'var(--color-accent-primary)' : 'transparent',
      }}
    >
      {state === 'all' && (
        <Check className="h-2.5 w-2.5" style={{ color: 'var(--color-text-on-accent)' }} />
      )}
      {state === 'some' && (
        <Minus className="h-2.5 w-2.5" style={{ color: 'var(--color-text-on-accent)' }} />
      )}
    </button>
  );
}
