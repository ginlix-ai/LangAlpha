/**
 * A radio option and a checkbox in the app's own drawn marks.
 *
 * The native input stays, visually hidden, so it keeps the keyboard (arrow
 * keys within a radio group, Space on a checkbox) and takes its accessible
 * name from the label around it. `rings-within` rings the row on its behalf,
 * which only works while the input is the label's direct child. The row's
 * layout and surface are the caller's; the cursor and the disabled fade are
 * decided here, so every unavailable choice reads the same.
 */
import type { CSSProperties, ReactNode } from 'react';
import { Check } from 'lucide-react';
import { cn } from '@/lib/utils';

interface ChoiceProps {
  checked: boolean;
  disabled?: boolean;
  /** For a host that swaps the question in for the control that raised it,
   *  so keyboard focus is not left on the page body. */
  autoFocus?: boolean;
  /** The row's layout and surface: alignment, gap, padding, radius, fill. */
  className?: string;
  style?: CSSProperties;
  /** Where the mark sits, e.g. `mt-0.5` to meet the first line of a
   *  top-aligned row. */
  markClassName?: string;
  /** For a row whose text holds more than its name, such as a description. */
  'aria-labelledby'?: string;
  'aria-describedby'?: string;
  children: ReactNode;
}

function rowClassName(disabled: boolean | undefined, className: string | undefined): string {
  return cn('rings-within flex', disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer', className);
}

export function RadioChoice<V extends string>({
  name,
  value,
  onSelect,
  checked,
  disabled,
  autoFocus,
  className,
  style,
  markClassName,
  'aria-labelledby': labelledBy,
  'aria-describedby': describedBy,
  children,
}: ChoiceProps & {
  name: string;
  value: V;
  onSelect: (value: V) => void;
}) {
  return (
    <label className={rowClassName(disabled, className)} style={style}>
      <input
        type="radio"
        name={name}
        value={value}
        checked={checked}
        disabled={disabled}
        autoFocus={autoFocus}
        aria-labelledby={labelledBy}
        aria-describedby={describedBy}
        onChange={() => onSelect(value)}
        className="sr-only"
      />
      <span
        aria-hidden="true"
        className={cn('flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-full border', markClassName)}
        style={{ borderColor: checked ? 'var(--color-btn-primary-bg)' : 'var(--color-text-quaternary)' }}
      >
        {checked && <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: 'var(--color-btn-primary-bg)' }} />}
      </span>
      {children}
    </label>
  );
}

/** The box grows with the text beside it: `sm` sits with `text-xs`, `md`
 *  with `text-sm`. */
const CHECKBOX_SIZE = {
  sm: { box: 'h-3.5 w-3.5', check: 'h-2.5 w-2.5' },
  md: { box: 'h-4 w-4', check: 'h-3 w-3' },
} as const;

export function CheckboxChoice({
  onCheckedChange,
  size = 'sm',
  checked,
  disabled,
  autoFocus,
  className,
  style,
  markClassName,
  'aria-labelledby': labelledBy,
  'aria-describedby': describedBy,
  children,
}: ChoiceProps & {
  onCheckedChange: (checked: boolean) => void;
  size?: keyof typeof CHECKBOX_SIZE;
}) {
  const dims = CHECKBOX_SIZE[size];
  return (
    <label className={rowClassName(disabled, className)} style={style}>
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        autoFocus={autoFocus}
        aria-labelledby={labelledBy}
        aria-describedby={describedBy}
        onChange={(e) => onCheckedChange(e.target.checked)}
        className="sr-only"
      />
      <span
        aria-hidden="true"
        className={cn('flex shrink-0 items-center justify-center rounded-[3px] border', dims.box, markClassName)}
        style={checked
          ? { backgroundColor: 'var(--color-btn-primary-bg)', borderColor: 'var(--color-btn-primary-bg)' }
          : { borderColor: 'var(--color-text-quaternary)' }}
      >
        {checked && <Check className={dims.check} strokeWidth={3} style={{ color: 'var(--color-btn-primary-text)' }} />}
      </span>
      {children}
    </label>
  );
}
