import { useTranslation } from 'react-i18next';
import { Select } from '@/components/ui/select';
import { cn } from '@/lib/utils';
import { EFFORT_LABELS, effortOptions } from '@/lib/modelTuning';

export interface CompactOption {
  value: string;
  label: string;
  disabled?: boolean;
}

/**
 * The settings tables' small select: an empty first option that names what an
 * untouched control resolves to, then the choices. `fill` stretches it to its
 * container, for a side column rather than a table cell.
 */
export function CompactSelect({
  value,
  onChange,
  ariaLabel,
  defaultLabel,
  options,
  disabled,
  fill = false,
}: {
  value: string;
  onChange: (v: string) => void;
  ariaLabel: string;
  defaultLabel: string;
  options: ReadonlyArray<CompactOption>;
  disabled?: boolean;
  fill?: boolean;
}) {
  return (
    <Select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      aria-label={ariaLabel}
      disabled={disabled}
      className={cn(fill ? 'w-full min-w-0' : 'min-w-30')}
      // className lands on the wrapper, so the compact type has to come through
      // style — the select itself hardcodes text-sm.
      style={{ fontSize: '0.75rem', paddingTop: '0.25rem', paddingBottom: '0.25rem' }}
    >
      <option value="">{defaultLabel}</option>
      {options.map((o) => (
        <option key={o.value} value={o.value} disabled={o.disabled}>
          {o.label}
        </option>
      ))}
    </Select>
  );
}

/**
 * A reasoning-effort pick over the levels a model offers, weakest first. A
 * stored value the ladder no longer offers is still listed, or the control
 * would show a setting with no way to clear it.
 */
export function EffortSelect({
  value,
  onChange,
  ariaLabel,
  defaultLabel,
  efforts,
  disabled,
  fill = false,
}: {
  /** The stored level, '' for none. */
  value: string;
  onChange: (v: string) => void;
  ariaLabel: string;
  defaultLabel: string;
  /** The levels to offer. */
  efforts: readonly string[];
  disabled?: boolean;
  fill?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <CompactSelect
      value={value}
      onChange={onChange}
      ariaLabel={ariaLabel}
      defaultLabel={defaultLabel}
      options={effortOptions(efforts, value).map((lv) => ({ value: lv, label: t(EFFORT_LABELS[lv]) }))}
      disabled={disabled}
      fill={fill}
    />
  );
}
