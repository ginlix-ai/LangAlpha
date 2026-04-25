import { Select } from '@/components/ui/select';

interface Option {
  value: string;
  label: string;
}

interface Props {
  label: string;
  value: string;
  onChange: (next: string) => void;
  options: Option[];
  helper?: string;
}

export function EnumField({ label, value, onChange, options, helper }: Props) {
  return (
    <label className="block">
      <span className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
        {label}
      </span>
      <Select value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </Select>
      {helper && (
        <span className="text-[11px] mt-1 block" style={{ color: 'var(--color-text-tertiary)' }}>
          {helper}
        </span>
      )}
    </label>
  );
}
