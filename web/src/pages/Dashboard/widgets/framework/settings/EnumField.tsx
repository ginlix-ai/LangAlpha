import { Select } from '@/components/ui/select';
import '@/pages/Dashboard/components/dashForms.css';

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
      <span className="term-label dash-form-label">
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
        <span className="dash-form-help">
          {helper}
        </span>
      )}
    </label>
  );
}
