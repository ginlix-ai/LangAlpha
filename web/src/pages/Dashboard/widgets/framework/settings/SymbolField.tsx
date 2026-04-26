import { useTranslation } from 'react-i18next';
import { Input } from '@/components/ui/input';

interface Props {
  label: string;
  value: string;
  onChange: (next: string) => void;
  placeholder?: string;
  helper?: string;
}

/**
 * Single-symbol picker. Plain uppercase input — autocomplete from a curated
 * list could come later; for now we accept any user-entered symbol because
 * TradingView resolves them server-side anyway.
 */
export function SymbolField({ label, value, onChange, placeholder, helper }: Props) {
  const { t } = useTranslation();
  return (
    <label className="block">
      <span className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
        {label}
      </span>
      <Input
        value={value}
        onChange={(e) => onChange(e.target.value.toUpperCase())}
        onBlur={(e) => {
          const trimmed = e.target.value.trim();
          if (trimmed !== e.target.value) onChange(trimmed);
        }}
        placeholder={placeholder ?? t('dashboard.widgets.settings.symbolFieldPlaceholder')}
        className="border"
        style={{
          backgroundColor: 'var(--color-bg-card)',
          borderColor: 'var(--color-border-default)',
          color: 'var(--color-text-primary)',
          textTransform: 'uppercase',
        }}
      />
      {helper && (
        <span className="text-[11px] mt-1 block" style={{ color: 'var(--color-text-tertiary)' }}>
          {helper}
        </span>
      )}
    </label>
  );
}
