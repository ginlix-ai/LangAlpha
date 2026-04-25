import { useState, type KeyboardEvent } from 'react';
import { X } from 'lucide-react';
import { Input } from '@/components/ui/input';

interface Props {
  label: string;
  value: string[];
  onChange: (next: string[]) => void;
  placeholder?: string;
  helper?: string;
  max?: number;
}

/**
 * Multi-symbol picker — chips with add-by-Enter and remove-by-X.
 * Symbols are uppercased and de-duplicated on add.
 */
export function SymbolListField({
  label,
  value,
  onChange,
  placeholder,
  helper,
  max = 50,
}: Props) {
  const [draft, setDraft] = useState('');

  const add = () => {
    const sym = draft.trim().toUpperCase();
    if (!sym) return;
    if (value.includes(sym)) {
      setDraft('');
      return;
    }
    if (value.length >= max) return;
    onChange([...value, sym]);
    setDraft('');
  };

  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === ',') {
      e.preventDefault();
      add();
    } else if (e.key === 'Backspace' && draft === '' && value.length > 0) {
      onChange(value.slice(0, -1));
    }
  };

  const remove = (sym: string) => onChange(value.filter((s) => s !== sym));

  return (
    <div>
      <span className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
        {label}
      </span>
      <div
        className="flex flex-wrap gap-1.5 p-1.5 rounded border"
        style={{
          backgroundColor: 'var(--color-bg-card)',
          borderColor: 'var(--color-border-default)',
        }}
      >
        {value.map((sym) => (
          <span
            key={sym}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] dashboard-mono"
            style={{
              backgroundColor: 'var(--color-bg-subtle)',
              color: 'var(--color-text-primary)',
            }}
          >
            {sym}
            <button
              type="button"
              onClick={() => remove(sym)}
              aria-label={`Remove ${sym}`}
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <X size={10} />
            </button>
          </span>
        ))}
        <Input
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={onKey}
          onBlur={add}
          placeholder={value.length === 0 ? placeholder ?? 'Add symbols (Enter)' : ''}
          className="flex-1 min-w-[100px] border-0 !p-0 !h-6 text-xs bg-transparent shadow-none focus-visible:ring-0"
          style={{
            color: 'var(--color-text-primary)',
            textTransform: 'uppercase',
          }}
        />
      </div>
      {helper && (
        <span className="text-[11px] mt-1 block" style={{ color: 'var(--color-text-tertiary)' }}>
          {helper}
        </span>
      )}
      {value.length >= max && (
        <span className="text-[11px] mt-1 block" style={{ color: 'var(--color-text-tertiary)' }}>
          Max {max} symbols.
        </span>
      )}
    </div>
  );
}
