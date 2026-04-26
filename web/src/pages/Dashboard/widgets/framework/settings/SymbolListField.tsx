import { useState, type KeyboardEvent, type ClipboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { Input } from '@/components/ui/input';

// Split incoming text on whitespace, comma, semicolon, or newline so a paste
// of `NVDA, AAPL MSFT;TSLA` becomes 4 chips instead of 1 bad symbol that the
// schema later rewrites to the catch default.
const TOKEN_SEPARATORS = /[\s,;]+/;

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
  const { t } = useTranslation();
  const [draft, setDraft] = useState('');

  const atCap = value.length >= max;

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

  // Paste handler: split on commas/whitespace and add each token. Falls back
  // to default Input behavior if the paste contains no separators (single
  // symbol) so the user can still edit-by-paste mid-input if they want.
  const onPaste = (e: ClipboardEvent<HTMLInputElement>) => {
    const raw = e.clipboardData.getData('text');
    if (!raw || !TOKEN_SEPARATORS.test(raw)) return;
    e.preventDefault();
    const tokens = raw
      .split(TOKEN_SEPARATORS)
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    if (tokens.length === 0) return;
    const next = [...value];
    const seen = new Set(next);
    for (const t of tokens) {
      if (next.length >= max) break;
      if (seen.has(t)) continue;
      seen.add(t);
      next.push(t);
    }
    if (next.length !== value.length) onChange(next);
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

  // Click-outside without Enter shouldn't drop the half-typed symbol — but
  // also shouldn't blindly commit. Commit only when there's a non-whitespace
  // draft AND we're under the cap. The schema layer (Zod regex on consumer
  // widgets) enforces the symbol grammar at sanitize-on-load time.
  const onBlur = () => {
    if (atCap) return;
    if (!draft.trim()) return;
    add();
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
              aria-label={t('dashboard.widgets.settings.symbolListRemove', { symbol: sym })}
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
          onPaste={onPaste}
          onBlur={onBlur}
          disabled={atCap}
          placeholder={
            atCap
              ? t('dashboard.widgets.settings.symbolListAtCap', { max })
              : value.length === 0
                ? placeholder ?? t('dashboard.widgets.settings.symbolListPlaceholder')
                : ''
          }
          className="flex-1 min-w-[100px] border-0 !p-0 !h-6 text-xs bg-transparent shadow-none focus-visible:ring-0 disabled:opacity-50 disabled:cursor-not-allowed"
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
      {atCap && (
        <span className="text-[11px] mt-1 block" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('dashboard.widgets.settings.symbolListHelperAtCap', { max })}
        </span>
      )}
    </div>
  );
}
