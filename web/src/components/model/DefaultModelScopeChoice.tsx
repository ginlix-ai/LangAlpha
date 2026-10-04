import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Check } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { ApplyDefaultTo } from '@/lib/modelPreferences';
import { namePhrase, type DefaultModelQuestion } from '@/hooks/useDefaultModelChange';

type DefaultModelScopeChoiceProps = DefaultModelQuestion & {
  className?: string;
  /** For a host that swaps the question in for the control that raised it,
   *  so keyboard focus is not left on the page body. */
  autoFocus?: boolean;
};

function ScopeOption({ name, value, checked, label, disabled, autoFocus, onSelect }: {
  name: string;
  value: ApplyDefaultTo;
  checked: boolean;
  label: string;
  disabled: boolean;
  autoFocus?: boolean;
  onSelect: (value: ApplyDefaultTo) => void;
}) {
  // The native input is what takes focus and arrow keys; `rings-within`
  // rings the row on its behalf, since the input itself is visually hidden.
  return (
    <label className={cn(
      'rings-within flex items-center gap-2.5 rounded-md px-2 py-1.5 text-sm',
      disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer hover:bg-(--color-bg-hover)',
    )}>
      <input
        type="radio"
        name={name}
        value={value}
        checked={checked}
        disabled={disabled}
        autoFocus={autoFocus}
        onChange={() => onSelect(value)}
        className="sr-only"
      />
      <span
        aria-hidden="true"
        className="flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-full border"
        style={{ borderColor: checked ? 'var(--color-btn-primary-bg)' : 'var(--color-text-quaternary)' }}
      >
        {checked && <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: 'var(--color-btn-primary-bg)' }} />}
      </span>
      <span className="min-w-0" style={{ color: checked ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}>
        {label}
      </span>
    </label>
  );
}

/**
 * The question a default change raises: whether threads already on the old
 * default move with it. New threads only is preselected because it is the
 * answer that changes nothing the user can already see.
 */
export function DefaultModelScopeChoice({
  models,
  previous,
  saving,
  onConfirm,
  onCancel,
  className,
  autoFocus,
}: DefaultModelScopeChoiceProps) {
  const { t } = useTranslation();
  const id = useId();
  const [applyTo, setApplyTo] = useState<ApplyDefaultTo>('new_threads');
  const [remember, setRemember] = useState(false);
  const named = namePhrase(t, models);
  const replaced = namePhrase(t, previous);

  return (
    <div
      className={cn('flex flex-col gap-2 rounded-md px-3 py-2.5', className)}
      style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-default)' }}
    >
      <div id={`${id}-title`} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
        {t('settings.defaultModelChange.title', { model: named.text, context: named.context })}
      </div>
      <div role="radiogroup" aria-labelledby={`${id}-title`} className="flex flex-col -mx-2">
        <ScopeOption
          name={`${id}-scope`}
          value="new_threads"
          checked={applyTo === 'new_threads'}
          label={t('settings.defaultModelChange.newThreads', { context: named.context })}
          disabled={saving}
          autoFocus={autoFocus}
          onSelect={setApplyTo}
        />
        <ScopeOption
          name={`${id}-scope`}
          value="existing_threads"
          checked={applyTo === 'existing_threads'}
          label={previous.length > 0
            ? t('settings.defaultModelChange.existingThreads', { previous: replaced.text, context: replaced.context })
            : t('settings.defaultModelChange.scopeExistingThreads')}
          disabled={saving}
          onSelect={setApplyTo}
        />
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <label className={cn(
          'rings-within flex items-center gap-2 rounded-md text-xs',
          saving ? 'cursor-not-allowed opacity-50' : 'cursor-pointer',
        )}>
          <input
            type="checkbox"
            checked={remember}
            disabled={saving}
            onChange={(e) => setRemember(e.target.checked)}
            className="sr-only"
          />
          <span
            aria-hidden="true"
            className="flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-[3px] border"
            style={remember
              ? { backgroundColor: 'var(--color-btn-primary-bg)', borderColor: 'var(--color-btn-primary-bg)' }
              : { borderColor: 'var(--color-text-quaternary)' }}
          >
            {remember && <Check className="h-2.5 w-2.5" strokeWidth={3} style={{ color: 'var(--color-btn-primary-text)' }} />}
          </span>
          <span style={{ color: 'var(--color-text-tertiary)' }}>{t('settings.defaultModelChange.remember')}</span>
        </label>
        <div className="flex items-center gap-1.5">
          <button
            type="button"
            onClick={onCancel}
            disabled={saving}
            className="text-xs font-medium rounded-md px-2.5 py-1 hover:bg-(--color-bg-hover) disabled:opacity-50"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('common.cancel')}
          </button>
          <button
            type="button"
            onClick={() => onConfirm(applyTo, remember)}
            disabled={saving}
            className="text-xs font-medium whitespace-nowrap rounded-md px-2.5 py-1 hover:opacity-90 disabled:opacity-50"
            style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
          >
            {t('settings.defaultModelChange.confirm')}
          </button>
        </div>
      </div>
    </div>
  );
}
