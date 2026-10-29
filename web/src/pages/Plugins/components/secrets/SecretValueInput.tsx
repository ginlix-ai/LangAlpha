import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Eye, EyeOff } from 'lucide-react';
import { cn } from '@/lib/utils';

/**
 * A key's value field, wherever one is typed: masked until the eye beside it
 * is pressed, and kept out of autofill and spellcheck, since what it holds is
 * a credential. Each surface keeps its own look for the field itself.
 */
export function SecretValueInput({
  value,
  onChange,
  visible,
  onToggleVisible,
  labels,
  wrapperClassName,
  ...input
}: Omit<React.ComponentPropsWithRef<'input'>, 'type' | 'value' | 'onChange'> & {
  value: string;
  onChange: (value: string) => void;
  visible: boolean;
  onToggleVisible: () => void;
  /** What the eye says in each state; the vault's own words by default. */
  labels?: { show: string; hide: string };
  wrapperClassName?: string;
}) {
  const { t } = useTranslation();
  const label = visible
    ? (labels?.hide ?? t('vault.hideValue'))
    : (labels?.show ?? t('vault.revealValue'));
  return (
    <div className={cn('relative', wrapperClassName)}>
      <input
        {...input}
        type={visible ? 'text' : 'password'}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        autoComplete="off"
        spellCheck={false}
      />
      <button
        type="button"
        onClick={onToggleVisible}
        aria-label={label}
        aria-pressed={visible}
        aria-controls={input.id}
        className="absolute right-1 top-1/2 -translate-y-1/2 p-1 rounded transition-colors hover:bg-foreground/10"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {visible ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
      </button>
    </div>
  );
}
