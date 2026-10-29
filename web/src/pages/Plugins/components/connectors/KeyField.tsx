import { useId, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useCreateUserVaultSecret, useUpdateUserVaultSecret } from '@/hooks/useUserVault';
import { MAX_SECRET_VALUE_LENGTH } from '@/lib/secretNames';
import { formatApiErrorDetail } from '@/pages/ChatAgent/utils/api';
import { SecretValueInput } from '../secrets/SecretValueInput';

/**
 * One key the check named, filled in where the problem is said. Leaving for
 * Secrets to replace it put the fix a page away from the verdict that asked
 * for it and gave no answer on whether the new value worked; here the save
 * writes the stored key (or stores a missing one) and the callout reports the
 * re-check in place.
 */
export function KeyField({
  name,
  stored,
  checks,
  onSaved,
}: {
  name: string;
  /** Whether Secrets holds it already: a replace, not an add. */
  stored: boolean;
  /** Whether a save is followed by a check, which the button then says. */
  checks: boolean;
  onSaved: () => void;
}) {
  const { t } = useTranslation();
  const id = useId();
  const [value, setValue] = useState('');
  const [visible, setVisible] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const create = useCreateUserVaultSecret();
  const update = useUpdateUserVaultSecret();
  const saving = create.isPending || update.isPending;
  const save = async () => {
    if (!value.trim() || saving) return;
    setError(null);
    try {
      if (stored) await update.mutateAsync({ name, body: { value } });
      else await create.mutateAsync({ name, value });
    } catch (err) {
      setError(formatApiErrorDetail(err));
      return;
    }
    setValue('');
    onSaved();
  };
  return (
    <form
      className="flex flex-col gap-1.5"
      onSubmit={(e) => {
        e.preventDefault();
        void save();
      }}
    >
      <label htmlFor={id} style={{ color: 'var(--color-text-secondary)' }}>
        <Trans
          i18nKey={
            stored
              ? 'plugins.connectorsPage.callout.keyNewValue'
              : 'plugins.connectorsPage.callout.keyValue'
          }
          values={{ key: name }}
          components={{ mono: <span className="connector-mono" /> }}
        />
      </label>
      <div className="flex items-center gap-2">
        <SecretValueInput
          id={id}
          value={value}
          onChange={setValue}
          visible={visible}
          onToggleVisible={() => setVisible((v) => !v)}
          labels={{
            show: t('plugins.connectorsPage.callout.keyShow'),
            hide: t('plugins.connectorsPage.callout.keyHide'),
          }}
          maxLength={MAX_SECRET_VALUE_LENGTH}
          data-testid={`detail-key-${name}`}
          wrapperClassName="flex-1 min-w-0"
          className="w-full h-[30px] pl-2.5 pr-8 rounded-lg bg-transparent font-mono text-xs"
          style={{
            color: 'var(--color-text-primary)',
            border: '1px solid var(--color-border-default)',
          }}
        />
        <button
          type="submit"
          className="connector-btn is-primary"
          disabled={saving || !value.trim()}
        >
          {saving && <Loader size={12} className="text-current" />}
          {checks
            ? t('plugins.connectorsPage.callout.keySaveAndCheck')
            : t('plugins.connectorsPage.callout.keySave')}
        </button>
      </div>
      {error && (
        <p role="alert" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}
    </form>
  );
}
