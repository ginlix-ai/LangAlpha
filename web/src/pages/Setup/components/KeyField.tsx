import { useId, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ExternalLink, Lock } from 'lucide-react';
import { testSetupKey, type SetupKey } from '@/api/setup';
import type { TestResult } from '@/api/model';
import { ApiKeyInput } from '@/components/model/ApiKeyInput';
import { Button } from '@/components/ui/button';
import { useRemoveSetupKey, useSaveErrorCopy } from '../hooks/useSetupDrafts';
import { keyTestErrorCopy } from '../utils/keyTestCopy';
import { KEY_SIGNUP_URLS, vendorName } from '../utils/vendors';

interface KeyFieldProps {
  setupKey: SetupKey;
  /** Defaults to the vendor's name. */
  label?: string;
  description?: ReactNode;
  draft: string;
  onDraft: (value: string) => void;
  /** Said under the field: why this key is needed right now. */
  error?: string | null;
}

/**
 * One vendor's key, in the app's key field (`ApiKeyInput`): empty, it takes a
 * key and can test it before it is saved. Saved, it shows masked and may be
 * replaced or removed. Set in the server's environment, it can only be read
 * about here, since that value wins.
 *
 * Keyed by `keyFieldKey`, so a save that goes through closes the replace field.
 */
export function KeyField({ setupKey, label, description, draft, onDraft, error }: KeyFieldProps) {
  const { t } = useTranslation();
  const inputId = useId();
  const [replacing, setReplacing] = useState(false);
  const saveCopy = useSaveErrorCopy();
  const remove = useRemoveSetupKey();

  const name = label ?? vendorName(setupKey.provider, t);
  const signup = Object.hasOwn(KEY_SIGNUP_URLS, setupKey.provider) ? KEY_SIGNUP_URLS[setupKey.provider] : undefined;
  const editing = setupKey.state === 'unset' || replacing;

  const test = async (_provider: string, value: string): Promise<TestResult> => {
    const result = await testSetupKey(setupKey.name, value.trim());
    return result.ok ? { success: true } : { success: false, error: keyTestErrorCopy(result.error, t) };
  };

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3">
        <label htmlFor={editing ? inputId : undefined} className="text-sm font-medium text-(--color-text-primary)">
          {name}
        </label>
        {signup && setupKey.state !== 'environment' && (
          <a
            href={signup}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex shrink-0 items-center gap-1 text-xs text-(--color-text-secondary) hover:text-(--color-text-primary)"
          >
            {t('setup.key.getKey')}
            <ExternalLink aria-hidden="true" className="h-3 w-3" />
          </a>
        )}
      </div>
      {description && <p className="text-xs leading-snug text-(--color-text-tertiary)">{description}</p>}

      {setupKey.state === 'environment' ? (
        <p className="mt-0.5 flex items-center gap-1.5 text-[0.8125rem] text-(--color-text-secondary)">
          <Lock aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
          {t('setup.key.environment')}
        </p>
      ) : editing ? (
        <div className="mt-0.5 flex items-start gap-2">
          <div className="min-w-0 flex-1">
            <ApiKeyInput
              id={inputId}
              provider={setupKey.provider}
              value={draft}
              onChange={onDraft}
              maskedKey={setupKey.masked}
              onTest={test}
              optional
            />
          </div>
          {replacing && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-10 shrink-0 text-(--color-text-secondary)"
              onClick={() => {
                onDraft('');
                setReplacing(false);
              }}
            >
              {t('common.cancel')}
            </Button>
          )}
        </div>
      ) : (
        <div className="mt-0.5 flex items-center gap-2">
          <span
            className="min-w-0 flex-1 truncate font-mono text-[0.8125rem] text-(--color-text-secondary)"
            style={{ fontVariantNumeric: 'tabular-nums' }}
          >
            {setupKey.masked}
          </span>
          <Button type="button" variant="outline" size="sm" className="shrink-0" onClick={() => setReplacing(true)}>
            {t('setup.key.replace')}
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="shrink-0 text-(--color-text-secondary)"
            disabled={remove.isPending}
            onClick={() => remove.mutate(setupKey.name)}
          >
            {t('setup.key.remove')}
          </Button>
        </div>
      )}

      <div role="status" aria-live="polite" className="empty:hidden">
        {remove.isError && <p className="text-xs text-(--color-loss)">{saveCopy(remove.error)}</p>}
        {error && <p className="text-xs text-(--color-loss)">{error}</p>}
      </div>
    </div>
  );
}
