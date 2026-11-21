import { useId } from 'react';
import { useTranslation } from 'react-i18next';

import { ToggleSwitch } from '@/components/ui/switch';
import type { Computer } from '@/types/api';

import { useSetBrowserSignins } from '../hooks/useComputerActions';

/**
 * Whether this computer's browser keeps sign-ins between its starts. A person
 * signs in from the Browser tab, and every thread on the computer then browses
 * as them. Every save starts the browser signed out of everything, so signing
 * out is the same save again.
 */
export function BrowserSigninsSetting({ computer }: { computer: Computer }) {
  const { t } = useTranslation();
  const hintId = useId();
  const mutation = useSetBrowserSignins();
  // Kept unless turned off, as the server reads it.
  const kept = computer.config?.browser_signins !== false;
  const busy = mutation.isPending && mutation.variables?.computerId === computer.computer_id;
  const disabled = busy || computer.status === 'deleted';
  const save = (keep: boolean) => mutation.mutate({ computerId: computer.computer_id, keep });
  return (
    <section className="computer-details-group">
      <dl className="computer-card-spec mt-0">
        <div>
          <dt>{t('computer.browserSignins.label', 'Keep sign-ins')}</dt>
          <dd>
            <ToggleSwitch
              checked={kept}
              onChange={() => save(!kept)}
              disabled={disabled}
              ariaLabel={t('computer.browserSignins.label', 'Keep sign-ins')}
              ariaDescribedBy={hintId}
            />
          </dd>
        </div>
      </dl>
      <p id={hintId} className="mt-1.5 text-[12px]" style={{ color: 'var(--color-text-secondary)' }}>
        {kept
          ? t(
              'computer.browserSignins.onHint',
              'Sites you sign in to in the Browser tab stay signed in, for every thread on this computer. Turning this off signs out of all of them.',
            )
          : t(
              'computer.browserSignins.offHint',
              'The browser forgets every site when it closes. Turn this on to sign in once in the Browser tab and keep it for every thread on this computer. It applies the next time the browser opens.',
            )}
      </p>
      {kept && (
        <button
          type="button"
          className="mt-1.5 block text-left text-[12px] underline underline-offset-2 disabled:opacity-50"
          style={{ color: 'var(--color-text-primary)' }}
          disabled={disabled}
          onClick={() => save(true)}
        >
          {t('computer.browserSignins.signOut', 'Sign out of every site')}
        </button>
      )}
    </section>
  );
}
