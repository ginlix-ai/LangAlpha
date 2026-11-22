import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { useSetBrowserChoices } from '../hooks/useSetBrowserChoices';
import { BrowserSigninImport } from './BrowserSigninImport';
import { SettingToggle } from './computerParts';

/**
 * Whether this computer's browser keeps sign-ins between its starts. A person
 * signs in from the Browser tab, and every thread on the computer then browses
 * as them. Every save starts the browser signed out of everything, so signing
 * out is the same save again.
 */
export function BrowserSigninsSetting({ computer }: { computer: Computer }) {
  const { t } = useTranslation();
  const mutation = useSetBrowserChoices();
  const kept = computer.browser?.signins !== false;
  const disabled = mutation.busyFor(computer.computer_id) || computer.status === 'deleted';
  const save = (signins: boolean) => mutation.mutate({ computerId: computer.computer_id, patch: { signins } });
  return (
    <SettingToggle
      label={t('computer.browserSignins.label', 'Keep sign-ins')}
      checked={kept}
      onChange={() => save(!kept)}
      disabled={disabled}
      hint={
        kept
          ? t(
              'computer.browserSignins.onHint',
              'Sites you sign in to in the Browser tab stay signed in, for every thread on this computer. Turning this off signs out of all of them.',
            )
          : t(
              'computer.browserSignins.offHint',
              'The browser forgets every site when it closes. Turn this on to sign in once in the Browser tab and keep it for every thread on this computer. It applies the next time the browser opens.',
            )
      }
    >
      {kept && (
        <button
          type="button"
          className="tap-target mt-1.5 block text-left text-[12px] underline underline-offset-2 disabled:opacity-50"
          style={{ color: 'var(--color-text-primary)' }}
          disabled={disabled}
          onClick={() => save(true)}
        >
          {t('computer.browserSignins.signOut', 'Sign out of every site')}
        </button>
      )}
      {kept && computer.kind === 'ssh' && <BrowserSigninImport computer={computer} />}
    </SettingToggle>
  );
}
