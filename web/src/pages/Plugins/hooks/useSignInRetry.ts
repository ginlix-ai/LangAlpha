import { useEffect, useEffectEvent, useState } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { pendingIntent } from '@/pages/ChatAgent/utils/api/mcpOauth';
import { signInFailureParts } from '../connectOutcome';
import type { ConnectRetry } from '../connectReturn';
import type { PluginSource } from '../components/PluginSourceStep';
import { spendConnectLanding, useConnectLanding } from './useConnectLanding';

/** Two sentences as one line: a space between them in English, none in Chinese. */
function joined(t: TFunction, first: string, second: string): string {
  return t('plugins.signin.joined', { first, second });
}

/** What a form reopened after a sign-in says about it. */
export interface SignInSaid {
  /** A cancel at the vendor: said plainly, beside the form, not as an error. */
  note?: string;
  /** A failure: said where the form says its other refusals. */
  error?: string;
}

/**
 * Reopen a form a sign-in was saving, for the surface that owns that form.
 *
 * `reopen` opens it on what the sign-in carried, saying why. Waits for
 * `ready`, the surface's own data, so a landing that arrives before the list
 * does is not spent on a form with nothing to open.
 */
export function useSignInRetry(
  kinds: readonly ConnectRetry['kind'][],
  ready: boolean,
  reopen: (retry: ConnectRetry, said: SignInSaid) => void,
): void {
  const { t } = useTranslation();
  const landing = useConnectLanding(kinds);
  const land = useEffectEvent((taken: NonNullable<typeof landing>) => {
    const { draft, outcome } = taken;
    if (outcome === 'denied') {
      reopen(draft.retry, {
        note: t(`plugins.signin.deniedLine.${pendingIntent(draft.retry.kind)}`, { label: draft.label }),
      });
    } else if (outcome === 'failed') {
      const [reason, detail] = signInFailureParts(t, taken.reasonKey, taken.detail);
      reopen(draft.retry, { error: detail ? joined(t, reason, detail) : reason });
    } else {
      reopen(draft.retry, {});
    }
  });
  useEffect(() => {
    if (landing && ready && spendConnectLanding(landing.id)) land(landing);
  }, [landing, ready]);
}

/** The install wizard's opening state, as `PluginInstallWizard` takes it. */
export interface WizardReopen {
  source: PluginSource | null;
  error: string | null;
  note: string | null;
}

/**
 * The install wizard reopened after a sign-in it was installing through, for
 * the list that owns the wizard. A link reopens filled in. A zip is not kept
 * between pages, so an upload reopens empty and says to choose the file again.
 */
export function useWizardSignInRetry(open: () => void): {
  initial: WizardReopen | undefined;
  clear: () => void;
} {
  const { t } = useTranslation();
  const [initial, setInitial] = useState<WizardReopen>();
  useSignInRetry(['plugin', 'plugin_upload'], true, (retry, said) => {
    const again = retry.kind === 'plugin_upload' ? t('plugins.signin.chooseFileAgain') : null;
    const join = (line: string | undefined) => (line && again ? joined(t, line, again) : (line ?? null));
    setInitial({
      source: retry.kind === 'plugin' ? { url: retry.sourceUrl } : null,
      error: join(said.error),
      note: said.error ? null : (join(said.note) ?? again),
    });
    open();
  });
  return { initial, clear: () => setInitial(undefined) };
}
