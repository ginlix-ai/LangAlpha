import { registerAuthReset } from '@/lib/authResets';
import { claudeCodeTokenAdded } from '@/lib/claudeCode';
import { userSessionStorage } from '@/lib/userStorage';
import { useClaudeCodeSetup } from './useClaudeCode';
import { useUser } from './useUser';
import { allows, RULE } from '@/lib/accountRules';
import type { AccountPolicy } from '@/types/api';

const nothingOwnToConnect = (policy: AccountPolicy | null) =>
  !!policy && !allows(policy, RULE.ownApiKeys) && !allows(policy, RULE.ownSubscriptions);

// Under the account that pressed it: another one signed in on the tab has its
// own setup, a switch made where no auth reset runs (the hosted console, which
// reloads the app) included. Named apart from the unscoped key it replaces,
// which the first account to read it would otherwise adopt.
const SKIP_KEY = 'ginlix_setup_exited';

/**
 * Mark setup as skipped for this browser session.
 * Called by the wizard's "Exit setup" button so the gate lets them through.
 */
export function skipSetup(): void {
  userSessionStorage.setItem(SKIP_KEY, '1');
}

registerAuthReset(() => userSessionStorage.removeItem(SKIP_KEY));

/** Whether this account pressed "Exit setup" this browser session. */
export function setupSkipped(): boolean {
  return Boolean(userSessionStorage.getItem(SKIP_KEY));
}

/**
 * Determines whether the current user still needs to complete the
 * BYOK setup wizard: no key or subscription of their own, no plan, and no
 * deployment key that runs the default model. A self-hosted stack with keys
 * in its config is the last case: its turns already run, so sending its user
 * to the wizard on every visit was asking for a provider they have. Claude
 * Code runs on the user's own plan from a token the profile cannot name, so
 * only for a user none of those cover is its setup read: once they added a
 * token, setup has nothing left to ask.
 *
 * "Exit setup" sets a sessionStorage flag that suppresses the redirect for
 * the rest of this browser session. `enabled` false asks nothing, for an
 * account whose setup runs on its answers instead.
 */
export function useSetupGate({ enabled = true }: { enabled?: boolean } = {}): { isLoading: boolean; needsSetup: boolean } {
  const { user, isLoading } = useUser();
  const skipped = setupSkipped();
  const covered = !!user && (
    Boolean(user.has_api_key)
    || Boolean(user.has_oauth_token)
    || (user.access_tier ?? -1) >= 0
    || Boolean(user.deployment_serves)
    // Rules that keep both the user's own keys and sign-ins off leave setup
    // nothing to connect: the account's models are provided.
    || nothingOwnToConnect(user.policy ?? null)
  );
  const claudeCode = useClaudeCodeSetup({ enabled: enabled && !!user && !skipped && !covered });

  if (!enabled) return { isLoading: false, needsSetup: false };
  if (isLoading || !user) {
    return { isLoading: true, needsSetup: false };
  }
  if (skipped || covered) {
    return { isLoading: false, needsSetup: false };
  }
  if (claudeCode.isLoading) {
    return { isLoading: true, needsSetup: false };
  }
  // A read that failed says nothing about a token: send no one to setup on it,
  // where Continue would wait on the same read.
  if (claudeCode.unread) {
    return { isLoading: false, needsSetup: false };
  }
  return { isLoading: false, needsSetup: !claudeCodeTokenAdded(claudeCode.setup) };
}
