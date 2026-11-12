import { claudeCodeTokenAdded } from '@/lib/claudeCode';
import { useClaudeCodeSetup } from './useClaudeCode';
import { useUser } from './useUser';

const SKIP_KEY = 'ginlix_setup_skipped';

/**
 * Mark setup as skipped for this browser session.
 * Called by the wizard's "Exit setup" button so the gate lets them through.
 */
export function skipSetup(): void {
  sessionStorage.setItem(SKIP_KEY, '1');
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
 * the rest of this browser session.
 */
export function useSetupGate(): { isLoading: boolean; needsSetup: boolean } {
  const { user, isLoading } = useUser();
  const skipped = Boolean(sessionStorage.getItem(SKIP_KEY));
  const covered = !!user && (
    Boolean(user.has_api_key)
    || Boolean(user.has_oauth_token)
    || (user.access_tier ?? -1) >= 0
    || Boolean(user.deployment_serves)
  );
  const claudeCode = useClaudeCodeSetup({ enabled: !!user && !skipped && !covered });

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
