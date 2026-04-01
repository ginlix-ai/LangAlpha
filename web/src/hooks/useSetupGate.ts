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
 * BYOK setup wizard (i.e. has neither configured an API key nor
 * redeemed an invitation code).
 *
 * When auth is off (self-hosted / local dev), the gate still fires so
 * users land on the wizard, but SetupWizard always shows an "Exit" button
 * so they can skip it freely. Clicking "Exit" sets a sessionStorage flag
 * that suppresses the redirect for the rest of this browser session.
 */
export function useSetupGate(): { isLoading: boolean; needsSetup: boolean } {
  const { user, isLoading } = useUser();

  if (isLoading || !user) {
    return { isLoading: true, needsSetup: false };
  }

  // User explicitly skipped the wizard this session
  if (sessionStorage.getItem(SKIP_KEY)) {
    return { isLoading: false, needsSetup: false };
  }

  const hasApiKey = Boolean(user.has_api_key);
  const hasOAuth = Boolean(user.has_oauth_token);
  const hasPlatformAccess = (user.access_tier ?? -1) >= 0;
  const needsSetup = !hasApiKey && !hasOAuth && !hasPlatformAccess;

  return { isLoading: false, needsSetup };
}
