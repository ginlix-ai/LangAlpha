import { useLocation, useNavigate } from 'react-router';
import { useAllWorkspacesAgent } from '@/hooks/useAllWorkspacesAgent';

/** The query parameter that opens the brokerage connect sheet over the page. */
export const ONBOARDING_PARAM = 'onboarding';
export const ONBOARDING_CONNECT = 'connect';

/**
 * Start onboarding: open the brokerage connect sheet that comes before the
 * Chief of Staff's interview, over whatever page the user is on.
 *
 * A URL parameter rather than router state, because connecting leaves the app
 * for the vendor's sign-in page and the callback lands on the URL it was given:
 * the parameter is what reopens the sheet the user left. `available` is false
 * without the Chief of Staff, the only agent that runs onboarding, and every
 * entry point hides itself then.
 */
export function useStartOnboarding() {
  const available = useAllWorkspacesAgent();
  const navigate = useNavigate();
  const location = useLocation();

  /** `at` opens the sheet over another page, for a caller outside the app
   *  shell; `replace` drops the caller's own page from history on the way. */
  function start({ at, replace = false }: { at?: string; replace?: boolean } = {}) {
    if (!available) return;
    const params = new URLSearchParams(at ? '' : location.search);
    params.set(ONBOARDING_PARAM, ONBOARDING_CONNECT);
    navigate({ pathname: at ?? location.pathname, search: `?${params}` }, { replace });
  }

  return { available, start };
}
