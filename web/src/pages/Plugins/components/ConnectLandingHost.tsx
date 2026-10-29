import { useEffect, useEffectEvent } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useBfcacheRestore } from '@/hooks/useBfcacheRestore';
import { CONNECTS_KEY } from '../hooks/useConnectLanding';

/** Whether this landing can have anything to say, judged without loading
 *  what reads it: a callback's answer, or a connect this tab sent out. */
function mayHaveLanded(): boolean {
  if (/[?&]mcp_(connected|error)=/.test(window.location.search)) return true;
  try {
    return sessionStorage.getItem(CONNECTS_KEY) !== null;
  } catch {
    return false;
  }
}

/**
 * The one reader of a connect's return, mounted once in the app shell so it
 * covers every page a connect can start from or come back to. A sign-in comes
 * back as a page load, through its callback or by Back from the vendor's page,
 * or as a back/forward-cache restore, which remounts nothing. What reads it
 * stays off the first load: most loads have nothing to read.
 */
export function ConnectLandingHost() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const land = () => {
    if (!mayHaveLanded()) return;
    void import('../connectLanding').then(
      (m) => m.landConnectReturn({ t, navigate }),
      // A chunk that fails here is the app's stale-build recovery's to take.
      () => {},
    );
  };
  const landOnMount = useEffectEvent(land);
  useEffect(() => landOnMount(), []);
  useBfcacheRestore(land);
  return null;
}
