import { useEffect, useEffectEvent } from 'react';

/**
 * Run `onRestore` when the page comes back from the back/forward cache.
 *
 * A restore brings the page back exactly as it was left, on its way out to a
 * vendor's sign-in included, and remounts nothing, so whatever a fresh mount
 * would have reset is reset here instead. Backing out of the vendor's page is
 * the usual way back from a sign-in, and a mount effect never sees it.
 */
export function useBfcacheRestore(onRestore: () => void, enabled = true): void {
  const restore = useEffectEvent(onRestore);
  useEffect(() => {
    if (!enabled) return;
    const onShow = (e: PageTransitionEvent) => {
      if (e.persisted) restore();
    };
    window.addEventListener('pageshow', onShow);
    return () => window.removeEventListener('pageshow', onShow);
  }, [enabled]);
}
