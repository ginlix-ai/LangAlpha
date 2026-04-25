import { useEffect, useState } from 'react';

/**
 * Track the browser's reported online status.
 *
 * `navigator.onLine` is a soft signal — true means "the OS thinks we have a
 * link," not "the network actually works." It still catches the common cases
 * (dropped wifi, airplane mode, ethernet unplug) which is the v1 target. DNS
 * failures, captive portals, and origin-specific outages aren't covered.
 *
 * SSR-safe: returns `true` when `navigator` is undefined so build-time render
 * doesn't crash. The effect bails on missing `window`.
 */
export function useNetworkStatus(): { online: boolean } {
  const [online, setOnline] = useState<boolean>(() =>
    typeof navigator === 'undefined' ? true : navigator.onLine
  );

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const goOnline = () => setOnline(true);
    const goOffline = () => setOnline(false);
    window.addEventListener('online', goOnline);
    window.addEventListener('offline', goOffline);
    return () => {
      window.removeEventListener('online', goOnline);
      window.removeEventListener('offline', goOffline);
    };
  }, []);

  return { online };
}
