import { useMemo, useSyncExternalStore } from 'react';

/** One blob's object URL, minted while something subscribes and revoked when the last of it leaves. */
function objectUrlStore(blob: Blob | null | undefined) {
  let url: string | null = null;
  return {
    subscribe(notify: () => void) {
      if (!blob) return () => {};
      const minted = URL.createObjectURL(blob);
      url = minted;
      notify();
      return () => {
        URL.revokeObjectURL(minted);
        if (url === minted) url = null;
      };
    },
    get: () => url,
  };
}

/**
 * An object URL of `blob` for as long as the component showing it is mounted,
 * null before the first commit and without a blob. Minted after commit rather
 * than in render, so a render React throws away leaves no URL behind, and
 * each component mints its own, so one unmounting never breaks another's.
 */
export function useObjectUrl(blob: Blob | null | undefined): string | null {
  const store = useMemo(() => objectUrlStore(blob), [blob]);
  return useSyncExternalStore(store.subscribe, store.get);
}
