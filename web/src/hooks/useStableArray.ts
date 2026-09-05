import { useRef } from 'react';

/**
 * Keeps the PREVIOUS array identity whenever a freshly derived array is
 * equivalent under `isEqual`. Upstream state (e.g. the messages array) takes a
 * new identity on every streamed chunk, so a derived projection would too —
 * tearing down effects and breaking memos on every token. Pass the derived
 * array (usually straight out of a `useMemo`) and the projection's own
 * equality; the identity only changes when a rendered field actually did.
 *
 * The compare runs in render phase, like the ref-mirror pattern it replaces.
 */
export function useStableArray<T>(next: T[], isEqual: (prev: T[], next: T[]) => boolean): T[] {
  const ref = useRef<T[]>(next);
  if (ref.current !== next && !isEqual(ref.current, next)) {
    ref.current = next;
  }
  return ref.current;
}
