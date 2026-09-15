import { useEffect, useState } from 'react';

/**
 * `value` as it stood `delayMs` ago, so a caller can ask "has this rested?"
 * by comparing the two. Used to hold a request back while the user is still
 * typing: the query key changes on every keystroke, the debounced copy catches
 * up only once the typing stops, and the gate is `rested === live`.
 */
export function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [rested, setRested] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setRested(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return rested;
}
