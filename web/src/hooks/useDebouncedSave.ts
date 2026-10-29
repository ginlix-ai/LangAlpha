import { useCallback, useEffect, useRef, useState } from 'react';

type SaveStatus = 'idle' | 'saving' | 'saved' | 'error';

/**
 * Wraps a save function with debounce and status tracking.
 * - `trigger()`: schedule a debounced save (resets timer on rapid calls)
 * - `flush()`: save immediately, cancelling any pending debounce
 * - `status`: 'idle' | 'saving' | 'saved' | 'error'
 *
 * A save that resolves `false` wrote nothing, and the status goes back to idle
 * rather than claiming it saved.
 */
export function useDebouncedSave(
  saveFn: () => Promise<void | false>,
  delay = 800,
) {
  const [status, setStatus] = useState<SaveStatus>('idle');
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const savedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const saveFnRef = useRef(saveFn);
  saveFnRef.current = saveFn;
  const runningRef = useRef(false);
  const pendingRef = useRef(false);
  const mountedRef = useRef(true);

  const execute = useCallback(async () => {
    if (runningRef.current) {
      pendingRef.current = true;
      return;
    }
    runningRef.current = true;
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    if (savedTimerRef.current) {
      clearTimeout(savedTimerRef.current);
      savedTimerRef.current = null;
    }
    // The save itself must outlive unmount. `flush()` from a cleanup is the last
    // chance to persist a pending edit, and React clears `mountedRef` first —
    // this hook's own cleanup is registered before the caller's. Only the status
    // updates are gated on still being mounted.
    if (mountedRef.current) setStatus('saving');
    try {
      const saved = await saveFnRef.current();
      if (!mountedRef.current) return;
      if (saved === false) {
        setStatus('idle');
        return;
      }
      setStatus('saved');
      savedTimerRef.current = setTimeout(() => setStatus('idle'), 2000);
    } catch {
      if (!mountedRef.current) return;
      setStatus('error');
      savedTimerRef.current = setTimeout(() => setStatus('idle'), 3000);
    } finally {
      runningRef.current = false;
      // An edit made while this save ran still has to land, unmounted or not.
      if (pendingRef.current) {
        pendingRef.current = false;
        execute();
      }
    }
  }, []);

  const trigger = useCallback(() => {
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(execute, delay);
  }, [delay, execute]);

  const flush = useCallback(() => {
    execute();
  }, [execute]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (timerRef.current) clearTimeout(timerRef.current);
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
    };
  }, []);

  return { trigger, flush, status };
}
