import { useCallback, useEffect, useRef, useState } from 'react';

type SaveStatus = 'idle' | 'saving' | 'saved' | 'error';

/**
 * Wraps a save function with debounce and status tracking.
 * - `trigger()`: schedule a debounced save (resets timer on rapid calls)
 * - `flush()`: save immediately, cancelling any pending debounce
 * - `status`: 'idle' | 'saving' | 'saved' | 'error'
 */
export function useDebouncedSave(
  saveFn: () => Promise<void>,
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
    if (!mountedRef.current) { runningRef.current = false; pendingRef.current = false; return; }
    setStatus('saving');
    try {
      await saveFnRef.current();
      if (!mountedRef.current) return;
      setStatus('saved');
      savedTimerRef.current = setTimeout(() => setStatus('idle'), 2000);
    } catch {
      if (!mountedRef.current) return;
      setStatus('error');
      savedTimerRef.current = setTimeout(() => setStatus('idle'), 3000);
    } finally {
      runningRef.current = false;
      if (pendingRef.current && mountedRef.current) {
        pendingRef.current = false;
        execute();
      } else {
        pendingRef.current = false;
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
    return () => {
      mountedRef.current = false;
      if (timerRef.current) clearTimeout(timerRef.current);
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
    };
  }, []);

  return { trigger, flush, status };
}
