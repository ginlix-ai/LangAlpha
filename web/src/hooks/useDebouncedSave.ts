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

  const execute = useCallback(async () => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    if (savedTimerRef.current) {
      clearTimeout(savedTimerRef.current);
      savedTimerRef.current = null;
    }
    setStatus('saving');
    try {
      await saveFnRef.current();
      setStatus('saved');
      savedTimerRef.current = setTimeout(() => setStatus('idle'), 2000);
    } catch {
      setStatus('error');
      savedTimerRef.current = setTimeout(() => setStatus('idle'), 3000);
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
      if (timerRef.current) clearTimeout(timerRef.current);
      if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
    };
  }, []);

  return { trigger, flush, status };
}
