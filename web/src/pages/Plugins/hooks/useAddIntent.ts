import { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ADD_PARAM, parseAddIntent, type AddIntent } from '../utils/addParam';

/**
 * Act on the page-header Add menu's `?add=` intent, once.
 *
 * The param is stripped before the handler runs, which is what makes it
 * single-shot: the tab bodies are conditionally rendered, so a switch away and
 * back remounts the list, and an intent still sitting in the URL would open
 * the modal again on every remount.
 */
export function useAddIntent(handlers: Partial<Record<AddIntent, () => void>>) {
  const [searchParams, setSearchParams] = useSearchParams();
  const intent = parseAddIntent(searchParams);
  const handler = intent ? handlers[intent] : undefined;

  useEffect(() => {
    if (!handler) return;
    const next = new URLSearchParams(searchParams);
    next.delete(ADD_PARAM);
    setSearchParams(next, { replace: true });
    handler();
    // The intent is the trigger; re-running on a fresh `handlers` object (a new
    // one every render) would fire it again for the same param.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intent]);
}
