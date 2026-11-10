import { useEffect, useEffectEvent } from 'react';

/**
 * Calls `onSlash` when `/` is pressed outside a text field: a page's
 * shortcut to its search. Where text is being typed, the key is the text's;
 * so is one something took already (the palette's field, buffering keys
 * while it opens), and one pressed in an open dialog.
 */
export function useSlashKey(onSlash: () => void, enabled = true): void {
  const handle = useEffectEvent(onSlash);
  useEffect(() => {
    if (!enabled) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey || e.defaultPrevented) return;
      const active = document.activeElement;
      if (active?.tagName === 'INPUT' || active?.tagName === 'TEXTAREA' || (active as HTMLElement | null)?.isContentEditable) return;
      if (active?.closest('[role="dialog"]')) return;
      e.preventDefault();
      handle();
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [enabled]);
}
