import { useEffect, useEffectEvent } from 'react';
import { isImeComposing } from '@/lib/ime';

/** Whether a key pressed here is someone writing, or working a menu or a dialog. */
function ownsKeys(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement) return true;
  if (target instanceof HTMLInputElement) return !['button', 'checkbox', 'radio', 'range', 'submit', 'reset'].includes(target.type);
  return !!target.closest('[role="dialog"], [role="menu"], [role="listbox"], [role="combobox"]');
}

/**
 * C turns comment mode on and off (`setMode`, null where the layer has no
 * mode), and Esc steps back one layer (`onEscape`) while there is one to step
 * back from (`escapes`: the mode, or the list), wherever the focus is that is
 * not someone writing, on the tab in front (`active`). Each Esc taken is
 * marked taken, as an open box marks the one it takes (`CommentPopover`), so
 * what listens after it (the Drive's close) lets it be. Inside the frame the
 * page's bridge reports them instead, and only while the mode is on.
 */
export function useModeKeys(
  active: boolean,
  mode: boolean,
  setMode: ((on: boolean) => void) | null,
  onEscape: () => void,
  escapes: boolean,
): void {
  const onKey = useEffectEvent((e: KeyboardEvent) => {
    if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey || isImeComposing(e) || ownsKeys(e.target)) return;
    if (e.key === 'c' || e.key === 'C') {
      if (e.repeat || !setMode) return;
      e.preventDefault();
      setMode(!mode);
    } else if (e.key === 'Escape' && escapes) {
      // Marked taken, as an open box marks the Esc it takes: one Esc steps
      // back one layer.
      e.preventDefault();
      onEscape();
    }
  });
  useEffect(() => {
    if (!active) return;
    const listener = (e: KeyboardEvent) => onKey(e);
    document.addEventListener('keydown', listener);
    return () => document.removeEventListener('keydown', listener);
  }, [active]);
}
