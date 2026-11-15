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
 * C turns comment mode on and off and Esc steps back one layer while the
 * mode is on (`onEscape`), wherever the focus is that is not someone writing,
 * on the tab in front (`active`). An open box takes Esc first and marks it
 * taken (`CommentPopover`). Inside the frame the page's bridge reports them
 * instead, and only while the mode is on.
 */
export function useModeKeys(active: boolean, mode: boolean, setMode: (on: boolean) => void, onEscape: () => void): void {
  const onKey = useEffectEvent((e: KeyboardEvent) => {
    if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey || isImeComposing(e) || ownsKeys(e.target)) return;
    if (e.key === 'c' || e.key === 'C') {
      if (e.repeat) return;
      e.preventDefault();
      setMode(!mode);
    } else if (e.key === 'Escape' && mode) {
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
