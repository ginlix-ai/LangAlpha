import { useEffect, useEffectEvent } from 'react';

/** Whether a key pressed here is someone writing, or working a menu or a dialog. */
function ownsKeys(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  if (target.isContentEditable || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement) return true;
  if (target instanceof HTMLInputElement) return !['button', 'checkbox', 'radio', 'range', 'submit', 'reset'].includes(target.type);
  return !!target.closest('[role="dialog"], [role="menu"], [role="listbox"], [role="combobox"]');
}

/**
 * C turns comment mode on and off, Esc leaves it while no box is open,
 * wherever the focus is that is not someone writing, on the tab in front
 * (`active`). Inside the frame the page's bridge reports them instead, and
 * only while the mode is on.
 */
export function useModeKeys(active: boolean, mode: boolean, boxOpen: boolean, setMode: (on: boolean) => void): void {
  const onKey = useEffectEvent((e: KeyboardEvent) => {
    if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey || e.isComposing || ownsKeys(e.target)) return;
    if (e.key === 'c' || e.key === 'C') {
      if (e.repeat) return;
      e.preventDefault();
      setMode(!mode);
    } else if (e.key === 'Escape' && mode && !boxOpen) {
      setMode(false);
    }
  });
  useEffect(() => {
    if (!active) return;
    const listener = (e: KeyboardEvent) => onKey(e);
    document.addEventListener('keydown', listener);
    return () => document.removeEventListener('keydown', listener);
  }, [active]);
}
