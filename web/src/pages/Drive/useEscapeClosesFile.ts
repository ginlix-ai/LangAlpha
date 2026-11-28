import { useEffect, useEffectEvent, useRef } from 'react';
import { isImeComposing } from '@/lib/ime';

/**
 * How long after an Escape something on the page took (a comment box, a card
 * or comment mode closed) another one leaves the file open. People press
 * Escape a few times to put things away, and the press after the last thing
 * closed is not a request to close the file too.
 */
export const ESCAPE_GRACE_MS = 600;

/**
 * Escape puts an open file away, as it closes any other panel. Not while
 * `on` is false (no file, or the editor open, which keeps its own keys), nor
 * over a field, a dialog or a menu, which Escape closes first; nor within
 * {@link ESCAPE_GRACE_MS} of an Escape something else took, nor while the key
 * is held down.
 */
export function useEscapeClosesFile(on: boolean, close: () => void): void {
  const onEscape = useEffectEvent(close);
  // When something else last took an Escape; kept across `on` so a box closed
  // the moment before the file came back still counts.
  const takenAt = useRef(Number.NEGATIVE_INFINITY);
  useEffect(() => {
    if (!on) return;
    // An Escape on its way down that has not come back up yet, and when. One
    // whose taker stopped it (an edit box, a draft card) never comes back, so
    // the next press finds it still here and counts it as taken.
    let down: number | null = null;
    const onDown = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' || isImeComposing(e)) return;
      if (down !== null) takenAt.current = down;
      down = performance.now();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' || isImeComposing(e)) return;
      down = null;
      // Listened for on the window, after the document's listeners have run,
      // so an Escape the comment layer took is already marked.
      if (e.defaultPrevented) {
        takenAt.current = performance.now();
        return;
      }
      if (e.repeat || performance.now() - takenAt.current < ESCAPE_GRACE_MS) return;
      const target = e.target as HTMLElement | null;
      if (target?.closest?.('input, textarea, select, [contenteditable="true"], .monaco-editor')) return;
      if (document.querySelector('[role="dialog"], [role="menu"], [role="listbox"]')) return;
      onEscape();
    };
    window.addEventListener('keydown', onDown, { capture: true });
    window.addEventListener('keydown', onKey);
    return () => {
      window.removeEventListener('keydown', onDown, { capture: true });
      window.removeEventListener('keydown', onKey);
    };
  }, [on]);
}
