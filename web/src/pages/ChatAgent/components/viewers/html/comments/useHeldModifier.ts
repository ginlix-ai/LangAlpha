import { useEffect, useState } from 'react';

/**
 * Whether Cmd (Ctrl elsewhere) is held, while `active`. Each document hears
 * only its own keys, so the last word holds: the page's report (`pageHeld`,
 * for keys pressed with the focus in the frame) or a key in this document.
 * Listens only while active, since every other Cmd press in the app would
 * otherwise render the layer, and forgets the key once inactive, whose
 * release it may never hear.
 */
export function useHeldModifier(active: boolean, pageHeld: boolean): boolean {
  const [held, setHeld] = useState(false);
  const [seen, setSeen] = useState({ active, pageHeld });
  if (active !== seen.active || pageHeld !== seen.pageHeld) {
    setSeen({ active, pageHeld });
    setHeld(active && pageHeld);
  }
  useEffect(() => {
    if (!active) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Meta' || e.key === 'Control') setHeld(e.type === 'keydown');
      // With another key it is a shortcut being typed (Cmd-Z, Ctrl-Arrow),
      // not a part being picked.
      else if (e.type === 'keydown' && (e.metaKey || e.ctrlKey)) setHeld(false);
    };
    // Focus going into the page blurs this window with the key still down,
    // and the page reports when it comes up.
    const onBlur = () => {
      if (!(document.activeElement instanceof HTMLIFrameElement)) setHeld(false);
    };
    window.addEventListener('keydown', onKey, true);
    window.addEventListener('keyup', onKey, true);
    window.addEventListener('blur', onBlur);
    return () => {
      window.removeEventListener('keydown', onKey, true);
      window.removeEventListener('keyup', onKey, true);
      window.removeEventListener('blur', onBlur);
    };
  }, [active]);
  return active && held;
}
