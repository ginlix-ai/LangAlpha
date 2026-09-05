import { useEffect, useState } from 'react';
import { shellHandoffUrl } from '../../lib/desktopAuthHandoff';

/**
 * Hands an email link to the desktop app, and reports that this page must
 * therefore not redeem it.
 *
 * Decided once at mount rather than per render. Navigating to a custom scheme
 * leaves `window.location` untouched, so a decision taken again on a later
 * render would ask the OS to open the app a second time.
 *
 * Nothing reports back whether the app opened: a browser fires no event either
 * way, and a scheme it does not know fails silently. So the page carries its own
 * way out instead of waiting for a signal that never comes, and `continueHere`
 * is what a user on a machine without the app presses.
 */
export function useShellHandoff() {
  const [target] = useState(() =>
    shellHandoffUrl(window.location.pathname, window.location.search)
  );
  const [tookItBack, setTookItBack] = useState(false);
  const handingOff = target !== null && !tookItBack;

  useEffect(() => {
    if (!handingOff || !target) return;
    // `assign`, not an href write: this is a navigation to a scheme the OS
    // resolves, and the method says so where the assignment reads like state.
    window.location.assign(target);
  }, [handingOff, target]);

  return { handingOff, continueHere: () => setTookItBack(true) };
}
