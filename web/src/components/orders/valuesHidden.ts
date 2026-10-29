import { useState } from 'react';

/**
 * One device-local answer to "should amounts be readable over my shoulder",
 * not one per surface: the dashboard's eye toggle, the Orders page and an
 * order's receipt in chat are the same numbers about the same account on the
 * same screen. `OrderApprovalCard` ignores it on purpose: a person approving
 * an order has to see the size and prices they are approving.
 */
const VALUES_HIDDEN_KEY = 'portfolio_values_hidden';

/** Read at render, so a toggle in another tab lands on the next mount rather
 *  than immediately. A browser with site data blocked still reads as visible. */
export function readValuesHidden(): boolean {
  try {
    return localStorage.getItem(VALUES_HIDDEN_KEY) === 'true';
  } catch {
    return false;
  }
}

export function useValuesHidden(): [boolean, () => void] {
  const [hidden, setHidden] = useState(readValuesHidden);
  const toggle = () =>
    setHidden((prev) => {
      const next = !prev;
      try {
        localStorage.setItem(VALUES_HIDDEN_KEY, String(next));
      } catch {
        /* a browser with site data blocked still gets the toggle */
      }
      return next;
    });
  return [hidden, toggle];
}
