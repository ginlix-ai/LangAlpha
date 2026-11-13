import { useSyncExternalStore } from 'react';
import { createEmitter } from '@/lib/emitter';

/**
 * One device-local answer to "should amounts be readable over my shoulder",
 * not one per surface: the dashboard's eye toggle, the Orders page, an order's
 * receipt in chat and the desktop menu bar panel are the same numbers about
 * the same account on the same screen. `OrderApprovalCard` ignores it on
 * purpose: a person approving an order has to see the size and prices they
 * are approving.
 */
const VALUES_HIDDEN_KEY = 'portfolio_values_hidden';

/** A browser with site data blocked still reads as visible. */
export function readValuesHidden(): boolean {
  try {
    return localStorage.getItem(VALUES_HIDDEN_KEY) === 'true';
  } catch {
    return false;
  }
}

const localWrites = createEmitter();

function subscribe(onChange: () => void): () => void {
  // `storage` carries a toggle made in another tab or window, such as the
  // long-lived menu bar page, which would otherwise see it only on a reload.
  window.addEventListener('storage', onChange);
  const off = localWrites.subscribe(onChange);
  return () => {
    window.removeEventListener('storage', onChange);
    off();
  };
}

/** Kept only in memory where site data is blocked, so the toggle still works for the session. */
let sessionHidden: boolean | null = null;

function snapshot(): boolean {
  return sessionHidden ?? readValuesHidden();
}

export function useValuesHidden(): [boolean, () => void] {
  const hidden = useSyncExternalStore(subscribe, snapshot, () => false);
  const toggle = () => {
    const next = !snapshot();
    try {
      localStorage.setItem(VALUES_HIDDEN_KEY, String(next));
      sessionHidden = null;
    } catch {
      sessionHidden = next;
    }
    localWrites.emit();
  };
  return [hidden, toggle];
}
