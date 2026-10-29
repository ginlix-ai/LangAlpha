import { useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';
import type { ResolvedConnect } from './hooks/useMcpOauthActions';

/**
 * The question a connect raises before the vendor's page, held where the one
 * dialog that answers it can read it.
 *
 * One question for the whole app, because only one can be on screen: every
 * surface that starts a connect raises it here, and `ConnectConsentHost`,
 * mounted once in the app shell, renders it. A surface that had to render the
 * dialog itself was a gate the next surface could forget, and a forgotten one
 * failed silently, holding a question nobody could see. Kept apart from the
 * connect lifecycle so the shell can watch it without loading that lifecycle.
 */
export interface ConnectQuestion {
  /** The surface that asked, which only it may take back or read as its own. */
  owner: symbol;
  request: ResolvedConnect;
  /** The answer is in and the sign-in is starting: the dialog stays up, busy,
   *  until the page leaves or the start fails. */
  starting: boolean;
  /** Go ahead, on the terms the dialog collected. */
  answer: (grantedCapabilities?: string[]) => void;
  /** Take the question back. */
  cancel: () => void;
}

let current: ConnectQuestion | null = null;
const listeners = new Set<() => void>();

export function readConnectQuestion(): ConnectQuestion | null {
  return current;
}

export function setConnectQuestion(next: ConnectQuestion | null): void {
  current = next;
  for (const listener of listeners) listener();
}

// A question belongs to the account that raised it: it can hold the keys a
// plugin's install was given, and its Yes would start that install as whoever
// signs in next. Taken back rather than dropped, so the surface waiting on it
// hears so.
registerAuthReset(() => {
  const open = current;
  if (open === null) return;
  open.cancel();
  if (current === open) setConnectQuestion(null);
});

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useConnectQuestion(): ConnectQuestion | null {
  return useSyncExternalStore(subscribe, readConnectQuestion, () => null);
}

/** The dialog that answers, in its own chunk: most sessions never ask. */
export const loadConnectConsent = () => import('./components/ConnectConsent');

/** Warm the dialog's chunk, so a question opens on the click that raised it. */
export function preloadConnectConsent(): void {
  // A chunk that fails here fails again on the question, where the app's
  // stale-build recovery takes it.
  void loadConnectConsent().catch(() => {});
}
