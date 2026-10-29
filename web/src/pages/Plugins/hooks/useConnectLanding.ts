import { useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';
import type { ConnectRecord, ConnectRetry } from '../connectReturn';

/** Where the connects this tab sent out are kept, one record per server. The
 *  name the first build gave it, so a marker an older tab left is still read. */
export const CONNECTS_KEY = 'mcp:connect-started';

/** What a connect was in the middle of, as the surface that reopens it needs. */
export type ConnectStart = Pick<ConnectRecord, 'server' | 'label' | 'retry' | 'desktopOnly'>;

/**
 * What came of a sign-in, handed to the surface that can act on it.
 *
 * The callback lands on whatever page it returns to, and the one shell
 * controller reads it there once; the surface that acts on it may not be
 * mounted until that page or the welcome flow loads. Held outside React
 * for that reason, and spent by the first surface that takes it, so a remount
 * never acts twice.
 */
export type ConnectLanding =
  | {
      id: number;
      /** A form or an entry's page to put the user back on. */
      owner: 'page';
      draft: ConnectStart;
      /**
       * `failed` came back with an error, `denied` with the user's own cancel
       * at the vendor, and `retry` is the user asking to start again after a
       * sign-in that never came back at all.
       */
      outcome: 'failed' | 'denied' | 'retry';
      /** For `failed`: the sentence for the callback's reason. */
      reasonKey?: string;
      /** For `failed`: the server's own words, plain text, said after our lead. */
      detail?: string | null;
    }
  | {
      id: number;
      /** The welcome flow's brokerage step, which the connect returns to. */
      owner: 'welcome';
      server: string;
      /** `connected` only for a connect this tab started: the flow moves on by
       *  itself on it, which a link must not be able to make it do. */
      outcome: 'connected' | 'failed';
      /** For `failed`: the reason's sentence, or null for one backed out of. */
      reasonKey: string | null;
    };

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

let current: ConnectLanding | null = null;
let nextId = 1;
const listeners = new Set<() => void>();

function emit(next: ConnectLanding | null) {
  current = next;
  for (const listener of listeners) listener();
}

export function setConnectLanding(landing: DistributiveOmit<ConnectLanding, 'id'>): void {
  emit({ ...landing, id: nextId++ });
}

/**
 * Take the landing, so no other surface and no remount acts on it again.
 * False when it was already taken: an effect run twice (Strict Mode, or deps
 * that changed before the store's null arrived) acts once.
 */
export function spendConnectLanding(id: number): boolean {
  if (current?.id !== id) return false;
  emit(null);
  return true;
}

// A landing and the connects this tab sent out belong to the account that
// started them: the next one to sign in here must not reopen its drafts.
registerAuthReset(() => {
  if (current !== null) emit(null);
  try {
    sessionStorage.removeItem(CONNECTS_KEY);
  } catch {
    // Private windows, blocked site data: nothing was kept to clear.
  }
});

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

const getSnapshot = () => current;

type PageLanding = Extract<ConnectLanding, { owner: 'page' }>;

/** The landing waiting for a surface of this kind, or null. */
export function useConnectLanding(kinds: readonly ConnectRetry['kind'][]): PageLanding | null {
  const landing = useSyncExternalStore(subscribe, getSnapshot);
  return landing?.owner === 'page' && kinds.includes(landing.draft.retry.kind) ? landing : null;
}

/** The landing waiting for the welcome flow, or null. */
export function useWelcomeLanding(): Extract<ConnectLanding, { owner: 'welcome' }> | null {
  const landing = useSyncExternalStore(subscribe, getSnapshot);
  return landing?.owner === 'welcome' ? landing : null;
}
