import { useSyncExternalStore } from 'react';
import { isPlatformMode } from '../config/hostMode';
import { LOCAL_USER_ID, hasAuthEnv } from '../config/localUser';

/**
 * Browser storage keyed by the signed-in user, for state that names a workspace
 * or a thread.
 *
 * That state outlives a sign-out, and a workspace id reaches the next account on
 * the browser through history or a shared link, so an unscoped key shows one
 * user's open thread or file tabs to another and lets the second overwrite the
 * first's. With nobody signed in on a platform build there is no one to keep it
 * for: reads find nothing and writes are dropped, since an unscoped write would
 * be adopted by whoever signs in next. A value stored before keys were scoped is
 * adopted by the first user to read it, then removed.
 */
export interface UserStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
  /** Removes the current user's keys under `prefix`, and any unscoped ones. */
  removeByPrefix(prefix: string): void;
}

let platformUser: string | null = null;
const userListeners = new Set<() => void>();

/**
 * The signed-in platform user, set by `authToken` whenever its session changes
 * hands. Pushed rather than read from there so that storage stays importable
 * without pulling in the auth client.
 */
export function setStorageUser(userId: string | null): void {
  if (userId === platformUser) return;
  platformUser = userId;
  userListeners.forEach((listener) => listener());
}

function subscribeStorageUser(listener: () => void): () => void {
  userListeners.add(listener);
  return () => userListeners.delete(listener);
}

/**
 * As AuthProvider presents the user: the local one wherever there is no auth
 * client, a platform build without its Supabase env included.
 */
function currentUser(): string | null {
  return isPlatformMode && hasAuthEnv ? platformUser : LOCAL_USER_ID;
}

/**
 * The user whose keys reads and writes reach now, for a hook that keeps a read
 * across renders: an account switch in place, with no sign-out between, leaves
 * the page mounted, so the hook must read again when this changes.
 */
export function useStorageUser(): string | null {
  return useSyncExternalStore(subscribeStorageUser, currentUser);
}

function scoped(userId: string, key: string): string {
  return `user:${userId}:${key}`;
}

function userScoped(store: () => Storage): UserStorage {
  // A blocked or full store (private mode, quota) reads as empty and drops
  // writes: none of this state is worth failing a render over.
  const read = (key: string) => {
    try { return store().getItem(key); } catch { return null; }
  };
  const write = (key: string, value: string) => {
    try { store().setItem(key, value); return true; } catch { return false; /* blocked or full */ }
  };
  const remove = (key: string) => {
    try { store().removeItem(key); } catch { /* blocked */ }
  };

  return {
    getItem(key) {
      const user = currentUser();
      if (!user) return null;
      const own = read(scoped(user, key));
      if (own !== null) return own;
      const legacy = read(key);
      // A full store refuses the copy, and the legacy key is then its only one.
      if (legacy !== null && write(scoped(user, key), legacy)) remove(key);
      return legacy;
    },
    setItem(key, value) {
      const user = currentUser();
      if (!user) return;
      // As above: a refused write leaves the legacy key the only copy.
      if (write(scoped(user, key), value)) remove(key);
    },
    removeItem(key) {
      const user = currentUser();
      if (user) remove(scoped(user, key));
      remove(key);
    },
    removeByPrefix(prefix) {
      const user = currentUser();
      const prefixes = user ? [scoped(user, prefix), prefix] : [prefix];
      try {
        const s = store();
        const doomed: string[] = [];
        for (let i = 0; i < s.length; i += 1) {
          const key = s.key(i);
          if (key && prefixes.some((p) => key.startsWith(p))) doomed.push(key);
        }
        doomed.forEach((key) => s.removeItem(key));
      } catch { /* blocked */ }
    },
  };
}

export const userLocalStorage = userScoped(() => localStorage);
export const userSessionStorage = userScoped(() => sessionStorage);
