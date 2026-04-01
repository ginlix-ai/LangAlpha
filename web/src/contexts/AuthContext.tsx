import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { supabase } from '../lib/supabase';
import { setTokenGetter } from '../api/client';
import { queryKeys } from '../lib/queryKeys';

import type { AuthResponse, OAuthResponse, Provider, Session } from '@supabase/supabase-js';

export interface AuthContextValue {
  userId: string | null;
  isInitialized: boolean;
  isLoggedIn: boolean;
  loginWithEmail: (email: string, password: string) => Promise<AuthResponse | void>;
  signupWithEmail: (email: string, password: string, name: string) => Promise<AuthResponse | void>;
  loginWithProvider: (provider: Provider) => Promise<OAuthResponse | void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

const _SUPABASE_AUTH_ENABLED = !!import.meta.env.VITE_SUPABASE_URL;
const _LOCAL_DEV_USER_ID = (import.meta.env.VITE_AUTH_USER_ID as string) || 'local-dev-user';

const baseURL = (import.meta.env.VITE_API_BASE_URL as string) ?? '';

/**
 * Static provider value used when Supabase auth is disabled.
 * Presents the app as permanently logged-in with a local-dev identity.
 */
const _localDevValue: AuthContextValue = {
  userId: _LOCAL_DEV_USER_ID,
  isInitialized: true,
  isLoggedIn: true,
  loginWithEmail: () => Promise.resolve(),
  signupWithEmail: () => Promise.resolve(),
  loginWithProvider: () => Promise.resolve(),
  logout: () => Promise.resolve(),
};

export function AuthProvider({ children }: { children: React.ReactNode }) {
  // Skip all Supabase logic when auth is disabled.
  if (!_SUPABASE_AUTH_ENABLED) {
    return <AuthContext.Provider value={_localDevValue}>{children}</AuthContext.Provider>;
  }

  return <SupabaseAuthProvider>{children}</SupabaseAuthProvider>;
}

// Module-level — deduplicates concurrent syncUser calls within the same tab
let _syncPromise: Promise<void> | null = null;

/** Inner provider that uses hooks — only rendered when Supabase auth is enabled. */
function SupabaseAuthProvider({ children }: { children: React.ReactNode }) {
  // supabase is guaranteed non-null here because SupabaseAuthProvider is only
  // rendered when _SUPABASE_AUTH_ENABLED is true.
  const sb = supabase!;
  const [session, setSession] = useState<Session | null>(null);
  const [isInitialized, setIsInitialized] = useState(false);
  const queryClient = useQueryClient();

  /** Wire up the axios token getter immediately when we have a session. */
  const wireTokenGetter = useCallback(() => {
    setTokenGetter(() =>
      sb.auth.getSession().then((r) => r.data.session?.access_token ?? null)
    );
  }, [sb]);

  /** Sync user on actual sign-in: create/migrate + backfill fields. Seed React Query cache. */
  const syncUser = useCallback(async (sess: Session) => {
    if (!sess) return;
    if (_syncPromise) return _syncPromise;
    _syncPromise = (async () => {
      try {
        const token = sess.access_token;
        const meta = sess.user?.user_metadata ?? {};
        const res = await fetch(`${baseURL}/api/v1/auth/sync`, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            email: sess.user?.email,
            name: meta.name || meta.full_name || null,
            avatar_url: meta.avatar_url || null,
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || null,
            locale: navigator.language || null,
          }),
        });
        if (res.ok) {
          const data = await res.json();
          // Seed preferences cache (auth/sync is authoritative for these).
          // Do NOT seed user.me() here — auth/sync omits fields like
          // access_tier, and seeding would overwrite the correct value
          // from the GET /users/me fetch already in-flight (triggered
          // by invalidateQueries in the getSession() handler).
          if (data.preferences !== undefined) {
            queryClient.setQueryData(queryKeys.user.preferences(), data.preferences ?? null);
          }
        }
      } catch (err) {
        console.error('[auth] syncUser failed:', err);
      } finally {
        _syncPromise = null;
      }
    })();
    return _syncPromise;
  }, [queryClient]);

  // Bootstrap: read existing session and listen for auth changes.
  useEffect(() => {
    sb.auth.getSession().then(({ data: { session: sess } }) => {
      setSession(sess);
      if (sess) {
        wireTokenGetter();
        // Trigger background refetch of user data via React Query
        queryClient.invalidateQueries({ queryKey: queryKeys.user.all });
      }
      setIsInitialized(true);
    });

    const {
      data: { subscription },
    } = sb.auth.onAuthStateChange((event, sess) => {
      setSession(sess);
      if (sess) {
        wireTokenGetter();
        if (event === 'SIGNED_IN') {
          syncUser(sess);  // Full sync only on actual login
        } else if (event === 'INITIAL_SESSION' || event === 'TOKEN_REFRESHED') {
          // INITIAL_SESSION: getSession() above already triggers invalidation
          // TOKEN_REFRESHED: no backend call needed
        } else {
          queryClient.invalidateQueries({ queryKey: queryKeys.user.all });
        }
      } else {
        // Logged out — wipe all cached data
        queryClient.clear();
        setTokenGetter(() => Promise.resolve(null));
      }
    });

    return () => subscription.unsubscribe();
  }, [sb, wireTokenGetter, syncUser, queryClient]);

  const loginWithEmail = useCallback(
    (email: string, password: string) => sb.auth.signInWithPassword({ email, password }),
    [sb.auth]
  );

  const signupWithEmail = useCallback(
    (email: string, password: string, name: string) =>
      sb.auth.signUp({ email, password, options: { data: { name } } }),
    [sb.auth]
  );

  const loginWithProvider = useCallback(
    (provider: Provider) =>
      sb.auth.signInWithOAuth({
        provider,
        options: { redirectTo: window.location.origin + '/callback' },
      }),
    [sb.auth]
  );

  const logout = useCallback(async () => {
    await sb.auth.signOut();
    queryClient.clear();
  }, [sb.auth, queryClient]);

  const value: AuthContextValue = {
    userId: session?.user?.id ?? null,
    isInitialized,
    isLoggedIn: !!session,
    loginWithEmail,
    signupWithEmail,
    loginWithProvider,
    logout,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// eslint-disable-next-line react-refresh/only-export-components
export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
