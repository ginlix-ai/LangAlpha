import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { supabase } from '../lib/supabase';
import { setTokenGetter } from '../api/client';

const AuthContext = createContext(null);

const _SUPABASE_AUTH_ENABLED = !!import.meta.env.VITE_SUPABASE_URL;
const _LOCAL_DEV_USER_ID = import.meta.env.VITE_AUTH_USER_ID || 'local-dev-user';

const baseURL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';

/**
 * Static provider value used when Supabase auth is disabled.
 * Presents the app as permanently logged-in with a local-dev identity.
 */
const _localDevValue = {
  userId: _LOCAL_DEV_USER_ID,
  user: { id: _LOCAL_DEV_USER_ID, name: 'Local User' },
  isInitialized: true,
  isLoggedIn: true,
  loginWithEmail: () => Promise.resolve(),
  signupWithEmail: () => Promise.resolve(),
  loginWithProvider: () => Promise.resolve(),
  logout: () => Promise.resolve(),
  refreshUser: () => {},
};

export function AuthProvider({ children }) {
  // Skip all Supabase logic when auth is disabled.
  if (!_SUPABASE_AUTH_ENABLED) {
    return <AuthContext.Provider value={_localDevValue}>{children}</AuthContext.Provider>;
  }

  return <SupabaseAuthProvider>{children}</SupabaseAuthProvider>;
}

/** Inner provider that uses hooks — only rendered when Supabase auth is enabled. */
function SupabaseAuthProvider({ children }) {
  const [session, setSession] = useState(null);
  const [localUser, setLocalUser] = useState(null);
  const [isInitialized, setIsInitialized] = useState(false);

  /** Sync Supabase user to our backend and store the profile locally. */
  const syncUser = useCallback(async (sess) => {
    if (!sess) return;
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
        }),
      });
      if (res.ok) {
        const data = await res.json();
        setLocalUser(data.user ?? data);
      }
    } catch (err) {
      console.error('[auth] syncUser failed:', err);
    }
    // Wire up the token getter so the axios interceptor can attach Bearer tokens.
    setTokenGetter(() =>
      supabase.auth.getSession().then((r) => r.data.session?.access_token)
    );
  }, []);

  // Bootstrap: read existing session and listen for auth changes.
  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session: sess } }) => {
      setSession(sess);
      if (sess) syncUser(sess);
      setIsInitialized(true);
    });

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, sess) => {
      setSession(sess);
      if (sess) {
        syncUser(sess);
      } else {
        setLocalUser(null);
        setTokenGetter(null);
      }
    });

    return () => subscription.unsubscribe();
  }, [syncUser]);

  const loginWithEmail = useCallback(
    (email, password) => supabase.auth.signInWithPassword({ email, password }),
    []
  );

  const signupWithEmail = useCallback(
    (email, password, name) =>
      supabase.auth.signUp({ email, password, options: { data: { name } } }),
    []
  );

  const loginWithProvider = useCallback(
    (provider) =>
      supabase.auth.signInWithOAuth({
        provider,
        options: { redirectTo: window.location.origin + '/callback' },
      }),
    []
  );

  const logout = useCallback(() => supabase.auth.signOut(), []);

  const refreshUser = useCallback(() => {
    if (session) syncUser(session);
  }, [session, syncUser]);

  const value = {
    userId: session?.user?.id ?? null,
    user: localUser,
    isInitialized,
    isLoggedIn: !!session,
    loginWithEmail,
    signupWithEmail,
    loginWithProvider,
    logout,
    refreshUser,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
