import React, { createContext, useContext, useState, useEffect, useCallback, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { supabase } from '../lib/supabase';
import { publishSession, sessionAdopter, clearAuthToken } from '../lib/authToken';
import { queryKeys } from '../lib/queryKeys';
import { AUTH_BROADCAST_CHANNEL, OAUTH_POPUP_WINDOW_NAME, OAUTH_POPUP_FEATURES } from '../lib/oauthPopup';
import { clearFlashWorkspaceCache } from '@/pages/MarketView/utils/flashWorkspace';
import { resetNavPanelExpansion } from '@/pages/ChatAgent/components/navExpansionStore';
import { resetStableNavOrder } from '@/pages/ChatAgent/hooks/useNavigationData';
import { resetSharedWorkspaceThreads } from '@/lib/navThreadsStore';
import { runAuthResets } from '../lib/authResets';

import type {
  AuthError,
  AuthOtpResponse,
  AuthResponse,
  EmailOtpType,
  OAuthResponse,
  Provider,
  Session,
  UserResponse,
} from '@supabase/supabase-js';

export interface AuthContextValue {
  userId: string | null;
  isInitialized: boolean;
  isLoggedIn: boolean;
  loginWithEmail: (email: string, password: string) => Promise<AuthResponse | void>;
  signupWithEmail: (email: string, password: string, name: string) => Promise<AuthResponse | void>;
  loginWithProvider: (provider: Provider) => Promise<OAuthResponse | void>;
  logout: () => Promise<void>;
  /** Emails a magic sign-in link; creates the account if the email is new. */
  sendMagicLink: (email: string) => Promise<AuthOtpResponse | void>;
  /** Emails a password-recovery link. */
  sendPasswordReset: (email: string) => Promise<{ data: object | null; error: AuthError | null } | void>;
  /** Re-sends the signup confirmation email (server-limited to one per 60s). */
  resendConfirmation: (email: string) => Promise<AuthOtpResponse | void>;
  /** Verifies a token_hash from an email link, establishing a session. */
  verifyEmailOtp: (tokenHash: string, type: EmailOtpType) => Promise<AuthResponse | void>;
  /** Sets a new password on the current (e.g. recovery) session. */
  updatePassword: (password: string) => Promise<UserResponse | void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

import { isPlatformMode } from '@/config/hostMode';
import { withShellReturn } from '../lib/desktopAuthHandoff';

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
  sendMagicLink: () => Promise.resolve(),
  sendPasswordReset: () => Promise.resolve(),
  resendConfirmation: () => Promise.resolve(),
  verifyEmailOtp: () => Promise.resolve(),
  updatePassword: () => Promise.resolve(),
};

// Landing routes for Supabase email links. Both must be in the Supabase
// redirect-URL allow-list. Recovery links land directly on the reset form:
// the PKCE `?code=` a default email template produces is auto-exchanged and
// stripped from the URL before any component can read it, so the landing
// path is the only reliable carrier of the "reset password" intent.
// `withShellReturn` marks these when this window is the desktop app, so the
// browser that opens the mail passes the link back instead of redeeming it and
// stranding the app on "check your inbox".
const emailConfirmUrl = () => withShellReturn(window.location.origin + '/auth/confirm');
const passwordResetUrl = () => withShellReturn(window.location.origin + '/reset-password');

export function AuthProvider({ children }: { children: React.ReactNode }) {
  // Skip all Supabase logic in OSS mode. `supabase` is checked separately
  // because it keys on the two Supabase env vars, not on the host mode: a
  // platform build with those unset still has no client to talk to.
  if (!isPlatformMode || !supabase) {
    // Say so loudly in that second case. The fallback renders a signed-in
    // local-dev UI, which for a platform build is a misconfiguration wearing a
    // working app's face: requests go out unauthenticated and the backend
    // rejects them, with nothing on screen to explain why.
    if (isPlatformMode) {
      console.error(
        '[auth] VITE_HOST_MODE=platform but VITE_SUPABASE_URL/_KEY are unset, '
        + 'so there is no auth client. Falling back to the local-dev context; '
        + 'every authenticated request will be rejected.',
      );
    }
    return <AuthContext.Provider value={_localDevValue}>{children}</AuthContext.Provider>;
  }

  return <SupabaseAuthProvider>{children}</SupabaseAuthProvider>;
}

// Module-level — deduplicates concurrent syncUser calls within the same tab
let _syncPromise: Promise<void> | null = null;

/** Inner provider that uses hooks — only rendered when Supabase auth is enabled. */
function SupabaseAuthProvider({ children }: { children: React.ReactNode }) {
  // supabase is guaranteed non-null here: AuthProvider checks it before
  // rendering this component.
  const sb = supabase!;
  const [session, setSession] = useState<Session | null>(null);
  const [isInitialized, setIsInitialized] = useState(false);
  // Tracks the signed-in user across auth events so an account switch (e.g.
  // an email link for account B opened while A is logged in) can be detected.
  const lastUserIdRef = useRef<string | null>(null);
  const queryClient = useQueryClient();

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
            // `locale` deliberately omitted — only the Settings dropdown
            // writes it. The frontend detector reads browser locale on cold
            // load. See `useSyncUserLocale`.
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
    // Fenced, because this read is async: a cross-tab sign-out landing between
    // the request and this handler must not put the departed user's token back.
    const adopt = sessionAdopter();
    sb.auth.getSession().then(({ data: { session: sess } }) => {
      adopt(sess);
      setSession(sess);
      if (sess) {
        // Trigger background refetch of user data via React Query
        queryClient.invalidateQueries({ queryKey: queryKeys.user.all });
      }
      setIsInitialized(true);
    }).catch((err) => {
      // A corrupt/undecryptable session cookie can reject here — surface it but
      // still initialize so the app renders the login screen instead of wedging.
      console.error('[auth] getSession bootstrap failed:', err);
      setIsInitialized(true);
    });

    const {
      data: { subscription },
    } = sb.auth.onAuthStateChange((event, sess) => {
      // First, and synchronously: every event that carries a session is the
      // freshest token we will be told about, including TOKEN_REFRESHED from
      // auth-js's own background timer. This callback must stay non-async: it
      // runs under an exclusive lock and an async one can deadlock across tabs.
      publishSession(sess);
      // Switching accounts without a sign-out in between must not render the
      // new user against the old user's cached data.
      if (sess?.user && lastUserIdRef.current && sess.user.id !== lastUserIdRef.current) {
        queryClient.clear();
        clearFlashWorkspaceCache();
        resetNavPanelExpansion();
        resetStableNavOrder();
        resetSharedWorkspaceThreads();
        runAuthResets();
      }
      lastUserIdRef.current = sess?.user?.id ?? null;
      setSession(sess);
      if (sess) {
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
        clearFlashWorkspaceCache();
        // Module-level nav stores live on globalThis (no page reload on logout),
        // so they'd otherwise leak one user's folders/thread lists into the next
        // user's session on a shared tab. Reset them on every sign-out.
        resetNavPanelExpansion();
        resetStableNavOrder();
        resetSharedWorkspaceThreads();
        runAuthResets();
        clearAuthToken();
      }
    });

    return () => subscription.unsubscribe();
  }, [sb, syncUser, queryClient]);

  const loginWithEmail = useCallback(
    (email: string, password: string) => sb.auth.signInWithPassword({ email, password }),
    [sb.auth]
  );

  const signupWithEmail = useCallback(
    (email: string, password: string, name: string) =>
      sb.auth.signUp({
        email,
        password,
        options: { data: { name }, emailRedirectTo: emailConfirmUrl() },
      }),
    [sb.auth]
  );

  const sendMagicLink = useCallback(
    (email: string) =>
      sb.auth.signInWithOtp({
        email,
        options: { shouldCreateUser: true, emailRedirectTo: emailConfirmUrl() },
      }),
    [sb.auth]
  );

  const sendPasswordReset = useCallback(
    (email: string) => sb.auth.resetPasswordForEmail(email, { redirectTo: passwordResetUrl() }),
    [sb.auth]
  );

  const resendConfirmation = useCallback(
    (email: string) =>
      sb.auth.resend({ type: 'signup', email, options: { emailRedirectTo: emailConfirmUrl() } }),
    [sb.auth]
  );

  const verifyEmailOtp = useCallback(
    (tokenHash: string, type: EmailOtpType) => sb.auth.verifyOtp({ token_hash: tokenHash, type }),
    [sb.auth]
  );

  const updatePassword = useCallback(
    (password: string) => sb.auth.updateUser({ password }),
    [sb.auth]
  );

  const loginWithProvider = useCallback(
    async (provider: Provider) => {
      // Pop OAuth into a sized child window. Opening synchronously in the click
      // handler preserves the user-gesture so popup blockers don't fire; doing
      // it as a popup sidesteps the browsers/extensions that re-target a plain
      // window.location.href on cross-origin nav into a brand-new tab.
      const popup = window.open('about:blank', OAUTH_POPUP_WINDOW_NAME, OAUTH_POPUP_FEATURES);

      const result = await sb.auth.signInWithOAuth({
        provider,
        options: {
          redirectTo: window.location.origin + '/callback',
          skipBrowserRedirect: true,
        },
      });

      const url = result.data?.url;
      if (popup && url) {
        popup.location.href = url;
      } else if (url) {
        // Popup was blocked — fall back to same-tab navigation.
        window.location.href = url;
      } else if (popup) {
        // No auth URL came back — don't strand a blank popup.
        popup.close();
      }
      return result;
    },
    [sb.auth]
  );

  // The popup writes the session cookie then broadcasts here. Manually re-read
  // the session because cookie writes don't trigger storage events the way
  // localStorage would, so the opener's onAuthStateChange stays silent until
  // we ask.
  useEffect(() => {
    if (typeof BroadcastChannel === 'undefined') return;
    const channel = new BroadcastChannel(AUTH_BROADCAST_CHANNEL);
    const onMessage = (event: MessageEvent) => {
      if (event.data?.type === 'oauth-complete') {
        // Adopt the token here too: this is the one sign-in path that reaches
        // us without an auth event, so the cache would otherwise stay empty.
        const adopt = sessionAdopter();
        // A rejection here is survivable and must stay unhandled-free: the
        // popup has already written the cookie, so the next read picks the
        // session up even if this one could not.
        sb.auth.getSession().then(({ data }) => adopt(data.session)).catch(() => {});
      }
    };
    channel.addEventListener('message', onMessage);
    return () => {
      channel.removeEventListener('message', onMessage);
      channel.close();
    };
  }, [sb]);

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
    sendMagicLink,
    sendPasswordReset,
    resendConfirmation,
    verifyEmailOtp,
    updatePassword,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// eslint-disable-next-line react-refresh/only-export-components
export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
