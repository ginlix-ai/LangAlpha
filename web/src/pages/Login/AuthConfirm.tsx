import React, { useEffect, useRef, useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import type { EmailOtpType } from '@supabase/supabase-js';
import { useAuth } from '../../contexts/AuthContext';
import { isPlatformMode, APP_ENTRY_PATH } from '../../config/hostMode';
import { AUTH_BROADCAST_CHANNEL, type AuthBroadcastMessage } from '../../lib/oauthPopup';
import { authErrorMessage } from '../../lib/authErrors';
import WavesBackground from './WavesBackground';
import './LoginPage.css';

/** How long to wait for the client's automatic `?code=` exchange to land. */
const PKCE_FALLBACK_TIMEOUT_MS = 8000;

/** Let any still-open login tab pick up the fresh session cookie (cookie
 * writes don't fire events). */
function broadcastAuthComplete() {
  try {
    const channel = new BroadcastChannel(AUTH_BROADCAST_CHANNEL);
    const msg: AuthBroadcastMessage = { type: 'oauth-complete' };
    channel.postMessage(msg);
    channel.close();
  } catch {
    // BroadcastChannel unsupported — other tabs catch up on next check.
  }
}

/**
 * Landing target for Supabase email links (signup confirmation, magic link;
 * password-recovery links land on /reset-password directly, though the
 * type=recovery branch below still routes one that ends up here). The email
 * templates link here with `?token_hash=...&type=...`; verifying the hash
 * establishes the session — this works even when the link is opened in a
 * different browser than the one that started the flow (unlike the PKCE
 * `?code=` exchange).
 */
function AuthConfirm() {
  const { verifyEmailOtp, isLoggedIn } = useAuth();
  const navigate = useNavigate();
  const { t } = useTranslation();
  const [status, setStatus] = useState<'verifying' | 'confirmed' | 'error'>('verifying');
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [pkceFallback, setPkceFallback] = useState(false);
  const ranRef = useRef(false);

  useEffect(() => {
    if (!isPlatformMode) return;
    // verifyOtp consumes the single-use token — never run it twice (StrictMode).
    if (ranRef.current) return;
    ranRef.current = true;

    let redirectTimer: ReturnType<typeof setTimeout> | undefined;

    const params = new URLSearchParams(window.location.search);
    const tokenHash = params.get('token_hash');
    const type = params.get('type') as EmailOtpType | null;
    if (!tokenHash || !type) {
      // The verify endpoint reports rejected links (used/expired) via error
      // params in the query or the hash fragment — fail those fast.
      const hashParams = new URLSearchParams(window.location.hash.slice(1));
      const hasErrorParams = ['error', 'error_code'].some(
        (k) => params.get(k) || hashParams.get(k)
      );
      if (hasErrorParams) {
        setStatus('error');
        return;
      }
      // Half a token pair is a truncated or mangled link — no exchange can be
      // in flight for it, so fail fast instead of waiting on (or falsely
      // confirming from) an unrelated existing session.
      if (tokenHash || type) {
        setStatus('error');
        return;
      }
      // Default (un-customized) Supabase email templates land here with a
      // PKCE `?code=` instead of a token hash. The browser client exchanges
      // that code automatically on load when the flow started in this
      // browser — and when that exchange wins the race against this effect
      // it has already stripped the query from the URL, so a bare landing
      // can still be a sign-in in flight. Wait for the session in both
      // cases instead of rejecting a valid link.
      setPkceFallback(true);
      return;
    }

    (async () => {
      try {
        const result = await verifyEmailOtp(tokenHash, type);
        if (!result) {
          // Stubbed auth (shouldn't be reachable here) — fail visibly rather
          // than spin forever.
          setStatus('error');
          return;
        }
        if (result.error) throw result.error;

        if (type === 'recovery') {
          // Recovery session: go straight to the set-new-password form. The
          // broadcast is skipped only to avoid actively waking other tabs —
          // the session cookie is shared regardless, so other tabs still pick
          // the recovery session up on their next check. Not an isolation
          // boundary.
          navigate('/reset-password', { replace: true });
          return;
        }

        // Confirmed signup / magic link.
        broadcastAuthComplete();
        setStatus('confirmed');
        redirectTimer = setTimeout(() => navigate('/dashboard', { replace: true }), 1500);
      } catch (err: unknown) {
        setErrorMessage(authErrorMessage(err, t).message);
        setStatus('error');
      }
    })();

    // Clear the pending success redirect on unmount/back so a stale navigate
    // can't hijack a later route.
    return () => {
      if (redirectTimer) clearTimeout(redirectTimer);
    };
  }, [verifyEmailOtp, navigate, t]);

  // PKCE fallback: succeed as soon as the auto-exchanged session lands; if it
  // never does (link opened in a different browser, expired code), fail after
  // a bounded wait instead of spinning forever.
  useEffect(() => {
    if (!pkceFallback) return;
    if (!isLoggedIn) {
      const deadline = setTimeout(() => setStatus('error'), PKCE_FALLBACK_TIMEOUT_MS);
      return () => clearTimeout(deadline);
    }
    broadcastAuthComplete();
    setStatus('confirmed');
    const timer = setTimeout(() => navigate('/dashboard', { replace: true }), 1500);
    return () => clearTimeout(timer);
  }, [pkceFallback, isLoggedIn, navigate]);

  if (!isPlatformMode) {
    return <Navigate to={APP_ENTRY_PATH} replace />;
  }

  return (
    <div className="login-page">
      <WavesBackground />
      <div className="login-page__card">
        {status === 'error' ? (
          <div className="login-page__status">
            <div className="login-page__error">{errorMessage || t('auth.confirmError')}</div>
            <button
              type="button"
              className="login-page__submit"
              onClick={() => navigate(APP_ENTRY_PATH, { replace: true })}
            >
              {t('auth.requestNewLink')}
            </button>
          </div>
        ) : (
          <p className="login-page__status-text">
            {status === 'confirmed' ? t('auth.confirmedSigningIn') : t('auth.confirming')}
          </p>
        )}
      </div>
    </div>
  );
}

export default AuthConfirm;
