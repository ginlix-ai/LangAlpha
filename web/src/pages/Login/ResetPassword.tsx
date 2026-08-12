import React, { useEffect, useRef, useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import type { EmailOtpType } from '@supabase/supabase-js';
import { useAuth } from '../../contexts/AuthContext';
import { isPlatformMode, APP_ENTRY_PATH } from '../../config/hostMode';
import { authErrorMessage } from '../../lib/authErrors';
import PasswordInput from './PasswordInput';
import PasswordStrength from './PasswordStrength';
import { validatePasswordPair, MIN_PASSWORD_LENGTH } from './passwordRequirements';
import WavesBackground from './WavesBackground';
import './LoginPage.css';

/**
 * Set-new-password form — the landing target of password-recovery email
 * links. Custom email templates link here with `?token_hash=...&type=recovery`
 * (verified below); default templates land with a PKCE `?code=` the client
 * auto-exchanges on load, so by the time auth is initialized the recovery
 * session either exists or never will. Without a session (expired/used
 * link), offers to request a new one.
 */
function ResetPassword() {
  const { isInitialized, isLoggedIn, updatePassword, verifyEmailOtp } = useAuth();
  const navigate = useNavigate();
  const { t } = useTranslation();
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  // 'verified' means the token was accepted and the session announcement is
  // in flight; 'no-token' leaves the verdict to the session state.
  const [linkCheck, setLinkCheck] = useState<'checking' | 'no-token' | 'verified' | 'failed'>(
    'checking'
  );
  const ranRef = useRef(false);

  useEffect(() => {
    if (!isPlatformMode) return;
    // verifyOtp consumes the single-use token — never run it twice (StrictMode).
    if (ranRef.current) return;
    ranRef.current = true;
    const params = new URLSearchParams(window.location.search);
    const tokenHash = params.get('token_hash');
    const type = params.get('type') as EmailOtpType | null;
    if (!tokenHash || !type) {
      setLinkCheck('no-token');
      return;
    }
    (async () => {
      try {
        const result = await verifyEmailOtp(tokenHash, type);
        if (result?.error) throw result.error;
        setLinkCheck('verified');
      } catch {
        setLinkCheck('failed');
      }
    })();
  }, [verifyEmailOtp]);

  if (!isPlatformMode) {
    return <Navigate to={APP_ENTRY_PATH} replace />;
  }

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const validationError = validatePasswordPair(password, confirmPassword, t);
    if (validationError) {
      setError(validationError);
      return;
    }
    setIsSubmitting(true);
    setError(null);
    try {
      const result = await updatePassword(password);
      if (!result) return;
      if (result.error) throw result.error;
      navigate('/dashboard', { replace: true });
    } catch (err: unknown) {
      setError(authErrorMessage(err, t).message);
    } finally {
      setIsSubmitting(false);
    }
  };

  let body: React.ReactNode;
  if (!isInitialized || linkCheck === 'checking' || (linkCheck === 'verified' && !isLoggedIn)) {
    body = <p className="login-page__status-text">{t('common.loading')}</p>;
  } else if (linkCheck === 'failed' || !isLoggedIn) {
    body = (
      <div className="login-page__status">
        <div className="login-page__error">{t('auth.resetExpired')}</div>
        <button
          type="button"
          className="login-page__submit"
          onClick={() => navigate(APP_ENTRY_PATH, { replace: true })}
        >
          {t('auth.requestNewLink')}
        </button>
      </div>
    );
  } else {
    body = (
      <form onSubmit={handleSubmit} className="login-page__form">
        <h2 className="login-page__view-title">{t('auth.setNewPassword')}</h2>
        <div className="login-page__field">
          <label className="login-page__label">{t('auth.newPassword')}</label>
          <PasswordInput
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder={t('auth.choosePassword')}
            className="login-page__input"
            disabled={isSubmitting}
            required
            minLength={MIN_PASSWORD_LENGTH}
            autoComplete="new-password"
          />
          <PasswordStrength password={password} />
        </div>
        <div className="login-page__field">
          <label className="login-page__label">{t('auth.confirmNewPassword')}</label>
          <PasswordInput
            value={confirmPassword}
            onChange={(e) => setConfirmPassword(e.target.value)}
            placeholder={t('auth.choosePassword')}
            className="login-page__input"
            disabled={isSubmitting}
            required
            minLength={MIN_PASSWORD_LENGTH}
            autoComplete="new-password"
          />
        </div>
        {error && <div className="login-page__error">{error}</div>}
        <button type="submit" disabled={isSubmitting} className="login-page__submit">
          {isSubmitting ? t('auth.updatingPassword') : t('auth.updatePassword')}
        </button>
      </form>
    );
  }

  return (
    <div className="login-page">
      <WavesBackground />
      <div className="login-page__card">{body}</div>
    </div>
  );
}

export default ResetPassword;
