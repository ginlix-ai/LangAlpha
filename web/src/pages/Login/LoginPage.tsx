import React, { useEffect, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { Mail, Link2 } from 'lucide-react';
import { Input } from '../../components/ui/input';
import { useTranslation, Trans } from 'react-i18next';
import { useAuth } from '../../contexts/AuthContext';
import { authErrorMessage, type AuthErrorInfo } from '../../lib/authErrors';
import PasswordInput from './PasswordInput';
import PasswordStrength from './PasswordStrength';
import { validatePasswordPair, MIN_PASSWORD_LENGTH } from './passwordRequirements';
import CheckInbox, { type CheckInboxKind } from './CheckInbox';
import AccountRecoveryHint from './AccountRecoveryHint';
import EmailOnlyView from './EmailOnlyView';
import MarketScanlines from './MarketScanlines';
import EdgeGrain from './EdgeGrain';
import './LoginPage.css';

interface LogoIconProps {
  className?: string;
}

function LogoIcon({ className }: LogoIconProps) {
  return (
    <svg className={className} viewBox="0 0 60 60" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <path d="M40.0312 29.6023L49.9852 25.4051C50.5292 25.1758 50.7571 24.5277 50.4765 24.0084L45.6363 15.0496C45.3489 14.5178 44.6591 14.3605 44.1696 14.7153L34.6523 21.6136M40.0312 29.6023L33.933 32.1736C31.7869 33.0785 31.4456 35.9773 33.3229 37.3559L44.168 45.3202C44.6573 45.6795 45.3512 45.5235 45.6397 44.9895L50.5087 35.9776C50.7774 35.4803 50.5808 34.8593 50.0749 34.6072L40.0312 29.6023ZM34.6523 21.6136L30.5854 24.5614C28.7503 25.8916 26.1597 24.7846 25.8525 22.5391L24.1554 10.1356C24.0732 9.53499 24.54 9 25.1461 9H34.7163C35.3048 9 35.766 9.50561 35.7121 10.0916L34.6523 21.6136Z" stroke="currentColor" strokeWidth="3" />
      <path d="M35.282 47L35.6587 50.0175C35.7338 50.6188 35.2611 51.1482 34.6551 51.1413L25.1712 51.034C24.5829 51.0273 24.1274 50.5167 24.1878 49.9315L25.1428 40.668C25.2309 39.8127 24.2701 39.2523 23.5691 39.7501L15.853 45.2293C15.3591 45.58 14.6693 45.4146 14.3882 44.8781L9.68991 35.911C9.41644 35.389 9.65127 34.745 10.1965 34.5215L18.1128 31.2775C18.9026 30.9539 18.9499 29.8532 18.1909 29.4629L17.5 29.1076L10.2106 25.3592C9.70888 25.1012 9.51977 24.4795 9.79284 23.9858L14.7222 15.0745C15.0166 14.5423 15.714 14.3939 16.1995 14.7603L18.5 16.4959" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
    </svg>
  );
}

function GoogleIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 18 18" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M17.64 9.2c0-.637-.057-1.251-.164-1.84H9v3.481h4.844a4.14 4.14 0 01-1.796 2.716v2.259h2.908c1.702-1.567 2.684-3.875 2.684-6.615z" fill="#4285F4"/>
      <path d="M9 18c2.43 0 4.467-.806 5.956-2.18l-2.908-2.259c-.806.54-1.837.86-3.048.86-2.344 0-4.328-1.584-5.036-3.711H.957v2.332A8.997 8.997 0 009 18z" fill="#34A853"/>
      <path d="M3.964 10.71A5.41 5.41 0 013.682 9c0-.593.102-1.17.282-1.71V4.958H.957A8.996 8.996 0 000 9c0 1.452.348 2.827.957 4.042l3.007-2.332z" fill="#FBBC05"/>
      <path d="M9 3.58c1.321 0 2.508.454 3.44 1.345l2.582-2.58C13.463.891 11.426 0 9 0A8.997 8.997 0 00.957 4.958L3.964 7.29C4.672 5.163 6.656 3.58 9 3.58z" fill="#EA4335"/>
    </svg>
  );
}

function GitHubIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
      <path d="M12 0C5.374 0 0 5.373 0 12c0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23A11.509 11.509 0 0112 5.803c1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576C20.566 21.797 24 17.3 24 12c0-6.627-5.373-12-12-12z"/>
    </svg>
  );
}

type LoginView = 'method' | 'login' | 'signup' | 'magic-link' | 'forgot-password' | 'check-inbox';

interface SwitchPromptProps {
  i18nKey: string;
  onSwitch: () => void;
}

/** "No account? / Have an account?" view footer — the Trans slot <1> becomes
    the view-switch button. */
function SwitchPrompt({ i18nKey, onSwitch }: SwitchPromptProps) {
  return (
    <p className="login-page__switch">
      <Trans
        i18nKey={i18nKey}
        components={{
          1: (
            <button type="button" className="login-page__switch-link" onClick={onSwitch} />
          ),
        }}
      />
    </p>
  );
}

/**
 * LoginPage - Split-screen login: auth pane on the left (method picker first,
 * then email/password, magic-link, and password-reset views), market-tape
 * visual on the right. Shown at the app entry URL when the user is not
 * logged in.
 */
function LoginPage() {
  const {
    loginWithEmail,
    signupWithEmail,
    loginWithProvider,
    sendMagicLink,
    sendPasswordReset,
    resendConfirmation,
  } = useAuth();
  const [searchParams] = useSearchParams();
  const [view, setView] = useState<LoginView>(
    searchParams.get('mode') === 'signup' ? 'signup' : 'method'
  );
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [signupEmail, setSignupEmail] = useState('');
  const [signupPassword, setSignupPassword] = useState('');
  const [signupConfirm, setSignupConfirm] = useState('');
  const [signupName, setSignupName] = useState('');
  const [magicEmail, setMagicEmail] = useState('');
  const [forgotEmail, setForgotEmail] = useState('');
  const [sentKind, setSentKind] = useState<CheckInboxKind>('signup');
  const [sentEmail, setSentEmail] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<AuthErrorInfo | null>(null);
  const { t } = useTranslation();
  // Pane is display:none <=900px; skip mounting the rAF canvases there.
  const [visualHidden, setVisualHidden] = useState(() => window.matchMedia('(max-width: 900px)').matches);
  useEffect(() => {
    const mq = window.matchMedia('(max-width: 900px)');
    const onChange = (e: MediaQueryListEvent) => setVisualHidden(e.matches);
    mq.addEventListener('change', onChange);
    return () => mq.removeEventListener('change', onChange);
  }, []);

  // Bumped on every view change; a submit belongs to the epoch it started in,
  // so a response landing after the user left the view is discarded instead of
  // yanking them to check-inbox or painting a stale error on the new view.
  const viewEpochRef = useRef(0);

  const goToView = (next: LoginView) => {
    viewEpochRef.current += 1;
    setError(null);
    setView(next);
  };

  const fail = (err: unknown, fallback: string) => {
    const info = authErrorMessage(err, t);
    setError({ message: info.message || fallback, code: info.code });
  };

  /**
   * Shared submit wrapper for the auth calls: toggles the submitting flag,
   * clears any prior error, throws on a supabase-style `{ error }` result, and
   * routes failures through fail(). `clearError` is off for the inline resend,
   * which fires from inside the visible error box. A response that resolves
   * after the user navigated to another view (epoch mismatch) is dropped.
   */
  async function runSubmit<T>(
    op: () => Promise<T | void>,
    fallback: string,
    onSuccess?: (result: T) => void,
    clearError = true,
  ): Promise<void> {
    const epoch = viewEpochRef.current;
    setIsSubmitting(true);
    if (clearError) setError(null);
    try {
      const result = await op();
      if (viewEpochRef.current !== epoch) return;
      if (!result) return;
      const authError = (result as { error?: unknown }).error;
      if (authError) throw authError;
      onSuccess?.(result);
    } catch (err: unknown) {
      if (viewEpochRef.current === epoch) fail(err, fallback);
    } finally {
      // Always release the flag: at most one submit is in flight (buttons
      // disable while submitting), so a stale release can't race a fresh one.
      setIsSubmitting(false);
    }
  }

  const handleLogin = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    await runSubmit(() => loginWithEmail(loginEmail, loginPassword), 'Login failed');
  };

  const handleSignup = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const validationError = validatePasswordPair(signupPassword, signupConfirm, t);
    if (validationError) {
      setError({ message: validationError, code: null });
      return;
    }
    await runSubmit(
      () => signupWithEmail(signupEmail, signupPassword, signupName),
      'Sign up failed',
      (result) => {
        const { data } = result;
        if (data?.user && !data.session) {
          if (data.user.identities?.length === 0) {
            setError({ message: t('auth.signupEmailExists'), code: 'user_already_exists' });
          } else {
            // Confirmation email sent — make the pending state unmistakable.
            setSentKind('signup');
            setSentEmail(signupEmail);
            goToView('check-inbox');
          }
        }
      },
    );
  };

  /** Sends a magic link and routes to check-inbox. Also wired to the inline
      error actions — it signs in password and OAuth accounts alike. */
  const sendMagicLinkTo = async (email: string) => {
    await runSubmit(() => sendMagicLink(email), 'Failed to send link', () => {
      setSentKind('magic-link');
      setSentEmail(email);
      goToView('check-inbox');
    });
  };

  const handleMagicLink = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    await sendMagicLinkTo(magicEmail);
  };

  const handleForgotPassword = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    await runSubmit(() => sendPasswordReset(forgotEmail), 'Failed to send link', () => {
      setSentKind('reset');
      setSentEmail(forgotEmail);
      goToView('check-inbox');
    });
  };

  /** Inline recovery for "Email not confirmed" errors on the login form. */
  const handleResendConfirmation = async () => {
    // Fires from inside the visible error box, so keep the error on screen
    // (clearError off) — clearing it would unmount this very button.
    await runSubmit(
      () => resendConfirmation(loginEmail),
      'Failed to send email',
      () => {
        setSentKind('signup');
        setSentEmail(loginEmail);
        goToView('check-inbox');
      },
      false,
    );
  };

  const handleOAuth = (provider: 'google' | 'github') =>
    runSubmit(() => loginWithProvider(provider), `${provider} login failed`);

  // kind -> { back-navigation target, resend function } for the check-inbox view.
  const FLOWS: Record<
    CheckInboxKind,
    { backView: LoginView; send: (email: string) => Promise<{ error: unknown } | void> }
  > = {
    signup: { backView: 'signup', send: resendConfirmation },
    'magic-link': { backView: 'magic-link', send: sendMagicLink },
    reset: { backView: 'forgot-password', send: sendPasswordReset },
  };

  const inboxBackView: LoginView = FLOWS[sentKind].backView;

  return (
    <div className="login-page login-page--split">
      <div className="login-page__frame">
      {/* Not gated on visualHidden: the grain is a frame-level texture (visible
          on mobile too) and only animates on ember seeds, which never spawn
          while the tape pane is unmounted. */}
      <EdgeGrain />
      <div className="login-page__auth-pane">
        <div className="login-page__auth-inner">
        <div className="login-page__card-header">
          <LogoIcon className="login-page__logo-icon" />
          <h1 className="login-page__title">LangAlpha</h1>
        </div>

        {view === 'method' && (
          <div className="login-page__method">
            {error && <div className="login-page__error">{error.message}</div>}
            <div className="login-page__method-stack">
              <button
                type="button"
                className="login-page__method-btn login-page__method-btn--primary"
                onClick={() => goToView('login')}
              >
                <Mail size={18} strokeWidth={1.75} aria-hidden="true" />
                <span>{t('auth.continueWithEmail')}</span>
              </button>
              <button
                type="button"
                className="login-page__method-btn"
                onClick={() => handleOAuth('google')}
                disabled={isSubmitting}
              >
                <GoogleIcon />
                <span>{t('auth.continueWithGoogle')}</span>
              </button>
              <button
                type="button"
                className="login-page__method-btn"
                onClick={() => handleOAuth('github')}
                disabled={isSubmitting}
              >
                <GitHubIcon />
                <span>{t('auth.continueWithGithub')}</span>
              </button>
              <button
                type="button"
                className="login-page__method-btn"
                onClick={() => goToView('magic-link')}
              >
                <Link2 size={18} strokeWidth={1.75} aria-hidden="true" />
                <span>{t('auth.magicLinkCta')}</span>
              </button>
            </div>
            <SwitchPrompt i18nKey="auth.noAccount" onSwitch={() => goToView('signup')} />
          </div>
        )}

        {view === 'login' && (
          <form onSubmit={handleLogin} className="login-page__form">
            <div>
              <h2 className="login-page__view-title">{t('auth.loginTitle')}</h2>
            </div>
            <div className="login-page__field">
              <label className="login-page__label">{t('common.email')}</label>
              <Input
                type="email"
                value={loginEmail}
                onChange={(e) => setLoginEmail(e.target.value)}
                placeholder={t('auth.enterEmail')}
                className="login-page__input"
                disabled={isSubmitting}
                autoComplete="email"
                required
              />
            </div>
            <div className="login-page__field">
              <div className="login-page__label-row">
                <label className="login-page__label">{t('common.password')}</label>
                <button
                  type="button"
                  className="login-page__forgot"
                  onClick={() => {
                    setForgotEmail((cur) => cur || loginEmail);
                    goToView('forgot-password');
                  }}
                >
                  {t('auth.forgotPassword')}
                </button>
              </div>
              <PasswordInput
                value={loginPassword}
                onChange={(e) => setLoginPassword(e.target.value)}
                placeholder={t('auth.enterPassword')}
                className="login-page__input"
                disabled={isSubmitting}
                autoComplete="current-password"
                required
              />
            </div>
            {error && (
              <div className="login-page__error">
                {error.message}
                {error.code === 'email_not_confirmed' && (
                  <button
                    type="button"
                    className="login-page__inline-resend"
                    onClick={handleResendConfirmation}
                    disabled={isSubmitting}
                  >
                    {t('auth.resendConfirmationCta')}
                  </button>
                )}
                {error.code === 'invalid_credentials' && (
                  /* Shown for every failed login (Supabase returns the same
                     code whether the password is wrong or the account is
                     OAuth-only, to prevent enumeration), so the hint must
                     read right for both cases. */
                  <AccountRecoveryHint
                    i18nKey="auth.invalidCredentialsHint"
                    onPrimary={() => goToView('method')}
                    onMagic={() => void sendMagicLinkTo(loginEmail)}
                    onReset={() => {
                      setForgotEmail((cur) => cur || loginEmail);
                      goToView('forgot-password');
                    }}
                    disabled={isSubmitting}
                  />
                )}
              </div>
            )}
            <button
              type="submit"
              disabled={isSubmitting}
              className="login-page__submit"
            >
              {isSubmitting ? t('auth.loggingIn') : t('auth.login')}
            </button>
            <SwitchPrompt i18nKey="auth.noAccount" onSwitch={() => goToView('signup')} />
            <button type="button" className="login-page__back" onClick={() => goToView('method')}>
              {t('auth.backToOptions')}
            </button>
          </form>
        )}

        {view === 'signup' && (
          <form onSubmit={handleSignup} className="login-page__form">
            <div>
              <h2 className="login-page__view-title">{t('auth.signupTitle')}</h2>
            </div>
            <div className="login-page__field">
              <label className="login-page__label">{t('auth.name')}</label>
              <Input
                type="text"
                value={signupName}
                onChange={(e) => setSignupName(e.target.value)}
                placeholder={t('auth.enterName')}
                className="login-page__input"
                disabled={isSubmitting}
                required
              />
            </div>
            <div className="login-page__field">
              <label className="login-page__label">{t('common.email')}</label>
              <Input
                type="email"
                value={signupEmail}
                onChange={(e) => setSignupEmail(e.target.value)}
                placeholder={t('auth.enterEmail')}
                className="login-page__input"
                disabled={isSubmitting}
                autoComplete="email"
                required
              />
            </div>
            <div className="login-page__field">
              <label className="login-page__label">{t('common.password')}</label>
              <PasswordInput
                value={signupPassword}
                onChange={(e) => setSignupPassword(e.target.value)}
                placeholder={t('auth.choosePassword')}
                className="login-page__input"
                disabled={isSubmitting}
                autoComplete="new-password"
                required
                minLength={MIN_PASSWORD_LENGTH}
              />
              <PasswordStrength password={signupPassword} />
            </div>
            <div className="login-page__field">
              <label className="login-page__label">{t('auth.confirmPassword')}</label>
              <PasswordInput
                value={signupConfirm}
                onChange={(e) => setSignupConfirm(e.target.value)}
                placeholder={t('auth.confirmPassword')}
                className="login-page__input"
                disabled={isSubmitting}
                autoComplete="new-password"
                required
                minLength={MIN_PASSWORD_LENGTH}
              />
            </div>
            {error && (
              <div className="login-page__error">
                {error.message}
                {error.code === 'user_already_exists' && (
                  /* The existing account may be password- or OAuth-based
                     (Supabase hides which) — offer every route in: password
                     sign-in, a magic link that works regardless, or a reset
                     that also sets a first password on OAuth-only accounts. */
                  <AccountRecoveryHint
                    i18nKey="auth.signupEmailExistsHint"
                    onPrimary={() => {
                      setLoginEmail((cur) => cur || signupEmail);
                      goToView('login');
                    }}
                    onMagic={() => void sendMagicLinkTo(signupEmail)}
                    onReset={() => {
                      setForgotEmail((cur) => cur || signupEmail);
                      goToView('forgot-password');
                    }}
                    disabled={isSubmitting}
                  />
                )}
              </div>
            )}
            <button
              type="submit"
              disabled={isSubmitting}
              className="login-page__submit"
            >
              {isSubmitting ? t('auth.creatingAccount') : t('auth.signup')}
            </button>
            <SwitchPrompt i18nKey="auth.haveAccount" onSwitch={() => goToView('login')} />
            <button type="button" className="login-page__back" onClick={() => goToView('method')}>
              {t('auth.backToOptions')}
            </button>
          </form>
        )}

        {view === 'magic-link' && (
          <EmailOnlyView
            title="auth.magicTitle"
            subtitle="auth.magicSubtitle"
            submitLabel="auth.sendMagicLink"
            sendingLabel="auth.sending"
            backLabel="auth.backToOptions"
            email={magicEmail}
            onEmailChange={setMagicEmail}
            onSubmit={handleMagicLink}
            onBack={() => goToView('method')}
            error={error?.message ?? null}
            isSubmitting={isSubmitting}
          />
        )}

        {view === 'forgot-password' && (
          <EmailOnlyView
            title="auth.forgotTitle"
            subtitle="auth.forgotSubtitle"
            submitLabel="auth.sendResetLink"
            sendingLabel="auth.sending"
            email={forgotEmail}
            onEmailChange={setForgotEmail}
            onSubmit={handleForgotPassword}
            onBack={() => goToView('login')}
            error={error?.message ?? null}
            isSubmitting={isSubmitting}
          />
        )}

        {view === 'check-inbox' && (
          <CheckInbox
            kind={sentKind}
            email={sentEmail}
            onResend={() => FLOWS[sentKind].send(sentEmail)}
            onBack={() => goToView(inboxBackView)}
          />
        )}

        <p className="login-page__legal">
          <Trans
            i18nKey="auth.agreeToTerms"
            components={{ 1: <Link to="/legal" />, 2: <Link to="/privacy" /> }}
          />
        </p>
        </div>
      </div>
      <div className="login-page__visual-pane" aria-hidden="true">
        {!visualHidden && <MarketScanlines />}
        <div className="login-page__visual-copy">
          <h2 className="login-page__visual-title">{t('auth.visualTitle')}</h2>
          <p className="login-page__visual-subtitle">{t('auth.visualSubtitle')}</p>
        </div>
      </div>
      </div>
    </div>
  );
}

export default LoginPage;
