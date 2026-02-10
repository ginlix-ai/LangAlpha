import React, { useState } from 'react';
import { Input } from '../../components/ui/input';
import { useAuth } from '../../contexts/AuthContext';
import WavesBackground from './WavesBackground';
import './LoginPage.css';

function LogoIcon({ className }) {
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

/**
 * LoginPage - Full-page login and signup with email+password and OAuth providers.
 * Shown at root URL (/) when user is not logged in.
 */
function LoginPage() {
  const { loginWithEmail, signupWithEmail, loginWithProvider } = useAuth();
  const [mode, setMode] = useState('login'); // 'login' | 'signup'
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [signupEmail, setSignupEmail] = useState('');
  const [signupPassword, setSignupPassword] = useState('');
  const [signupName, setSignupName] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState(null);

  const handleLogin = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    try {
      const { error: authError } = await loginWithEmail(loginEmail, loginPassword);
      if (authError) throw authError;
    } catch (err) {
      setError(err?.message || 'Login failed');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleSignup = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    try {
      const { error: authError } = await signupWithEmail(signupEmail, signupPassword, signupName);
      if (authError) throw authError;
    } catch (err) {
      setError(err?.message || 'Sign up failed');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleOAuth = async (provider) => {
    setError(null);
    try {
      const { error: authError } = await loginWithProvider(provider);
      if (authError) throw authError;
    } catch (err) {
      setError(err?.message || `${provider} login failed`);
    }
  };

  return (
    <div className="login-page">
      <WavesBackground />
      <div className="login-page__card">
        <div className="login-page__card-header">
          <LogoIcon className="login-page__logo-icon" />
          <div>
            <h1 className="login-page__title">LangAlpha</h1>
            <p className="login-page__subtitle">Welcome</p>
          </div>
        </div>

        <div className="login-page__tabs">
          <button
            type="button"
            onClick={() => { setMode('login'); setError(null); }}
            className="login-page__tab"
            data-active={mode === 'login'}
          >
            Login
          </button>
          <button
            type="button"
            onClick={() => { setMode('signup'); setError(null); }}
            className="login-page__tab"
            data-active={mode === 'signup'}
          >
            Sign up
          </button>
        </div>

        {mode === 'login' && (
          <form onSubmit={handleLogin} className="login-page__form">
            <div className="login-page__field">
              <label className="login-page__label">Email</label>
              <Input
                type="email"
                value={loginEmail}
                onChange={(e) => setLoginEmail(e.target.value)}
                placeholder="Enter your email"
                className="login-page__input"
                disabled={isSubmitting}
                required
              />
            </div>
            <div className="login-page__field">
              <label className="login-page__label">Password</label>
              <Input
                type="password"
                value={loginPassword}
                onChange={(e) => setLoginPassword(e.target.value)}
                placeholder="Enter your password"
                className="login-page__input"
                disabled={isSubmitting}
                required
              />
            </div>
            {error && <div className="login-page__error">{error}</div>}
            <button
              type="submit"
              disabled={isSubmitting}
              className="login-page__submit"
            >
              {isSubmitting ? 'Logging in...' : 'Login'}
            </button>
          </form>
        )}

        {mode === 'signup' && (
          <form onSubmit={handleSignup} className="login-page__form">
            <div className="login-page__field">
              <label className="login-page__label">Name</label>
              <Input
                type="text"
                value={signupName}
                onChange={(e) => setSignupName(e.target.value)}
                placeholder="Enter your name"
                className="login-page__input"
                disabled={isSubmitting}
                required
              />
            </div>
            <div className="login-page__field">
              <label className="login-page__label">Email</label>
              <Input
                type="email"
                value={signupEmail}
                onChange={(e) => setSignupEmail(e.target.value)}
                placeholder="Enter your email"
                className="login-page__input"
                disabled={isSubmitting}
                required
              />
            </div>
            <div className="login-page__field">
              <label className="login-page__label">Password</label>
              <Input
                type="password"
                value={signupPassword}
                onChange={(e) => setSignupPassword(e.target.value)}
                placeholder="Choose a password"
                className="login-page__input"
                disabled={isSubmitting}
                required
                minLength={6}
              />
            </div>
            {error && <div className="login-page__error">{error}</div>}
            <button
              type="submit"
              disabled={isSubmitting}
              className="login-page__submit"
            >
              {isSubmitting ? 'Creating account...' : 'Sign up'}
            </button>
          </form>
        )}

        <div className="login-page__divider">
          <span className="login-page__divider-text">or continue with</span>
        </div>

        <div className="login-page__oauth-buttons">
          <button
            type="button"
            className="login-page__oauth-btn"
            onClick={() => handleOAuth('google')}
            disabled={isSubmitting}
          >
            <GoogleIcon />
            <span>Google</span>
          </button>
          <button
            type="button"
            className="login-page__oauth-btn"
            onClick={() => handleOAuth('github')}
            disabled={isSubmitting}
          >
            <GitHubIcon />
            <span>GitHub</span>
          </button>
        </div>
      </div>
    </div>
  );
}

export default LoginPage;
