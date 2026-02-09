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

/**
 * LoginPage - Full-page login and signup
 * Shown at root URL (/) when user is not logged in.
 * Uses same auth API as UserConfigPanel (login, signup from AuthContext).
 */
function LoginPage() {
  const { login, signup } = useAuth();
  const [mode, setMode] = useState('login'); // 'login' | 'signup'
  const [loginEmail, setLoginEmail] = useState('');
  const [signupEmail, setSignupEmail] = useState('');
  const [signupName, setSignupName] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState(null);

  const handleLogin = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    try {
      await login(loginEmail);
      setLoginEmail('');
    } catch (err) {
      const status = err?.response?.status;
      if (status === 404) {
        setError('No account found with this email. Please sign up.');
      } else {
        setError(err?.response?.data?.detail || err.message || 'Login failed');
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleSignup = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setError(null);
    try {
      await signup(signupEmail, signupName);
      setSignupEmail('');
      setSignupName('');
    } catch (err) {
      const status = err?.response?.status;
      if (status === 409) {
        setError('An account with this email already exists. Please login.');
      } else {
        setError(err?.response?.data?.detail || err.message || 'Sign up failed');
      }
    } finally {
      setIsSubmitting(false);
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
      </div>
    </div>
  );
}

export default LoginPage;
