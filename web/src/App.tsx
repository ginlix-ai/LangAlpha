import React, { Suspense, useEffect } from 'react';
import { Routes, Route, Navigate, useNavigate, useLocation } from 'react-router-dom';
import Sidebar from './components/Sidebar/Sidebar';
import BottomTabBar from './components/BottomTabBar/BottomTabBar';
import Main from './components/Main/Main';
import PageLoading from './components/PageLoading/PageLoading';
import AuthConfirm from './pages/Login/AuthConfirm';
import SharedChatView from './pages/SharedChat/SharedChatView';
import { useTranslation } from 'react-i18next';
import { useAuth } from './contexts/AuthContext';
import { useIsMobile } from './hooks/useIsMobile';
import { useSetupGate } from './hooks/useSetupGate';
import { isPlatformMode, APP_ENTRY_PATH } from './config/hostMode';
import { AUTH_BROADCAST_CHANNEL, type AuthBroadcastMessage } from './lib/oauthPopup';
import { OnboardingProvider, OnboardingHostGate } from './pages/Onboarding';
import './App.css';

// Login carries the market-tape canvas subsystem (~2k lines that only a
// logged-out visitor ever renders) — split it out of the main bundle.
const LoginPage = React.lazy(() => import('./pages/Login/LoginPage'));
const SetupWizard = React.lazy(() => import('./pages/Setup/SetupWizard'));
const PrivacyPolicy = React.lazy(() => import('./pages/Legal/PrivacyPolicy'));
const Legal = React.lazy(() => import('./pages/Legal/Legal'));
const ResetPassword = React.lazy(() => import('./pages/Login/ResetPassword'));

/**
 * Handles the OAuth redirect from Supabase. Two modes:
 * - Popup (opened by loginWithProvider): broadcast to the opener and close.
 * - Top-level (fallback when the popup was blocked): navigate to /dashboard.
 */
function AuthCallback() {
  const { isLoggedIn } = useAuth();
  const navigate = useNavigate();
  const { t: tAuth } = useTranslation();

  useEffect(() => {
    if (!isLoggedIn) return;

    const isPopup = typeof window !== 'undefined' && !!window.opener && window.opener !== window;
    if (isPopup) {
      try {
        const channel = new BroadcastChannel(AUTH_BROADCAST_CHANNEL);
        const msg: AuthBroadcastMessage = { type: 'oauth-complete' };
        channel.postMessage(msg);
        channel.close();
      } catch {
        // BroadcastChannel unsupported — the opener will pick up the cookie
        // on its next session check (page focus, navigation, etc.).
      }
      window.close();
      return;
    }

    const params = new URLSearchParams(window.location.search);
    const redirectTo = params.get('redirect');
    if (redirectTo && isSafeRedirect(redirectTo)) {
      window.location.href = redirectTo;
      return;
    }
    navigate('/dashboard', { replace: true });
  }, [isLoggedIn, navigate]);

  return (
    <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{tAuth('auth.signingIn')}</p>
    </div>
  );
}

// Rejects protocol-relative URLs (`//evil.com/x`) and cross-origin absolutes —
// both would let `?redirect=` be weaponized for phishing after OAuth.
function isSafeRedirect(target: string): boolean {
  // Resolving against the current origin normalizes backslash tricks
  // (`/\evil.com` -> `//evil.com`) that a prefix test would admit.
  try {
    return new URL(target, window.location.origin).origin === window.location.origin;
  } catch {
    return false;
  }
}

/** Redirects to dashboard or a ?redirect= target after login. */
function RootRedirect() {
  const params = new URLSearchParams(window.location.search);
  const redirectTo = params.get('redirect');
  if (redirectTo && isSafeRedirect(redirectTo)) {
    window.location.href = redirectTo;
    return null;
  }
  return <Navigate to="/dashboard" replace />;
}

/**
 * Authenticated app shell — sidebar + main content.
 * Redirects to the setup wizard if the user hasn't configured API keys.
 */
function AuthenticatedShell() {
  const isMobile = useIsMobile();
  const location = useLocation();
  const hideTabBar = isMobile && location.pathname.startsWith('/chat/t/');
  const { isLoading, needsSetup } = useSetupGate();

  // While the user profile is loading, show the loading state to avoid
  // flashing protected content before the gate check completes.
  if (isLoading) {
    return <PageLoading />;
  }

  if (needsSetup) {
    return <Navigate to="/setup/method" replace />;
  }

  return (
    <OnboardingProvider>
      <div className="app-layout">
        {!isMobile && <Sidebar />}
        {isMobile && !hideTabBar && <BottomTabBar />}
        <main className={`app-main${hideTabBar ? ' app-main--no-tab' : ''}`}>
          <Main />
        </main>
      </div>
      <OnboardingHostGate />
    </OnboardingProvider>
  );
}

function App() {
  const { isLoggedIn, isInitialized } = useAuth();

  if (!isInitialized) {
    return <PageLoading />;
  }

  const appEntryElement = isLoggedIn ? (
    <RootRedirect />
  ) : (
    <Suspense fallback={<PageLoading />}>
      <LoginPage />
    </Suspense>
  );

  return (
    <Routes>
      {isPlatformMode ? (
        <Route path="/app" element={appEntryElement} />
      ) : (
        <Route path="/" element={appEntryElement} />
      )}
      <Route path="/callback" element={<AuthCallback />} />
      {/* Supabase email-link landing (signup confirm, magic link, recovery).
          Static import so verification starts without a chunk-fetch flash. */}
      <Route path="/auth/confirm" element={<AuthConfirm />} />
      <Route path="/reset-password" element={
        <Suspense fallback={<PageLoading />}>
          <ResetPassword />
        </Suspense>
      } />
      <Route path="/s/:shareToken" element={<SharedChatView />} />
      <Route path="/privacy" element={
        <Suspense fallback={<PageLoading />}>
          <PrivacyPolicy />
        </Suspense>
      } />
      <Route path="/legal" element={
        <Suspense fallback={<PageLoading />}>
          <Legal />
        </Suspense>
      } />
      <Route path="/setup/*" element={
        isLoggedIn ? (
          <Suspense fallback={<PageLoading />}>
            <SetupWizard />
          </Suspense>
        ) : (
          <Navigate to={APP_ENTRY_PATH} replace />
        )
      } />
      <Route path="/*" element={
        isLoggedIn ? <AuthenticatedShell /> : <Navigate to={APP_ENTRY_PATH} replace />
      } />
    </Routes>
  );
}

export default App;
