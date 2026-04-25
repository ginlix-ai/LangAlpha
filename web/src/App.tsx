import React, { Suspense, useEffect } from 'react';
import { Routes, Route, Navigate, useNavigate, useLocation } from 'react-router-dom';
import Sidebar from './components/Sidebar/Sidebar';
import BottomTabBar from './components/BottomTabBar/BottomTabBar';
import Main from './components/Main/Main';
import LoginPage from './pages/Login/LoginPage';
import SharedChatView from './pages/SharedChat/SharedChatView';
import { useTranslation } from 'react-i18next';
import { useAuth } from './contexts/AuthContext';
import { useIsMobile } from './hooks/useIsMobile';
import { useSetupGate } from './hooks/useSetupGate';
import './App.css';

const SetupWizard = React.lazy(() => import('./pages/Setup/SetupWizard'));
const PrivacyPolicy = React.lazy(() => import('./pages/Legal/PrivacyPolicy'));
const Legal = React.lazy(() => import('./pages/Legal/Legal'));

/** Handles the OAuth redirect from Supabase — shows a spinner then redirects to /dashboard. */
function AuthCallback() {
  const { isLoggedIn } = useAuth();
  const navigate = useNavigate();
  const { t: tAuth } = useTranslation();

  useEffect(() => {
    if (isLoggedIn) {
      // Check for redirect parameter (e.g., from ginlix-auth account pages)
      const params = new URLSearchParams(window.location.search);
      const redirectTo = params.get('redirect');
      if (redirectTo && (redirectTo.startsWith('/') || redirectTo.startsWith('http'))) {
        window.location.href = redirectTo;
        return;
      }
      navigate('/dashboard', { replace: true });
    }
  }, [isLoggedIn, navigate]);

  return (
    <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{tAuth('auth.signingIn')}</p>
    </div>
  );
}

/** Redirects to dashboard or a ?redirect= target after login. */
function RootRedirect() {
  const params = new URLSearchParams(window.location.search);
  const redirectTo = params.get('redirect');
  if (redirectTo && (redirectTo.startsWith('/') || redirectTo.startsWith('http'))) {
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
  const { t } = useTranslation();

  // While the user profile is loading, show a neutral loading state
  // to avoid flashing protected content before the gate check completes.
  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('common.loading')}</p>
      </div>
    );
  }

  if (needsSetup) {
    return <Navigate to="/setup/method" replace />;
  }

  return (
    <div className="app-layout">
      {!isMobile && <Sidebar />}
      {isMobile && !hideTabBar && <BottomTabBar />}
      <main className={`app-main${hideTabBar ? ' app-main--no-tab' : ''}`}>
        <Main />
      </main>
    </div>
  );
}

function App() {
  const { isLoggedIn, isInitialized } = useAuth();
  const { t } = useTranslation();

  if (!isInitialized) {
    return (
        <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('common.loading')}</p>
      </div>
    );
  }

  return (
    <Routes>
      <Route path="/" element={isLoggedIn ? <RootRedirect /> : <LoginPage />} />
      <Route path="/callback" element={<AuthCallback />} />
      <Route path="/s/:shareToken" element={<SharedChatView />} />
      <Route path="/privacy" element={
        <Suspense fallback={
          <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
            <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('common.loading')}</p>
          </div>
        }>
          <PrivacyPolicy />
        </Suspense>
      } />
      <Route path="/legal" element={
        <Suspense fallback={
          <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
            <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('common.loading')}</p>
          </div>
        }>
          <Legal />
        </Suspense>
      } />
      <Route path="/setup/*" element={
        isLoggedIn ? (
          <Suspense fallback={
            <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
              <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('common.loading')}</p>
            </div>
          }>
            <SetupWizard />
          </Suspense>
        ) : (
          <Navigate to="/" replace />
        )
      } />
      <Route path="/*" element={
        isLoggedIn ? <AuthenticatedShell /> : <Navigate to="/" replace />
      } />
    </Routes>
  );
}

export default App;
