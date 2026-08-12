import React, { Suspense, useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { MotionConfig } from 'framer-motion';
import { Routes, Route, Navigate, useNavigate, useLocation } from 'react-router-dom';
import AppSidebar from './components/Sidebar/AppSidebar';
import { SIDEBAR_DEFAULT_WIDTH, clampSidebarWidth } from './components/Sidebar/sidebarWidth';
import { useScrollMemory } from './lib/scrollMemory';
import BottomTabBar from './components/BottomTabBar/BottomTabBar';
import Main, { preloadRouteChunk } from './components/Main/Main';
import PageLoading from './components/PageLoading/PageLoading';
import AuthConfirm from './pages/Login/AuthConfirm';
import { useTranslation } from 'react-i18next';
import { useAuth } from './contexts/AuthContext';
import { useIsMobile } from './hooks/useIsMobile';
import { useSetupGate } from './hooks/useSetupGate';
import { isPlatformMode, APP_ENTRY_PATH } from './config/hostMode';
import { AUTH_BROADCAST_CHANNEL, type AuthBroadcastMessage } from './lib/oauthPopup';
import { OnboardingProvider, OnboardingHostGate } from './pages/Onboarding';
import { ThreadLifecycleFeed } from './lib/threadLifecycle/ThreadLifecycleFeed';
import './App.css';

// Login carries the market-tape canvas subsystem (~2k lines that only a
// logged-out visitor ever renders) — split it out of the main bundle.
const LoginPage = React.lazy(() => import('./pages/Login/LoginPage'));
// The public share route reuses the chat transcript renderer, so a static import
// pulled the whole ChatAgent tree — plus the markdown and chart vendors it reaches
// — into the entry chunk that every visitor loads before login.
const SharedChatView = React.lazy(() => import('./pages/SharedChat/SharedChatView'));
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

/** Legacy shared-host entry path — sends stale /app links to the root entry. */
function LegacyAppPathRedirect() {
  const { search } = useLocation();
  return <Navigate to={{ pathname: '/', search }} replace />;
}

/**
 * Authenticated app shell — sidebar + main content.
 * Redirects to the setup wizard if the user hasn't configured API keys.
 */
const SIDEBAR_COLLAPSED_KEY = 'app-sidebar-collapsed';
const SIDEBAR_WIDTH_KEY = 'app-sidebar-width';

function AuthenticatedShell() {
  const isMobile = useIsMobile();
  const location = useLocation();
  const hideTabBar = isMobile && location.pathname.startsWith('/chat/t/');
  const { isLoading, needsSetup } = useSetupGate();

  // Warm the target route's chunk while /users/me resolves — the gate below
  // otherwise serializes two network legs (profile fetch, then the lazy
  // import only starts on first render of Main). import() is deduped, so
  // this is free when the gate is already settled. Mount-only: later
  // navigations mount their lazy component directly.
  useEffect(() => {
    preloadRouteChunk(window.location.pathname);
  }, []);

  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    try {
      return localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === 'true';
    } catch {
      return false;
    }
  });
  const toggleSidebar = useCallback(() => {
    setSidebarCollapsed((prev) => {
      const next = !prev;
      try {
        localStorage.setItem(SIDEBAR_COLLAPSED_KEY, String(next));
      } catch {
        // localStorage unavailable (private mode) — collapse still works for the session
      }
      return next;
    });
  }, []);

  // Per-route scroll memory for the shared content scroller: leaving a tab and
  // coming back restores where the user was (0 for first visits, so positions
  // never bleed between routes that share this container).
  const mainRef = useRef<HTMLElement>(null);
  useScrollMemory(mainRef, `route:${location.pathname}`);

  const [sidebarWidth, setSidebarWidth] = useState(() => {
    try {
      const stored = Number(localStorage.getItem(SIDEBAR_WIDTH_KEY));
      return Number.isFinite(stored) && stored > 0 ? clampSidebarWidth(stored) : SIDEBAR_DEFAULT_WIDTH;
    } catch {
      return SIDEBAR_DEFAULT_WIDTH;
    }
  });
  // Live drag updates write --sidebar-width straight to the DOM (no re-render
  // per pointermove — the whole route tree lives under this shell); React state
  // and localStorage only sync on commit (drag end / double-click reset).
  const handleSidebarWidthChange = useCallback((width: number, commit = false) => {
    // Written on every call, commit or not: when the committed width equals the
    // current state React bails out of the render, so the effect below never
    // re-runs and the DOM would keep whatever the last pointermove left behind.
    document.documentElement.style.setProperty('--sidebar-width', `${width}px`);
    if (!commit) return;
    setSidebarWidth(width);
    try {
      localStorage.setItem(SIDEBAR_WIDTH_KEY, String(width));
    } catch {
      // localStorage unavailable — width still applies for the session
    }
  }, []);
  // Published on the document root, not on .app-layout: custom properties only
  // inherit downward, and every fixed overlay that has to dodge the sidebar
  // (the getting-started card, the dashboard's floating chat and edit toolbar)
  // is viewport-anchored — some of them portalled clean out of the layout
  // subtree. Off the root they'd read the collapsed default from tokens.css and
  // sit under the sidebar. 0 on mobile, where no sidebar renders at all.
  const sidebarWidthVar = isMobile ? '0px' : sidebarCollapsed ? '80px' : `${sidebarWidth}px`;
  useLayoutEffect(() => {
    const root = document.documentElement;
    root.style.setProperty('--sidebar-width', sidebarWidthVar);
    return () => {
      root.style.removeProperty('--sidebar-width');
    };
  }, [sidebarWidthVar]);

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
      <ThreadLifecycleFeed />
      <div className="app-layout">
        {!isMobile && (
          <AppSidebar
            collapsed={sidebarCollapsed}
            onToggleCollapse={toggleSidebar}
            width={sidebarWidth}
            onWidthChange={handleSidebarWidthChange}
          />
        )}
        {isMobile && !hideTabBar && <BottomTabBar />}
        <main ref={mainRef} className={`app-main${hideTabBar ? ' app-main--no-tab' : ''}`}>
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
    // reducedMotion="user": every framer-motion transform/layout animation
    // app-wide collapses to instant for prefers-reduced-motion users (opacity
    // still animates) — no per-component wiring.
    <MotionConfig reducedMotion="user">
    <Routes>
      <Route path={APP_ENTRY_PATH} element={appEntryElement} />
      {isPlatformMode && APP_ENTRY_PATH === '/' && (
        <Route path="/app" element={<LegacyAppPathRedirect />} />
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
      <Route path="/s/:shareToken" element={
        <Suspense fallback={<PageLoading />}>
          <SharedChatView />
        </Suspense>
      } />
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
    </MotionConfig>
  );
}

export default App;
