import React, { Suspense, useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { I18nProvider } from 'react-aria-components';
import { Routes, Route, Navigate, useNavigate, useLocation } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
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
import { useLocale } from './hooks/useLocale';
import { useSetupGate } from './hooks/useSetupGate';
import { isPlatformMode, APP_ENTRY_PATH } from './config/hostMode';
import { PRIVACY_URL } from './config/supportLinks';
import { AUTH_BROADCAST_CHANNEL, type AuthBroadcastMessage } from './lib/oauthPopup';
import { inAppPath } from './lib/inAppPath';
import { clearSetupReturn } from './lib/setupReturn';
import { OnboardingProvider, OnboardingHostGate } from './pages/Onboarding';
import { WELCOME_PATH } from './pages/Onboarding/welcome/paths';
import { StaleBuildBoundary } from './components/StaleBuildBoundary';
import { ConnectConsentHost } from './pages/Plugins/components/ConnectConsentHost';
import { ConnectLandingHost } from './pages/Plugins/components/ConnectLandingHost';
import { ThreadLifecycleFeed } from './lib/threadLifecycle/ThreadLifecycleFeed';
import { WarmHome } from './components/WarmHome';
import { HostedSignInGate } from './components/HostedSignInGate';
import { forgetHostedSignIn, hostedSignInReturn, landAfterSignIn } from './lib/hostedSignIn';
import { markBooted, reloadIfStale, watchStaleBuild } from './lib/staleBuild';
import { appSectionOf } from './lib/appRoutes';
import { lazyWithMotion } from './lib/lazyWithMotion';
import './App.css';

// Login carries the market-tape canvas subsystem (~2k lines that only a
// logged-out visitor ever renders) — split it out of the main bundle.
// Login, reset and the legal pages animate nothing, so they stay on plain
// React.lazy: lazyWithMotion would fetch framer before the logged-out first
// screen could render. One that gains motion moves to lazyWithMotion.
const LoginPage = React.lazy(() => import('./pages/Login/LoginPage'));
// The public share route reuses the chat transcript renderer, so a static import
// pulled the whole ChatAgent tree — plus the markdown and chart vendors it reaches
// — into the entry chunk that every visitor loads before login.
const SharePage = lazyWithMotion(() => import('./pages/SharedChat/SharePage'));
const SetupWizard = lazyWithMotion(() => import('./pages/Setup/SetupWizard'));
// The welcome flow renders in place of the whole app layout, so it is the
// shell's to load rather than one of Main's routes.
const Welcome = lazyWithMotion(() => import('./pages/Onboarding/welcome/Welcome'));
const Legal = React.lazy(() => import('./pages/Legal/Legal'));
const ResetPassword = React.lazy(() => import('./pages/Login/ResetPassword'));

/** How long a callback may sit holding neither a session nor a stated reason
 *  before it is called a failure. What it waits on is the `?code=` exchange the
 *  Supabase client runs on load, which is the same wait AuthConfirm bounds. */
const CALLBACK_TIMEOUT_MS = 8000;

/**
 * The failure a callback arrived carrying, or null for none. Two writers put it
 * in two places: Supabase reports a denied or expired authorization in the query
 * under PKCE and in the hash under implicit, and the desktop shell writes its own
 * prose into the query when it declines a flow it cannot finish (no free loopback
 * port, a timeout, a second sign-in superseding this one) — this route is the
 * only channel it has for saying so. Read during render, not from an effect: the
 * Supabase client strips the URL as soon as it has consumed it.
 */
function callbackFailure(): string | null {
  const query = new URLSearchParams(window.location.search);
  const hash = new URLSearchParams(window.location.hash.slice(1));
  const read = (key: string) => query.get(key) || hash.get(key);
  return read('error_description') || read('error');
}

/**
 * Handles the OAuth redirect from Supabase. Three modes:
 * - Popup (opened by loginWithProvider): broadcast to the opener and close.
 * - Top-level (fallback when the popup was blocked): navigate to /dashboard.
 * - Failed: say why, and offer the way back. Waiting only on a session leaves
 *   every flow that ends without one behind a message promising a sign-in that
 *   is not coming, and no way out of the page but quitting.
 */
function AuthCallback() {
  const { isLoggedIn } = useAuth();
  const navigate = useNavigate();
  const { t: tAuth } = useTranslation();
  // Two failures with different standing. A stated one is somebody's answer --
  // the provider denied it, or the shell could not finish it -- and it holds
  // even if a session from an earlier sign-in is sitting in the cookie jar.
  const [stated] = useState<string | null>(callbackFailure);
  // A timeout is only us giving up on the wait. Nobody said no, so a session
  // that lands on a slow connection after the deadline still wins the page;
  // latching this into `stated` is what used to strand it behind an error.
  const [timedOut, setTimedOut] = useState(false);
  const failure = stated ?? (timedOut ? tAuth('auth.errors.generic') : null);

  // Whether this document is the child window loginWithProvider opened. The
  // failure UI needs it too, so it is read here rather than inside the effect:
  // in a popup there is no back to go to, the page it would go back to is in
  // the opener and still on screen.
  const isPopup = typeof window !== 'undefined' && !!window.opener && window.opener !== window;

  useEffect(() => {
    if (stated) return;
    if (!isLoggedIn) {
      // No session and nobody said why. Whatever dropped the flow is not going
      // to report it later, so bound the wait instead of spinning on it.
      const deadline = setTimeout(() => setTimedOut(true), CALLBACK_TIMEOUT_MS);
      return () => clearTimeout(deadline);
    }

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

    const redirectTo = inAppPath(new URLSearchParams(window.location.search).get('redirect'));
    if (redirectTo) {
      window.location.href = redirectTo;
      return;
    }
    landAfterSignIn(navigate);
  }, [stated, isPopup, isLoggedIn, navigate]);

  if (failure) {
    return (
      // A timeout swaps this in eight seconds after the page settled on
      // "Signing you in", with nothing on screen changing that a screen reader
      // would otherwise report. `alert` is what makes the swap audible, and it
      // carries the button's label with it, so the way out is announced too.
      <div
        role="alert"
        className="flex flex-col items-center justify-center gap-5 min-h-screen px-6 text-center"
        style={{ backgroundColor: 'var(--color-bg-page)' }}
      >
        <p className="text-sm max-w-sm" style={{ color: 'var(--color-text-secondary)' }}>{failure}</p>
        <button
          type="button"
          className="text-sm rounded-md px-3 py-1.5"
          style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-default)' }}
          onClick={() => {
            // Navigating a popup would leave a second login page stranded in a
            // window the user still has to find and close, while the real one
            // waits untouched in the opener. Hand the window back instead --
            // the opener holds no pending state, so it needs no telling.
            if (isPopup) window.close();
            else navigate(APP_ENTRY_PATH, { replace: true });
          }}
        >
          {isPopup ? tAuth('common.close') : tAuth('auth.backToLogin')}
        </button>
      </div>
    );
  }

  return (
    <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{tAuth('auth.signingIn')}</p>
    </div>
  );
}

/**
 * The shell's one delivery route, which is why an email token can land on it.
 *
 * `deeplink.toAppUrl` resolves every `langalpha://` URL onto `/callback` and
 * copies only the query across, so a confirmation handed back from the browser
 * arrives here rather than at the page that owns email tokens. Forward it on
 * unread: verifying it in two places would be two ways to consume one
 * single-use token.
 */
function CallbackRoute() {
  const { search } = useLocation();
  const params = new URLSearchParams(search);
  if (params.get('token_hash') && params.get('type')) {
    return <Navigate to={`/auth/confirm${search}`} replace />;
  }
  return <AuthCallback />;
}

/**
 * Redirects to dashboard or a ?redirect= target after login. A desktop window
 * that signed in on the portal comes back here without the query, so the target
 * it left with stands in. Read once: `App` clears that note once signed in.
 */
function RootRedirect() {
  const [redirectTo] = useState(() =>
    inAppPath(new URLSearchParams(window.location.search).get('redirect') ?? hostedSignInReturn())
  );
  if (redirectTo) {
    window.location.href = redirectTo;
    return null;
  }
  return <Navigate to="/dashboard" replace />;
}

/** Leaves the app for an address outside it, without a history entry to come back to. */
function ExternalRedirect({ to }: { to: string }) {
  window.location.replace(to);
  return null;
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
/** Route state that only saves the page a fetch (the sidebar's workspace name and status). */
const ROUTE_HINTS = new Set(['workspaceName', 'workspaceStatus']);
/** A thread the URL names finds its workspace again after a load; a new one does not. */
const NAMED_THREAD = /^\/chat\/t\/(?!__default__(?:\/|$))[^/]+/;

function AuthenticatedShell() {
  const isMobile = useIsMobile();
  const location = useLocation();
  const onWelcome = location.pathname === WELCOME_PATH;
  const hideTabBar = isMobile && location.pathname.startsWith('/chat/t/');
  const { isLoading, needsSetup } = useSetupGate();

  // A new build is taken on a switch between sections, never inside one: chat
  // keeps every thread it has opened mounted, with its unsent message, while
  // its threads are switched, and only leaving the section lets them go. Not
  // on a switch that hands something over in its route state (a message the
  // dashboard sends into chat), which the page it opens takes and clears. And
  // a macrotask later, once the writes in flight have landed: the page being
  // left saves what it still holds as it unmounts (a dashboard edit on its
  // debounce), and by then that write has started. The timer is not cancelled
  // when the page that opens rewrites its own route state: the switch was
  // decided on the state it arrived with.
  const queryClient = useQueryClient();
  const section = appSectionOf(location.pathname);
  // An empty one hands nothing over (a thread opened from Automations), and
  // nor do hints the page that opens reads again after a load.
  const namesThread = NAMED_THREAD.test(location.pathname);
  const handsOver =
    location.state != null &&
    Object.entries(location.state as object).some(
      ([key, v]) => v !== undefined && !ROUTE_HINTS.has(key) && !(key === 'workspaceId' && namesThread),
    );
  const shownSection = useRef(section);
  useEffect(() => {
    if (section === shownSection.current) return;
    const left = shownSection.current;
    shownSection.current = section;
    if (!left || !section || handsOver) return;
    setTimeout(() => reloadIfStale(() => queryClient.isMutating() === 0), 0);
  }, [section, handsOver, queryClient]);

  // Reaching any page outside setup ends a visit another page sent here, so
  // its return can't catch a later setup opened from inside the app.
  useEffect(() => {
    clearSetupReturn();
  }, []);

  // Warm the target route's chunk while /users/me resolves — the gate below
  // otherwise serializes two network legs (profile fetch, then the lazy
  // import only starts on first render of Main). import() is deduped, so
  // this is free when the gate is already settled. Mount-only: later
  // navigations mount their lazy component directly.
  useEffect(() => {
    if (window.location.pathname === WELCOME_PATH) void Welcome.preload().catch(() => {});
    else preloadRouteChunk(window.location.pathname);
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
  // (the dashboard's floating chat and edit toolbar)
  // is viewport-anchored — some of them portalled clean out of the layout
  // subtree. Off the root they'd read the collapsed default from tokens.css and
  // sit under the sidebar. 0 on mobile, where no sidebar renders at all.
  const sidebarWidthVar = isMobile ? '0px' : sidebarCollapsed ? 'var(--sidebar-rail-width)' : `${sidebarWidth}px`;
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
      <WarmHome />
      {onWelcome ? (
        // Full screen and none of the app's chrome: the flow is the page.
        // The hosts below stay, since a brokerage connect it starts asks for
        // consent and comes back through them.
        <Suspense fallback={<PageLoading />}>
          <StaleBuildBoundary>
            <Welcome />
          </StaleBuildBoundary>
        </Suspense>
      ) : (
        <>
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
        </>
      )}
      <ConnectConsentHost />
      <ConnectLandingHost />
    </OnboardingProvider>
  );
}

/** The app's language for every react-aria control (month and weekday names,
 *  typeahead order, the controls' own labels) rather than the browser's. A
 *  component of its own, so a language switch re-renders only what reads it. */
function AriaLocale({ children }: { children: React.ReactNode }) {
  const locale = useLocale();
  return <I18nProvider locale={locale}>{children}</I18nProvider>;
}

function App() {
  const { isLoggedIn, isInitialized } = useAuth();

  // App, not AuthenticatedShell: this has to cover the logged-out routes too,
  // and a stale /login chunk is just as fatal as a stale /dashboard one. Mount
  // here means React rendered, so index.html must stop auto-reloading and start
  // publishing instead — a reload from this point on would discard an agent turn
  // or a half-written message.
  useEffect(() => {
    markBooted();
    return watchStaleBuild();
  }, []);

  // Once initialized, so `RootRedirect` has read the page the trip was for: a
  // session can land while the page is still loading.
  useEffect(() => {
    if (isInitialized && isLoggedIn) forgetHostedSignIn();
  }, [isInitialized, isLoggedIn]);

  if (!isInitialized) {
    return <PageLoading />;
  }

  const appEntryElement = isLoggedIn ? (
    <RootRedirect />
  ) : (
    <HostedSignInGate>
      <Suspense fallback={<PageLoading />}>
        <LoginPage />
      </Suspense>
    </HostedSignInGate>
  );

  return (
    <AriaLocale>
    <Routes>
      <Route path={APP_ENTRY_PATH} element={appEntryElement} />
      {isPlatformMode && APP_ENTRY_PATH === '/' && (
        <Route path="/app" element={<LegacyAppPathRedirect />} />
      )}
      <Route path="/callback" element={<CallbackRoute />} />
      {/* Supabase email-link landing (signup confirm, magic link, recovery).
          Static import so verification starts without a chunk-fetch flash.
          The trailing segment names the desktop edition a link came from, since
          `withShellReturn` marks the path: the email template appends its own
          `?`, leaving no room for a query marker. Taken as a parameter rather
          than one route per edition, because `desktopAuthHandoff` is what
          decides which segments mean anything — an unknown one just confirms
          here, like the bare path. */}
      <Route path="/auth/confirm" element={<AuthConfirm />} />
      <Route path="/auth/confirm/:shell" element={<AuthConfirm />} />
      <Route path="/reset-password" element={
        <Suspense fallback={<PageLoading />}>
          <ResetPassword />
        </Suspense>
      } />
      <Route path="/reset-password/:shell" element={
        <Suspense fallback={<PageLoading />}>
          <ResetPassword />
        </Suspense>
      } />
      {/* Chats share under /s/, files and apps live under /a/. The codes are
          one namespace, so either prefix opens any of them. */}
      <Route path="/s/:shareToken" element={
        <Suspense fallback={<PageLoading />}>
          <SharePage />
        </Suspense>
      } />
      <Route path="/a/:shareToken" element={
        <Suspense fallback={<PageLoading />}>
          <SharePage />
        </Suspense>
      } />
      {/* The policy is published at its own address; links made to this path
          still reach it. */}
      {PRIVACY_URL && <Route path="/privacy" element={<ExternalRedirect to={PRIVACY_URL} />} />}
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
    </AriaLocale>
  );
}

export default App;
