import React, { Suspense } from 'react';
import { Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import PageLoading from '@/components/PageLoading/PageLoading';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useSyncUserLocale } from '@/hooks/useSyncUserLocale';
import { ContextOverflowPill } from '@/components/ui/ContextOverflowPill';
import NetworkBanner from '@/components/NetworkBanner/NetworkBanner';
import { StaleBuildBoundary } from '@/components/StaleBuildBoundary';

// Chunk thunks shared by the lazy components and preloadRouteChunk — import()
// is deduped by the module system, so a preload and the lazy mount share one
// network fetch.
const routeChunks = {
  dashboard: () => import('../../pages/Dashboard/DashboardRouter'),
  chat: () => import('../../pages/ChatAgent/ChatAgent'),
  market: () => import('../../pages/MarketView/MarketView'),
  news: () => import('../../pages/Detail/NewsDetailPage'),
  automations: () => import('../../pages/Automations/Automations'),
  plugins: () => import('../../pages/Plugins/Plugins'),
  settings: () => import('../../pages/Settings/Settings'),
  // Alias so preloading /connectors (the legacy path) warms the right chunk.
  connectors: () => import('../../pages/Plugins/Plugins'),
};

const Dashboard = React.lazy(routeChunks.dashboard);
const ChatAgent = React.lazy(routeChunks.chat);
const MarketView = React.lazy(routeChunks.market);
const NewsDetailPage = React.lazy(routeChunks.news);
const Automations = React.lazy(routeChunks.automations);
const Plugins = React.lazy(routeChunks.plugins);
const Settings = React.lazy(routeChunks.settings);

/** Start downloading the chunk for `pathname` without rendering it, so the
 * shell can warm the target route while the /users/me gate is still
 * resolving instead of serializing the two network legs. Unknown segments
 * warm the dashboard chunk (the catch-all redirect's target). */
export function preloadRouteChunk(pathname: string): void {
  const chunkFor: Record<string, () => Promise<unknown>> = routeChunks;
  const segment = pathname.split('/')[1] || 'dashboard';
  // Swallowed on purpose, but it must be caught: a deploy deletes the previous
  // build's chunks, so this rejects routinely for a stale tab, and an unhandled
  // rejection is noise that hides real ones. The failure still surfaces — Vite
  // fires vite:preloadError (index.html reports it), and React.lazy retries the
  // same import at mount, where StaleBuildBoundary catches it.
  void (chunkFor[segment] ?? routeChunks.dashboard)().catch(() => {});
}

/** Permanent alias for the pre-rename Connectors page. Keeps search + hash:
 * OAuth `return_to` values persisted before the rename (Redis ConnectState,
 * 600s TTL) still land on `/connectors?mcp_connected=…` and must reach the
 * page that renders the toast. */
export function LegacyConnectorsRedirect() {
  const { search, hash } = useLocation();
  return <Navigate to={`/plugins${search}${hash}`} replace />;
}

function Main() {
  const location = useLocation();
  const isMobile = useIsMobile();
  useSyncUserLocale();
  // Key by top-level path segment so /chat sub-routes share a key (no re-animation)
  const pageKey = location.pathname.split('/')[1] || 'dashboard';

  // A chunk a deploy deleted rejects rather than stays pending, so Suspense
  // hands the throw straight up; without a boundary it reaches the root and
  // takes the sidebar with it, and the pane's spinner hangs forever.
  //
  // Inside Suspense, not around it, even though both placements catch the
  // rejection (pinned in src/lib/__tests__/staleBuild.test.tsx). The boundary is keyed by
  // route so navigating away from a dead chunk clears the error, and a key on
  // the outside remounts Suspense along with it. Desktop already remounts this
  // subtree through AnimatePresence, but mobile renders it directly, and a
  // freshly mounted Suspense boundary has no previous content to hold — so it
  // must show its fallback, and with v7_startTransition every first navigation
  // to a route flashed the pane spinner where it used to switch in place.
  const routes = (
    <Suspense fallback={<PageLoading variant="pane" />}>
      <StaleBuildBoundary key={pageKey} variant="pane">
        <Routes location={location}>
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/chat" element={<ChatAgent />} />
          <Route path="/chat/t/:threadId/:taskId" element={<ChatAgent />} />
          <Route path="/chat/t/:threadId" element={<ChatAgent />} />
          <Route path="/chat/:workspaceId" element={<ChatAgent />} />
          <Route path="/market" element={<MarketView />} />
          <Route path="/automations" element={<Automations />} />
          <Route path="/plugins" element={<Plugins />} />
          <Route path="/connectors" element={<LegacyConnectorsRedirect />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="/news/:id" element={<NewsDetailPage />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Routes>
      </StaleBuildBoundary>
    </Suspense>
  );

  // On mobile, skip AnimatePresence — instant page switches feel snappier. The
  // wrapper is not cosmetic symmetry with the desktop branch below: it carries
  // the same `minHeight: 0`, which is the only thing that lets the route shrink
  // when the banner takes part of the column. Without it a route root pinned
  // with `min-height: 100%` (the dashboard) keeps the full column height and
  // its last banner's-worth of scroll area slides under the bottom tab bar —
  // invisible to an `.app-main` overflow check, because `.app-main` reserves
  // exactly that strip as padding for the tab bar. It must also stay a flex
  // column: the mobile dashboard is `height: auto` and relies on being a flex
  // item for its height, so a plain block wrapper would let it grow to its full
  // scroll height instead.
  if (isMobile) {
    return (
      <div className="main" style={{ height: '100%' }}>
        <NetworkBanner />
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
          {routes}
        </div>
        <ContextOverflowPill />
      </div>
    );
  }

  return (
    <div className="main">
      <NetworkBanner />
      <AnimatePresence mode="wait">
        <motion.div
          key={pageKey}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.15, ease: 'easeInOut' }}
          // `minHeight: 0` is what lets flex actually shrink this when the
          // banner takes part of the column. A flex item defaults to
          // `min-height: auto`, so without it a tall route (the dashboard grid)
          // refuses to go below its min-content height and hangs out of the
          // shell instead. It only sizes THIS wrapper though: a route root that
          // pins itself to the viewport instead of its container overflows the
          // shrunken column by exactly the banner's height (see AGENTS.md).
          style={{ height: '100%', minHeight: 0 }}
        >
          {routes}
        </motion.div>
      </AnimatePresence>
      <ContextOverflowPill />
    </div>
  );
}

export default Main;
