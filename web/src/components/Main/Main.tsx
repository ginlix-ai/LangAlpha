import React, { Suspense } from 'react';
import { Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import PageLoading from '@/components/PageLoading/PageLoading';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useSyncUserLocale } from '@/hooks/useSyncUserLocale';
import { ContextOverflowPill } from '@/components/ui/ContextOverflowPill';

// Chunk thunks shared by the lazy components and preloadRouteChunk — import()
// is deduped by the module system, so a preload and the lazy mount share one
// network fetch.
const routeChunks = {
  dashboard: () => import('../../pages/Dashboard/DashboardRouter'),
  chat: () => import('../../pages/ChatAgent/ChatAgent'),
  market: () => import('../../pages/MarketView/MarketView'),
  news: () => import('../../pages/Detail/NewsDetailPage'),
  automations: () => import('../../pages/Automations/Automations'),
  settings: () => import('../../pages/Settings/Settings'),
};

const Dashboard = React.lazy(routeChunks.dashboard);
const ChatAgent = React.lazy(routeChunks.chat);
const MarketView = React.lazy(routeChunks.market);
const NewsDetailPage = React.lazy(routeChunks.news);
const Automations = React.lazy(routeChunks.automations);
const Settings = React.lazy(routeChunks.settings);

/** Start downloading the chunk for `pathname` without rendering it, so the
 * shell can warm the target route while the /users/me gate is still
 * resolving instead of serializing the two network legs. Unknown segments
 * warm the dashboard chunk (the catch-all redirect's target). */
export function preloadRouteChunk(pathname: string): void {
  const chunkFor: Record<string, () => Promise<unknown>> = routeChunks;
  const segment = pathname.split('/')[1] || 'dashboard';
  void (chunkFor[segment] ?? routeChunks.dashboard)();
}

function Main() {
  const location = useLocation();
  const isMobile = useIsMobile();
  useSyncUserLocale();
  // Key by top-level path segment so /chat sub-routes share a key (no re-animation)
  const pageKey = location.pathname.split('/')[1] || 'dashboard';

  const routes = (
    <Suspense fallback={<PageLoading variant="pane" />}>
      <Routes location={location}>
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/chat" element={<ChatAgent />} />
        <Route path="/chat/t/:threadId/:taskId" element={<ChatAgent />} />
        <Route path="/chat/t/:threadId" element={<ChatAgent />} />
        <Route path="/chat/:workspaceId" element={<ChatAgent />} />
        <Route path="/market" element={<MarketView />} />
        <Route path="/automations" element={<Automations />} />
        <Route path="/settings" element={<Settings />} />
        <Route path="/news/:id" element={<NewsDetailPage />} />
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </Suspense>
  );

  // On mobile, skip AnimatePresence — instant page switches feel snappier
  if (isMobile) {
    return (
      <div className="main" style={{ height: '100%' }}>
        {routes}
        <ContextOverflowPill />
      </div>
    );
  }

  return (
    <div className="main">
      <AnimatePresence mode="wait">
        <motion.div
          key={pageKey}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.15, ease: 'easeInOut' }}
          style={{ height: '100%' }}
        >
          {routes}
        </motion.div>
      </AnimatePresence>
      <ContextOverflowPill />
    </div>
  );
}

export default Main;
