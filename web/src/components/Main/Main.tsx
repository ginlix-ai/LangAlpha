import React, { Suspense } from 'react';
import { Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { useIsMobile } from '@/hooks/useIsMobile';
import { ContextOverflowPill } from '@/components/ui/ContextOverflowPill';

const Dashboard = React.lazy(() => import('../../pages/Dashboard/DashboardRouter'));
const ChatAgent = React.lazy(() => import('../../pages/ChatAgent/ChatAgent'));
const MarketView = React.lazy(() => import('../../pages/MarketView/MarketView'));
const DetailPage = React.lazy(() => import('../../pages/Detail/DetailPage'));
const NewsDetailPage = React.lazy(() => import('../../pages/Detail/NewsDetailPage'));
const Automations = React.lazy(() => import('../../pages/Automations/Automations'));
const Settings = React.lazy(() => import('../../pages/Settings/Settings'));

function Main() {
  const location = useLocation();
  const isMobile = useIsMobile();
  // Key by top-level path segment so /chat sub-routes share a key (no re-animation)
  const pageKey = location.pathname.split('/')[1] || 'dashboard';

  const routes = (
    <Suspense fallback={null}>
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
        <Route path="/detail/:indexNumber" element={<DetailPage />} />
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
