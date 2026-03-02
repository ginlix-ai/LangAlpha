import React from 'react';
import { Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import Dashboard from '../../pages/Dashboard/Dashboard';
import ChatAgent from '../../pages/ChatAgent/ChatAgent';
import MarketView from '../../pages/MarketView/MarketView';
import DetailPage from '../../pages/Detail/DetailPage';
import NewsDetailPage from '../../pages/Detail/NewsDetailPage';
import Automations from '../../pages/Automations/Automations';

function Main() {
  const location = useLocation();
  // Key by top-level path segment so /chat sub-routes share a key (no re-animation)
  const pageKey = location.pathname.split('/')[1] || 'dashboard';

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
          <Routes location={location}>
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/chat" element={<ChatAgent />} />
            <Route path="/chat/:workspaceId/:threadId" element={<ChatAgent />} />
            <Route path="/chat/:workspaceId" element={<ChatAgent />} />
            <Route path="/market" element={<MarketView />} />
            <Route path="/automations" element={<Automations />} />
            <Route path="/news/:id" element={<NewsDetailPage />} />
            <Route path="/detail/:indexNumber" element={<DetailPage />} />
            <Route path="*" element={<Navigate to="/dashboard" replace />} />
          </Routes>
        </motion.div>
      </AnimatePresence>
    </div>
  );
}

export default Main;
