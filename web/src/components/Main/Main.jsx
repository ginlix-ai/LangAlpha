import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Dashboard from '../../pages/Dashboard/Dashboard';
import ChatAgent from '../../pages/ChatAgent/ChatAgent';
import MarketView from '../../pages/MarketView/MarketView';
import DetailPage from '../../pages/Detail/DetailPage';
import Automations from '../../pages/Automations/Automations';

function Main() {
  return (
    <div className="main">
      <Routes>
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/chat" element={<ChatAgent />} />
        <Route path="/chat/:workspaceId/:threadId" element={<ChatAgent />} />
        <Route path="/chat/:workspaceId" element={<ChatAgent />} />
        <Route path="/market" element={<MarketView />} />
        <Route path="/automations" element={<Automations />} />
        <Route path="/detail/:indexNumber" element={<DetailPage />} />
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </div>
  );
}

export default Main;
