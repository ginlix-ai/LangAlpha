import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Dashboard from '../../pages/Dashboard/Dashboard';
import ChatAgent from '../../pages/ChatAgent/ChatAgent';
import TradingCenter from '../../pages/TradingCenter/TradingCenter';
import DetailPage from '../../pages/Detail/DetailPage';

function Main() {
  return (
    <div className="main">
      <Routes>
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/chat" element={<ChatAgent />} />
        <Route path="/chat/:workspaceId/:threadId" element={<ChatAgent />} />
        <Route path="/chat/:workspaceId" element={<ChatAgent />} />
        <Route path="/trading" element={<TradingCenter />} />
        <Route path="/detail/:indexNumber" element={<DetailPage />} />
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </div>
  );
}

export default Main;
