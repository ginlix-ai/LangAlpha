import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Dashboard from '../../pages/Dashboard/Dashboard';
import ChatAgent from '../../pages/ChatAgent/ChatAgent';
import PersonalHome from '../../pages/PersonalHome/PersonalHome';
import TradingCenter from '../../pages/TradingCenter/TradingCenter';

function Main() {
  return (
    <div className="main">
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/chat" element={<ChatAgent />} />
        <Route path="/chat/:workspaceId/:threadId" element={<ChatAgent />} />
        <Route path="/chat/:workspaceId" element={<ChatAgent />} />
        <Route path="/home" element={<PersonalHome />} />
        <Route path="/trading" element={<TradingCenter />} />
      </Routes>
    </div>
  );
}

export default Main;
