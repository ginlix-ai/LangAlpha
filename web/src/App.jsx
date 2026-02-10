import React, { useEffect } from 'react';
import { Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import Sidebar from './components/Sidebar/Sidebar';
import Main from './components/Main/Main';
import LoginPage from './pages/Login/LoginPage';
import { useAuth } from './contexts/AuthContext';
import './App.css';

/** Handles the OAuth redirect from Supabase — shows a spinner then redirects to /dashboard. */
function AuthCallback() {
  const { isLoggedIn } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (isLoggedIn) {
      navigate('/dashboard', { replace: true });
    }
  }, [isLoggedIn, navigate]);

  return (
    <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>Signing you in...</p>
    </div>
  );
}

function App() {
  const { isLoggedIn, isInitialized } = useAuth();

  if (!isInitialized) {
    return (
        <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>Loading...</p>
      </div>
    );
  }

  return (
    <Routes>
      <Route path="/" element={isLoggedIn ? <Navigate to="/dashboard" replace /> : <LoginPage />} />
      <Route path="/callback" element={<AuthCallback />} />
      <Route path="/*" element={
        isLoggedIn ? (
          <div className="app-layout">
            <Sidebar />
            <main className="app-main">
              <Main />
            </main>
          </div>
        ) : (
          <Navigate to="/" replace />
        )
      } />
    </Routes>
  );
}

export default App;
