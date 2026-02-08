import React from 'react';
import { useAuth } from '../contexts/AuthContext';
import UserConfigPanel from '../pages/Dashboard/components/UserConfigPanel';

/**
 * AuthGate - Blocks app usage until user is logged in.
 * When not logged in, shows only login/signup modal (no app content).
 */
function AuthGate({ children }) {
  const { isLoggedIn, isInitialized } = useAuth();

  if (!isInitialized) {
    return (
      <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-primary)' }}>
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>Loading...</p>
      </div>
    );
  }

  if (!isLoggedIn) {
    return (
      <UserConfigPanel
        isOpen={true}
        onClose={() => {}}
        requireLogin={true}
      />
    );
  }

  return children;
}

export default AuthGate;
