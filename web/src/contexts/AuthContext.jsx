import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { getStoredSession, storeSession, clearStoredSession } from './authStorage';
import { getCurrentUser, createUser } from '../pages/Dashboard/utils/api';
import { setAuthUserId } from '../api/client';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [userId, setUserId] = useState(null);
  const [user, setUser] = useState(null);
  const [isInitialized, setIsInitialized] = useState(false);

  const applySession = useCallback((session) => {
    if (session?.userId) {
      setUserId(session.userId);
      setUser(session.user ?? null);
      setAuthUserId(session.userId);
    } else {
      setUserId(null);
      setUser(null);
      setAuthUserId(null);
    }
  }, []);

  const login = useCallback(async (email) => {
    const trimmed = (email || '').trim().toLowerCase();
    if (!trimmed) throw new Error('Please enter your email');
    const data = await getCurrentUser(trimmed);
    if (data?.user) {
      const session = { userId: trimmed, user: data.user };
      storeSession(trimmed, data.user);
      applySession(session);
      return data;
    }
    throw new Error('User not found');
  }, [applySession]);

  const signup = useCallback(async (email, name) => {
    const trimmedEmail = (email || '').trim().toLowerCase();
    const trimmedName = (name || '').trim();
    if (!trimmedEmail) throw new Error('Please enter your email');
    if (!trimmedName) throw new Error('Please enter your name');
    await createUser(
      { email: trimmedEmail, name: trimmedName },
      trimmedEmail
    );
    const session = { userId: trimmedEmail, user: { email: trimmedEmail, name: trimmedName } };
    storeSession(trimmedEmail, session.user);
    applySession(session);
    return session;
  }, [applySession]);

  const logout = useCallback(() => {
    clearStoredSession();
    applySession(null);
  }, [applySession]);

  const refreshUser = useCallback(async () => {
    if (!userId) return;
    try {
      const data = await getCurrentUser(userId);
      if (data?.user) {
        setUser(data.user);
        storeSession(userId, data.user);
      }
    } catch {
      // Ignore refresh errors
    }
  }, [userId]);

  useEffect(() => {
    const session = getStoredSession();
    applySession(session);
    setIsInitialized(true);
  }, [applySession]);

  const value = {
    userId,
    user,
    isInitialized,
    isLoggedIn: !!userId,
    login,
    signup,
    logout,
    refreshUser,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
