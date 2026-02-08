/**
 * Auth session storage with expiration.
 * Stores user session in localStorage so user doesn't need to login every time.
 */

const STORAGE_KEY = 'auth_session';
const DEFAULT_EXPIRY_DAYS = 7;

export function getStoredSession() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const session = JSON.parse(raw);
    if (!session || !session.userId || !session.expiresAt) return null;
    if (Date.now() >= session.expiresAt) {
      localStorage.removeItem(STORAGE_KEY);
      return null;
    }
    return session;
  } catch {
    return null;
  }
}

export function storeSession(userId, user = null, expiryDays = DEFAULT_EXPIRY_DAYS) {
  const expiresAt = Date.now() + expiryDays * 24 * 60 * 60 * 1000;
  const session = { userId, user, expiresAt };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(session));
}

export function clearStoredSession() {
  localStorage.removeItem(STORAGE_KEY);
}
