/**
 * Shared API client for backend REST calls.
 * All portfolio, watchlist, and watchlist-items modules use this.
 * X-User-Id is set from AuthContext via setAuthUserId when user is logged in.
 */
import axios from 'axios';

const baseURL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';
export const DEFAULT_USER_ID = 'test_user_001';

let _authUserId = null;

export function setAuthUserId(userId) {
  _authUserId = userId;
}

export function getAuthUserId() {
  return _authUserId;
}

export const api = axios.create({
  baseURL,
  headers: { 'Content-Type': 'application/json' },
});

api.interceptors.request.use((config) => {
  if (_authUserId) {
    config.headers['X-User-Id'] = _authUserId;
  }
  return config;
});

export function headers(userId = DEFAULT_USER_ID) {
  return { 'X-User-Id': userId ?? _authUserId ?? DEFAULT_USER_ID };
}
