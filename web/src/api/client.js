/**
 * Shared API client for backend REST calls.
 * Bearer token is set automatically via setTokenGetter (called from AuthContext).
 */
import axios from 'axios';

const baseURL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';

/** Async function that returns the current access token (set by AuthContext). */
let _getAccessToken = null;

export function setTokenGetter(fn) {
  _getAccessToken = fn;
}

export const api = axios.create({
  baseURL,
  headers: { 'Content-Type': 'application/json' },
});

api.interceptors.request.use(async (config) => {
  if (_getAccessToken) {
    try {
      const token = await _getAccessToken();
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
    } catch {
      /* proceed without auth */
    }
  }
  return config;
});
