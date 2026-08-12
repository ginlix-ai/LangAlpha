/**
 * Shared API client for backend REST calls.
 * Bearer token is set automatically via setTokenGetter (called from AuthContext).
 */
import axios, { type AxiosError, type InternalAxiosRequestConfig } from 'axios';

const baseURL = import.meta.env.VITE_API_BASE_URL ?? '';

type TokenGetter = () => Promise<string | null>;

/** Axios request config carrying a single-shot 401-retry guard. */
type RetriableRequestConfig = InternalAxiosRequestConfig & { _retry?: boolean };

/** Async function that returns the current access token (set by AuthContext). */
let _getAccessToken: TokenGetter | null = null;

export function setTokenGetter(fn: TokenGetter) {
  _getAccessToken = fn;
}

/** Async function that force-refreshes the session and returns a fresh token (set by AuthContext). */
let _refreshToken: TokenGetter | null = null;

export function setTokenRefresher(fn: TokenGetter) {
  _refreshToken = fn;
}

export const api = axios.create({
  baseURL,
  headers: { 'Content-Type': 'application/json' },
});

api.interceptors.request.use(async (config: InternalAxiosRequestConfig) => {
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

// Enrich 429 errors with structured rate limit info; single-shot 401 refresh-and-retry.
api.interceptors.response.use(
  (response) => response,
  async (error: AxiosError & { status?: number; rateLimitInfo?: Record<string, unknown>; retryAfter?: number | null }) => {
    if (error.response?.status === 429) {
      const detail = (error.response.data as Record<string, unknown>)?.detail || {};
      error.status = 429;
      error.rateLimitInfo = typeof detail === 'object' ? detail as Record<string, unknown> : {};
      error.retryAfter = parseInt(error.response.headers?.['retry-after'] as string, 10) || null;
    }

    // iOS Safari returns from a frozen tab with a stale token before Supabase's auto-refresh
    // runs, so a refetch hits a 401. Force-refresh once and replay the request.
    const config = error.config as RetriableRequestConfig | undefined;
    if (error.response?.status === 401 && config && !config._retry && _refreshToken) {
      config._retry = true;
      try {
        const token = await _refreshToken();
        if (token) {
          config.headers.Authorization = `Bearer ${token}`;
          return api(config);
        }
      } catch {
        /* refresh failed — fall through and reject with the original error */
      }
    }

    return Promise.reject(error);
  },
);
