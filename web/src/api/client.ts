/**
 * Shared API client for backend REST calls.
 *
 * The Bearer token comes from `lib/authToken`, the app's single token cache,
 * never from a session read per request, which is what once turned a page load
 * into a refresh storm. In OSS mode there is no Supabase client and both calls
 * resolve null, so no header is set.
 */
import axios, { type AxiosError, type InternalAxiosRequestConfig } from 'axios';
import { authGeneration, bearerTokenOf, getAccessToken, refreshAccessToken } from '../lib/authToken';

const baseURL = import.meta.env.VITE_API_BASE_URL ?? '';

/**
 * Axios request config carrying a single-shot 401-retry guard, plus which user
 * the request went out as, so the retry below can tell that it is still them.
 */
type RetriableRequestConfig = InternalAxiosRequestConfig & {
  _retry?: boolean;
  _authGeneration?: number;
};

export const api = axios.create({
  baseURL,
  headers: { 'Content-Type': 'application/json' },
});

api.interceptors.request.use(async (config: InternalAxiosRequestConfig) => {
  try {
    const token = await getAccessToken();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
      (config as RetriableRequestConfig)._authGeneration = authGeneration();
    }
  } catch {
    /* proceed without auth */
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

    // Any gate that writes a structured `detail` wrote a sentence meant for the
    // user. Without this, every call site that falls back to `err.message` shows
    // axios's "Request failed with status code 503" and the explanation the
    // server took care to send is dropped on the floor.
    const structured = (error.response?.data as Record<string, unknown> | undefined)?.detail;
    if (structured && typeof structured === 'object' && typeof (structured as Record<string, unknown>).message === 'string') {
      error.message = (structured as Record<string, unknown>).message as string;
    }

    // iOS Safari returns from a frozen tab with a stale token before Supabase's auto-refresh
    // runs, so a refetch hits a 401. Force-refresh once and replay the request.
    const config = error.config as RetriableRequestConfig | undefined;
    if (error.response?.status === 401 && config && !config._retry) {
      // A 401 can outlive the account it was issued for: the reply arrives
      // after a sign-out or an account switch, and by then the cache holds
      // somebody else's token. `refreshAccessToken` would hand that one over,
      // because all it is asked is whether the cache moved on from the refused
      // token, and it has. Replaying then sends a request built for the
      // previous user as the current one. Their own 401 is the right answer.
      //
      // Asked twice, because the rotation between the two is a network round
      // trip and the switch can land inside it. `config` still carries the
      // previous user's URL and body, so a replay stamped with the new user's
      // token is their mutation written to the new account.
      const sameAccount = () =>
        config._authGeneration === undefined || config._authGeneration === authGeneration();
      if (!sameAccount()) return Promise.reject(error);
      config._retry = true;
      try {
        // The token THIS request carried, not whichever one the cache holds by
        // now: a burst of 401s arrives one at a time, and the later ones are
        // being answered after an earlier one already rotated.
        const token = await refreshAccessToken(bearerTokenOf(config.headers?.Authorization));
        if (token && sameAccount()) {
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
