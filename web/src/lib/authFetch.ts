/**
 * The `fetch` the Supabase client talks to GoTrue through.
 *
 * Two corrections live here, both of which have to happen before auth-js sees
 * the response:
 *
 * 1. The server's `expires_at` is dropped so auth-js recomputes it from
 *    `expires_in` on this machine's clock. Otherwise a server epoch is compared
 *    against a local `Date.now()` and clock skew never cancels.
 * 2. A 429 on the token endpoint is turned into a retryable 503 and the
 *    endpoint is held closed for the `Retry-After` window, so a rate limit
 *    cannot destroy the session.
 *
 * Kept apart from `lib/supabase` so it carries no client construction, and so
 * mocking the client in a test does not drag this in.
 */

import { openTokenCooldown, tokenEndpointCooldownRemainingMs } from './authCooldown';

const GOTRUE_PREFIX = '/auth/v1/';
const TOKEN_PATH = '/auth/v1/token';

/**
 * Only the refresh grant. GoTrue multiplexes five grants onto `/auth/v1/token`
 * -- `password`, `pkce`, `id_token`, `web3` and `refresh_token` -- and the
 * breaker below must not touch the other four. Signing in is how a user
 * recovers from a refresh storm, so answering their password or their OAuth
 * code exchange with a locally invented 503 would lock them out for the length
 * of the cool-off, which is the failure this whole module exists to prevent. It
 * would also swallow the sign-in limiter's own 429, which is a different limit
 * with a different message the login screen is meant to show.
 */
function isRefreshGrant(url: string): boolean {
  return url.includes(TOKEN_PATH) && url.includes('grant_type=refresh_token');
}

function urlOf(input: RequestInfo | URL): string {
  if (typeof input === 'string') return input;
  if (input instanceof URL) return input.href;
  return input.url;
}

/**
 * auth-js retries only `NETWORK_ERROR_CODES` (502/503/504 plus Cloudflare's
 * 520-530) and leaves the session in place; every other status is fatal and
 * takes `_callRefreshToken` into `_removeSession()`. A 429 therefore signs the
 * user out, which is the bug this stands in for. The exact status matters: 500
 * and 501 are not on that list, so returning one here would sign the user out
 * just as the real 429 did.
 */
function retryableStub(): Response {
  return new Response(JSON.stringify({ message: 'token endpoint cooling down' }), {
    status: 503,
    headers: { 'Content-Type': 'application/json' },
  });
}

/**
 * Drop the server's `expires_at` so auth-js recomputes it from `expires_in`
 * (`_sessionResponse` synthesizes one only when absent). A token's life is a
 * duration from issuance, so the clock that reads the expiry has to be the
 * clock that wrote it: otherwise a device an hour fast reads every fresh token
 * as already expiring and refreshes on every request. `expires_in` must
 * survive, or there is nothing to recompute from.
 */
async function withClientRelativeExpiry(res: Response): Promise<Response> {
  if (!(res.headers.get('content-type') ?? '').includes('application/json')) return res;
  // 204/205 cannot carry a body, so rebuilding one throws and turns a success
  // into a failed fetch. Sign-out answers 204, and a proxy that stamps a JSON
  // content-type on it is all it takes to get here.
  if (res.status === 204 || res.status === 205) return res;

  const body = await res.text();
  const headers = new Headers(res.headers);
  headers.delete('content-length');
  const rebuild = (text: string) =>
    new Response(text, { status: res.status, statusText: res.statusText, headers });

  try {
    const payload = JSON.parse(body);
    if (
      payload && typeof payload === 'object'
      && typeof payload.access_token === 'string'
      && typeof payload.expires_in === 'number'
      && 'expires_at' in payload
    ) {
      delete payload.expires_at;
      return rebuild(JSON.stringify(payload));
    }
  } catch {
    /* not JSON after all, so hand back what arrived */
  }
  return rebuild(body);
}

export const authFetch: typeof fetch = async (input, init) => {
  const url = urlOf(input);
  const isRefresh = isRefreshGrant(url);

  // Honour our own cool-off without spending a request on it. This is the
  // `Retry-After` compliance auth-js does not implement.
  if (isRefresh && tokenEndpointCooldownRemainingMs() > 0) return retryableStub();

  const res = await fetch(input, init);

  if (isRefresh && res.status === 429) {
    openTokenCooldown(res.headers.get('retry-after'));
    return retryableStub();
  }

  if (!res.ok || !url.includes(GOTRUE_PREFIX)) return res;
  return withClientRelativeExpiry(res);
};
