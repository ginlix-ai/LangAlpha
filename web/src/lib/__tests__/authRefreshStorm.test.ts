/**
 * Issue #379, end to end against the real `@supabase/auth-js`.
 *
 * The unit suites either side of this one check the two modules in isolation.
 * This one wires a real Supabase client to a fake GoTrue whose clock runs
 * behind the client's, which is the condition the outage needed, and measures
 * what a page load costs in `POST /auth/v1/token`. Supabase caps that endpoint
 * at 1800/hr per IP with a burst of 30 and does not make it configurable.
 *
 * The first test is the witness: it drives the client the way the app used to,
 * one `getSession()` per outbound request, and asserts the storm still happens
 * without our fetch. If that test ever goes green for the wrong reason -- the
 * library starts cancelling skew itself -- the ones below stop meaning anything,
 * and this is what says so.
 *
 * Concurrency is not the multiplier, which took a browser to establish. jsdom
 * has no `navigator.locks`, so auth-js falls back to `lockNoOp` and the
 * serialization a real tab gets is absent here; the same blind spot is why the
 * original Node repro reported a fan-out. Measured in Chrome, and reproduced
 * here since auth-js 2.110.1 added an in-process dedup that does what the lock
 * did, a burst of any width costs exactly **one** refresh.
 *
 * The drain is that the one refresh is spent again on the very next burst, and
 * the next, for the life of the tab, because a token measured against the wrong
 * clock never reads fresh. A drip, not an explosion, and it empties the same
 * budget. That is what the witness below drives.
 */
import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const AUTH_URL = 'https://auth.example.test';
const ANON_KEY = 'anon-key';
const STORAGE_KEY = 'langalpha-auth';

const JWT_TTL_SEC = 15 * 60;
/**
 * Client clock 20 minutes fast. The failure region is `skew > TTL - 90s`
 * (auth-js compares a server `expires_at` against a local `Date.now()` with a
 * 90s margin), so this sits inside it: every freshly issued token reads as
 * already expiring.
 */
const SERVER_SKEW_SEC = 20 * 60;
/** Stand-in for GoTrue's per-IP burst. */
const RATE_LIMIT = 8;

let tokenRequests = 0;
let issued = 0;
let rateLimitAfter = RATE_LIMIT;
let store: Map<string, string>;

function serverIssuedSession() {
  issued += 1;
  const serverNow = Math.floor(Date.now() / 1000) - SERVER_SKEW_SEC;
  return {
    access_token: `jwt.${issued}`,
    refresh_token: `rt.${issued}`,
    token_type: 'bearer',
    expires_in: JWT_TTL_SEC,
    // The server stamps this from its own epoch. Cancelling that is layer 1.
    expires_at: serverNow + JWT_TTL_SEC,
    user: {
      id: 'u1', aud: 'authenticated', app_metadata: {}, user_metadata: {},
      created_at: '2026-01-01T00:00:00Z',
    },
  };
}

const json = (body: unknown, status: number, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });

/** A GoTrue that rotates refresh tokens and rate-limits, like the real one. */
const fakeGoTrue = async (input: RequestInfo | URL): Promise<Response> => {
  const url = typeof input === 'string' ? input
    : input instanceof URL ? input.href : input.url;
  if (!url.includes('/auth/v1/token')) return json({}, 200);
  tokenRequests += 1;
  if (tokenRequests > rateLimitAfter) {
    return json(
      { code: 429, error_code: 'over_request_rate_limit', msg: 'Request rate limit reached' },
      429,
      { 'Retry-After': '60' },
    );
  }
  return json(serverIssuedSession(), 200);
};

function makeClient(fetchImpl: typeof fetch): SupabaseClient {
  return createClient(AUTH_URL, ANON_KEY, {
    auth: {
      storageKey: STORAGE_KEY,
      storage: {
        getItem: (k: string) => store.get(k) ?? null,
        setItem: (k: string, v: string) => void store.set(k, v),
        removeItem: (k: string) => void store.delete(k),
      },
      persistSession: true,
      // Off so the 30s background tick cannot add refreshes of its own; every
      // request counted below is one the read path asked for. `getSession()`
      // refreshes regardless of this flag (`__loadSession`).
      autoRefreshToken: false,
      detectSessionInUrl: false,
    },
    global: { fetch: fetchImpl },
  });
}

/** A session persisted before the fix: its expiry is the server's epoch. */
function seedStoredSession() {
  const session = serverIssuedSession();
  store.set(STORAGE_KEY, JSON.stringify(session));
  return session;
}

const held = vi.hoisted(() => ({ client: null as SupabaseClient | null }));
vi.mock('../supabase', () => ({
  get supabase() { return held.client; },
}));

/**
 * The mode is declared here, never inherited.
 *
 * `isPlatformMode` resolves from a build-time env var, and the token cache
 * consults it before it will touch a session at all. Left to the environment,
 * this file tests the platform auth path only on a machine whose `web/.env`
 * happens to set `VITE_HOST_MODE=platform`: it passes locally and reports zero
 * refreshes in CI, where there is no `.env` and the default is `oss`. That is
 * the worst possible failure for the one test that exercises the real auth-js,
 * because a green run would mean the storm was never provoked.
 */
vi.mock('../../config/hostMode', () => ({ isPlatformMode: true }));

/**
 * `authFetch` and `authToken` share the breaker in `authCooldown`, so all three
 * have to come from one module graph -- and a fresh one per test, since each
 * holds module state.
 */
async function loadModules() {
  vi.resetModules();
  const authFetch = await import('../authFetch');
  const authCooldown = await import('../authCooldown');
  const authToken = await import('../authToken');
  return { ...authFetch, ...authCooldown, ...authToken };
}

beforeEach(() => {
  tokenRequests = 0;
  issued = 0;
  rateLimitAfter = RATE_LIMIT;
  store = new Map();
  vi.stubGlobal('fetch', fakeGoTrue);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
  held.client = null;
});

describe('the refresh storm, against the real auth-js', () => {
  it('still happens when the session is read per request', async () => {
    // No custom fetch: the client the app shipped before this change.
    held.client = makeClient(fakeGoTrue);
    let signedOut = false;
    held.client.auth.onAuthStateChange((event) => {
      if (event === 'SIGNED_OUT') signedOut = true;
    });
    seedStoredSession();

    // One burst is one page interaction: a fan of requests that each read the
    // session. auth-js collapses the fan to a single refresh, so a burst costs
    // 1 -- and costs 1 again on the very next one, because the skewed expiry it
    // just wrote still reads as already expiring.
    const perBurst: number[] = [];
    for (let burst = 0; burst <= RATE_LIMIT; burst += 1) {
      const before = tokenRequests;
      await Promise.all(
        Array.from({ length: 12 }, () => held.client!.auth.getSession()),
      );
      perBurst.push(tokenRequests - before);
    }

    // Never a fan-out, always a drip. Asserting the whole shape rather than a
    // total: a total would also pass if one burst spent the budget by itself,
    // which is the diagnosis this file exists to correct.
    expect(perBurst).toEqual(Array(RATE_LIMIT + 1).fill(1));
    expect(tokenRequests).toBeGreaterThan(RATE_LIMIT);
    // And the 429 is what ends the session. auth-js 2.110.1 will preserve a
    // session whose access token is still valid when a refresh fails, but it
    // decides that by comparing the server's `expires_at` against the local
    // `Date.now()` -- the very comparison skew breaks -- so the token reads as
    // expired, `_removeSession` runs, and the user is ejected with no way to
    // reload back in. Cancelling the skew is what makes that guard work, and
    // that is layer 1's job, not the breaker's.
    expect(signedOut).toBe(true);
    expect(store.has(STORAGE_KEY)).toBe(false);
  });

  it('rotates once the corrected clock outlives the token, rather than serving it', async () => {
    // The sequel to the storm, and the reason it needs the real library. Turning
    // time synchronisation on is the remedy we hand these users, and it walks
    // `Date.now()` back past the stamp the expiry was written against. Storage
    // cannot repair that on its own: auth-js compares the same stamp, finds it
    // further in the future than before, and returns the session unrefreshed. A
    // guard that only re-reads storage therefore fires once, restamps, and
    // disarms itself, after which the dead token reads fresh for the length of
    // the skew. Mocking `getSession` hides all of this, because the mock hands
    // back a fresh expiry that the real one never would.
    vi.useFakeTimers();
    const base = new Date('2026-09-01T12:00:00Z').getTime();
    vi.setSystemTime(base);

    const { authFetch, getAccessToken } = await loadModules();
    held.client = makeClient(authFetch);
    seedStoredSession();

    const issuedOnTheFastClock = await getAccessToken();
    const spentAtSignIn = tokenRequests;

    // The correction lands. The token is still genuinely alive here.
    vi.setSystemTime(base - SERVER_SKEW_SEC * 1000);
    expect(await getAccessToken()).not.toBeNull();

    // And now real time carries it past the expiry the server actually stamped.
    vi.setSystemTime(base - SERVER_SKEW_SEC * 1000 + (JWT_TTL_SEC + 1) * 1000);
    const afterItReallyDied = await getAccessToken();

    expect(afterItReallyDied).not.toBe(issuedOnTheFastClock);
    expect(tokenRequests).toBeGreaterThan(spentAtSignIn);
  });

  it('costs one refresh, and then none, through the shared token', async () => {
    const { authFetch, getAccessToken } = await loadModules();
    held.client = makeClient(authFetch);
    let signedOut = false;
    held.client.auth.onAuthStateChange((event) => {
      if (event === 'SIGNED_OUT') signedOut = true;
    });
    seedStoredSession();

    const first = await Promise.all(
      Array.from({ length: 12 }, () => getAccessToken()),
    );

    // The stored session carries the old server-relative expiry, so it reads as
    // expired once and is refreshed once. Every other caller waits on that one.
    expect(tokenRequests).toBe(1);
    expect(new Set(first).size).toBe(1);
    expect(first[0]).toBe(`jwt.${issued}`);

    // The refreshed session came back through our fetch, so its expiry is now
    // measured on this clock -- the skew is gone and a second load is free.
    const second = await Promise.all(
      Array.from({ length: 12 }, () => getAccessToken()),
    );
    expect(tokenRequests).toBe(1);
    expect(second.every((t) => t === first[0])).toBe(true);

    // Past the renewal floor, which is the only thing bounding a still-skewed
    // expiry: a token measured on the server's clock reads as stale forever, so
    // without the correction every window costs another refresh, for the life
    // of the tab. With it the cache is simply fresh and nothing is re-derived.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.setSystemTime(Date.now() + 11_000);
    const third = await Promise.all(
      Array.from({ length: 12 }, () => getAccessToken()),
    );
    expect(tokenRequests).toBe(1);
    expect(third.every((t) => t === first[0])).toBe(true);
    expect(signedOut).toBe(false);
  });

  it('survives a rate limit instead of signing the user out', async () => {
    rateLimitAfter = 0; // every token request is refused
    // auth-js logs the retryable failure itself; the assertions are the report.
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { authFetch, getAccessToken, publishSession } = await loadModules();
    held.client = makeClient(authFetch);
    let signedOut = false;
    held.client.auth.onAuthStateChange((event) => {
      if (event === 'SIGNED_OUT') signedOut = true;
    });
    // Published as AuthContext publishes it, so there is a held token for the
    // request to fall back on -- which is the whole point of holding one.
    publishSession(seedStoredSession() as never);

    // auth-js answers a retryable failure with a backoff ladder bounded by its
    // 30s tick. Every rung after the first is served by the breaker without a
    // request, so the ladder is free; the timers are faked to skip the wait.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const pending = getAccessToken();
    await vi.advanceTimersByTimeAsync(60_000);
    const token = await pending;

    expect(signedOut).toBe(false);
    expect(store.has(STORAGE_KEY)).toBe(true);
    // One request reached the network; the ~7 retries behind it did not.
    expect(tokenRequests).toBe(1);
    // And the caller is handed the token we still hold rather than nothing.
    expect(token).toBe('jwt.1');
  });
});
