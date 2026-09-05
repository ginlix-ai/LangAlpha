import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { authFetch } from '../authFetch';
import { tokenEndpointCooldownRemainingMs, resetTokenEndpointCooldown } from '../authCooldown';

const TOKEN_URL = 'https://proj.supabase.co/auth/v1/token?grant_type=refresh_token';

function sessionResponse(overrides: Record<string, unknown> = {}, status = 200) {
  return new Response(
    JSON.stringify({
      access_token: 'jwt',
      refresh_token: 'r1',
      token_type: 'bearer',
      expires_in: 3600,
      // What a clock-skewed client receives: a server epoch it cannot compare
      // against its own Date.now().
      expires_at: 1_700_000_000,
      ...overrides,
    }),
    { status, headers: { 'Content-Type': 'application/json' } },
  );
}

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  resetTokenEndpointCooldown();
  fetchMock = vi.fn();
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('client-relative expiry', () => {
  it('drops the server expires_at so auth-js recomputes from expires_in', async () => {
    fetchMock.mockResolvedValue(sessionResponse());

    const body = await (await authFetch(TOKEN_URL)).json();

    expect(body).not.toHaveProperty('expires_at');
    // Without expires_in there is nothing for auth-js to recompute from.
    expect(body.expires_in).toBe(3600);
    expect(body.access_token).toBe('jwt');
  });

  it('leaves a session response that never carried expires_at alone', async () => {
    const payload = { access_token: 'jwt', expires_in: 3600 };
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify(payload), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    );

    expect(await (await authFetch(TOKEN_URL)).json()).toEqual(payload);
  });

  it('passes non-GoTrue responses straight through', async () => {
    const original = sessionResponse();
    fetchMock.mockResolvedValue(original);

    const res = await authFetch('https://proj.supabase.co/rest/v1/things');

    expect(res).toBe(original);
    expect(await res.json()).toHaveProperty('expires_at');
  });

  it('passes a non-JSON GoTrue response through untouched', async () => {
    fetchMock.mockResolvedValue(
      new Response('not json', { status: 200, headers: { 'Content-Type': 'text/plain' } }),
    );

    expect(await (await authFetch('https://proj.supabase.co/auth/v1/user')).text()).toBe('not json');
  });

  it('does not rewrite an error response', async () => {
    fetchMock.mockResolvedValue(sessionResponse({}, 400));

    const res = await authFetch(TOKEN_URL);

    expect(res.status).toBe(400);
    expect(await res.json()).toHaveProperty('expires_at');
  });
});

describe('429 breaker', () => {
  it('turns a 429 into a status auth-js treats as retryable', async () => {
    // 502/503/504 are the only statuses auth-js retries; everything else takes
    // _callRefreshToken into _removeSession() and signs the user out.
    fetchMock.mockResolvedValue(new Response('{}', { status: 429 }));

    expect((await authFetch(TOKEN_URL)).status).toBe(503);
  });

  it('answers subsequent token requests locally while cooling down', async () => {
    fetchMock.mockResolvedValue(new Response('{}', { status: 429 }));
    await authFetch(TOKEN_URL);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    // auth-js retries a 503 about seven times over ~25s. None may reach the
    // network, or the breaker would amplify the storm it exists to stop.
    for (let i = 0; i < 7; i += 1) {
      expect((await authFetch(TOKEN_URL)).status).toBe(503);
    }
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('honours Retry-After, and caps it', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-01T00:00:00Z'));

    fetchMock.mockResolvedValue(
      new Response('{}', { status: 429, headers: { 'Retry-After': '30' } }),
    );
    await authFetch(TOKEN_URL);
    expect(tokenEndpointCooldownRemainingMs()).toBe(30_000);

    vi.advanceTimersByTime(30_001);
    expect(tokenEndpointCooldownRemainingMs()).toBe(0);

    resetTokenEndpointCooldown();
    fetchMock.mockResolvedValue(
      new Response('{}', { status: 429, headers: { 'Retry-After': '99999' } }),
    );
    await authFetch(TOKEN_URL);
    expect(tokenEndpointCooldownRemainingMs()).toBe(300_000);
  });

  it('falls back to a default window when Retry-After is missing or junk', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-01T00:00:00Z'));

    fetchMock.mockResolvedValue(
      new Response('{}', { status: 429, headers: { 'Retry-After': 'soon' } }),
    );
    await authFetch(TOKEN_URL);

    expect(tokenEndpointCooldownRemainingMs()).toBe(60_000);
  });

  it('does not trip on a 429 from anywhere but the token endpoint', async () => {
    fetchMock.mockResolvedValue(new Response('{}', { status: 429 }));

    expect((await authFetch('https://proj.supabase.co/auth/v1/user')).status).toBe(429);
    expect(tokenEndpointCooldownRemainingMs()).toBe(0);
  });
});

describe('a clock corrected backwards', () => {
  it('neither stretches an open cool-off nor cancels it', async () => {
    // The machines that read every token as expiring are the machines whose
    // clocks get corrected, so a large backwards jump is the likely sequel to
    // the storm, not a hypothetical. It is also the remedy we hand the user, so
    // this runs at the exact moment they did what we asked.
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-01T00:00:00Z'));
    fetchMock.mockResolvedValue(
      new Response('{}', { status: 429, headers: { 'Retry-After': '60' } }),
    );
    await authFetch(TOKEN_URL);
    expect(tokenEndpointCooldownRemainingMs()).toBe(60_000);

    vi.setSystemTime(new Date('2026-08-31T23:00:00Z'));

    // The window is timed on `performance.now()`, which no correction reaches,
    // so the wall clock losing an hour is simply not an event here. Reading it
    // on `Date.now()` left two bad options and we took neither: honour the
    // stamp and refresh stays locally disabled for the whole jump, or notice
    // the jump and abandon the window, which retries a token endpoint that has
    // just rate-limited us.
    expect(tokenEndpointCooldownRemainingMs()).toBe(60_000);
    expect((await authFetch(TOKEN_URL)).status).toBe(503);

    // And it still ends on time, measured from when it opened.
    vi.advanceTimersByTime(60_001);
    expect(tokenEndpointCooldownRemainingMs()).toBe(0);
    fetchMock.mockResolvedValue(sessionResponse());
    expect((await authFetch(TOKEN_URL)).status).toBe(200);
  });
});

describe('the breaker leaves sign-in alone', () => {
  // GoTrue multiplexes five grants onto /auth/v1/token: password, pkce,
  // id_token, web3 and refresh_token. Only the last is ours to hold closed.
  const SIGN_IN_GRANTS = [
    'https://proj.supabase.co/auth/v1/token?grant_type=password',
    'https://proj.supabase.co/auth/v1/token?grant_type=pkce',
  ];

  it.each(SIGN_IN_GRANTS)('lets %s through an open cool-off', async (url) => {
    // Trip the breaker on a refresh, the way a storm would.
    fetchMock.mockResolvedValueOnce(new Response('{}', { status: 429 }));
    await authFetch(TOKEN_URL);
    expect(tokenEndpointCooldownRemainingMs()).toBeGreaterThan(0);

    fetchMock.mockResolvedValueOnce(sessionResponse());
    const res = await authFetch(url);

    // Signing in is how a user recovers from the storm. Answering it with our
    // own 503 would lock them out for the length of the cool-off, which is the
    // reporter's complaint in a new costume: signed out, unable to get back in.
    expect(res.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('does not convert a 429 the sign-in limiter raised', async () => {
    // A different limit with a different message, which the login screen shows.
    fetchMock.mockResolvedValue(new Response('{"msg":"too many attempts"}', {
      status: 429,
      headers: { 'Content-Type': 'application/json' },
    }));

    const res = await authFetch(SIGN_IN_GRANTS[0]);

    expect(res.status).toBe(429);
    expect(tokenEndpointCooldownRemainingMs()).toBe(0);
  });
});

describe('responses that cannot carry a body', () => {
  it('passes a 204 through instead of rebuilding it into a TypeError', async () => {
    // Sign-out answers 204, and `new Response(body, {status: 204})` throws for
    // any body at all -- an empty string included. Rebuilding one would turn a
    // successful sign-out into a rejected fetch.
    fetchMock.mockResolvedValue(
      new Response(null, { status: 204, headers: { 'Content-Type': 'application/json' } }),
    );

    const res = await authFetch('https://proj.supabase.co/auth/v1/logout');

    expect(res.status).toBe(204);
  });
});
