import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const getSession = vi.fn();
const refreshSession = vi.fn();
let cooldownMs = 0;
let platformMode = true;

vi.mock('../../config/hostMode', () => ({
  get isPlatformMode() { return platformMode; },
}));

vi.mock('../supabase', () => ({
  supabase: {
    auth: {
      getSession: (...args: unknown[]) => getSession(...args),
      refreshSession: (...args: unknown[]) => refreshSession(...args),
    },
  },
}));

vi.mock('../authCooldown', () => ({
  tokenEndpointCooldownRemainingMs: () => cooldownMs,
}));

const NOW = new Date('2026-09-01T12:00:00Z').getTime();

/** `expires_at` is a Unix timestamp in SECONDS, which is the usual trip hazard. */
function session(secondsUntilExpiry: number, token = 'jwt') {
  return {
    access_token: token,
    user: { id: 'u1' },
    expires_at: Math.floor((NOW + secondsUntilExpiry * 1000) / 1000),
  };
}

async function loadModule() {
  vi.resetModules();
  return import('../authToken');
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW);
  getSession.mockReset();
  refreshSession.mockReset();
  cooldownMs = 0;
  platformMode = true;
});

afterEach(() => {
  vi.useRealTimers();
});

describe('serving the cached token', () => {
  it('answers from memory without touching the network', async () => {
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(3600) as never);

    expect(await getAccessToken()).toBe('jwt');
    expect(getSession).not.toHaveBeenCalled();
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('adopts the token auth-js hands us on TOKEN_REFRESHED', async () => {
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(3600, 'first') as never);
    publishSession(session(3600, 'second') as never);

    expect(await getAccessToken()).toBe('second');
    expect(getSession).not.toHaveBeenCalled();
  });

  it('forgets everything on sign-out', async () => {
    const { publishSession, clearAuthToken, getAccessToken } = await loadModule();
    publishSession(session(3600) as never);
    clearAuthToken();
    getSession.mockResolvedValue({ data: { session: null } });
    refreshSession.mockResolvedValue({ data: { session: null } });

    expect(await getAccessToken()).toBeNull();
  });

  it('treats a session with no expiry as expired rather than trusting it', async () => {
    const { publishSession, getAccessToken } = await loadModule();
    publishSession({ access_token: 'jwt', user: { id: 'u1' } } as never);
    getSession.mockResolvedValue({ data: { session: session(3600, 'renewed') } });

    expect(await getAccessToken()).toBe('renewed');
  });
});

describe('a renewal that does not come back', () => {
  it('hands over the held token rather than freezing every reader behind it', async () => {
    // What a rate limit actually costs: auth-js walks a local retry ladder
    // bounded by its own 30s tick, so `getSession()` sat for ~39s in Chrome
    // with every caller parked on it. Measured, not theorised -- and invisible
    // to a suite that fakes its timers, which is how it shipped past review.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(-10) as never);
    getSession.mockReturnValue(new Promise(() => { /* never settles */ }));

    const pending = getAccessToken();
    await vi.advanceTimersByTimeAsync(5_000);

    await expect(pending).resolves.toBe('jwt');
  });

  it('keeps waiting when there is nothing to hand over instead', async () => {
    // The bound may only ever downgrade a fresh answer to a held one. Resolving
    // null here would send a request with no credential at all, which is a
    // guaranteed 401 -- worse than waiting for an answer still on its way.
    const { getAccessToken } = await loadModule();
    let release!: (v: unknown) => void;
    getSession.mockReturnValue(new Promise((resolve) => { release = resolve; }));

    let settled = false;
    const pending = getAccessToken().then((t) => { settled = true; return t; });
    await vi.advanceTimersByTimeAsync(30_000);
    expect(settled).toBe(false);

    release({ data: { session: session(3600, 'arrived-late') } });
    await expect(pending).resolves.toBe('arrived-late');
  });

  it('does not make later readers wait out a ladder the cool-off already lost', async () => {
    // The bound alone still charges every reader the full wait, for as long as
    // auth-js keeps walking its ladder. Once the cool-off is open we know that
    // ladder cannot produce a token, so waiting on it buys nothing at all.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(-10) as never);
    getSession.mockReturnValue(new Promise(() => { /* never settles */ }));

    void getAccessToken();
    cooldownMs = 60_000;

    let served: string | null | undefined;
    void getAccessToken().then((token) => { served = token; });
    await vi.advanceTimersByTimeAsync(0);

    expect(served).toBe('jwt');
  });
});

describe('a clock corrected backwards', () => {
  // The mitigation we hand a user whose machine is an hour fast is "turn on
  // automatic time synchronization", so the correction is not a hypothetical:
  // it is the next thing that happens to the exact device this module is for.
  it('stops trusting an expiry the same clock has since disowned', async () => {
    // The stamp was written on the fast clock, and the correction moves only
    // `Date.now()`. Left alone, the comparison calls the token fresh for the
    // length of the jump past its real exp, and auth-js's cookie carries the
    // same stamp, so its background timer would not rotate it either.
    //
    // This checks that the guard fires and nothing more. It CANNOT show the
    // guard is sufficient, because `getSession` is mocked here and answers with
    // a fresh expiry the real library would not: auth-js re-reads the same
    // stamp, finds it further in the future than before, and returns the
    // session unrefreshed, which is why re-reading storage is not enough and
    // the read escalates to a rotation. `authRefreshStorm.test.ts` owns that
    // proof, against the real auth-js. Do not let this one stand in for it.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(3600, 'stamped-fast') as never);
    getSession.mockResolvedValue({ data: { session: session(3600, 'restamped') } });

    // Served from memory for as long as the clock keeps its word.
    expect(await getAccessToken()).toBe('stamped-fast');
    expect(getSession).not.toHaveBeenCalled();

    vi.setSystemTime(NOW - 60 * 60 * 1000);
    expect(await getAccessToken()).toBe('restamped');
    expect(getSession).toHaveBeenCalledTimes(1);
  });

  it('ignores the millisecond step a time daemon takes when it cannot slew', async () => {
    // A real correction is minutes to hours. Treating jitter as one would spend
    // a storage read every time the floor reopened, for nothing.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(3600, 'stamped') as never);

    vi.setSystemTime(NOW - 500);
    expect(await getAccessToken()).toBe('stamped');
    expect(getSession).not.toHaveBeenCalled();
  });

  it('does not freeze the forced-rotation floor for the size of the jump', async () => {
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(-10) as never);
    getSession.mockResolvedValue({ data: { session: session(-10) } });
    refreshSession.mockResolvedValue({ data: { session: session(3600, 'fresh') } });

    await expect(refreshAccessToken('jwt')).resolves.toBe('fresh');

    // NTP takes the clock back an hour. `lastForceAt` now sits in the future,
    // so a floor measured as a raw subtraction stays closed for that whole hour
    // while the cache serves a token the server has already expired: every
    // request 401s and its retry is refused locally, with nothing to heal it.
    vi.setSystemTime(NOW - 60 * 60 * 1000);
    // Storage still answers with the token the server just refused, so the
    // rotation has to escalate -- which is also what restamps the expiry on the
    // corrected clock and gets the tab out of the hole.
    getSession.mockResolvedValue({ data: { session: session(3600, 'fresh') } });
    refreshSession.mockResolvedValue({ data: { session: session(3600, 'healed') } });

    await expect(refreshAccessToken('fresh')).resolves.toBe('healed');
  });
});

describe('adopting a session read asynchronously', () => {
  it('discards a reply that lands after the signed-in user changed', async () => {
    const { publishSession, sessionAdopter, getAccessToken } = await loadModule();
    publishSession(session(3600, 'alice') as never);

    // AuthContext starts a read, and a cross-tab sign-out lands before it
    // resolves. Adopting the reply anyway would put Alice's live token back and
    // send every request after it out as her.
    const adopt = sessionAdopter();
    publishSession(null);
    adopt(session(3600, 'alice') as never);

    getSession.mockResolvedValue({ data: { session: null } });
    await expect(getAccessToken()).resolves.toBeNull();
    expect(getSession).not.toHaveBeenCalled();
  });

  it('still adopts a reply when nothing changed under it', async () => {
    const { sessionAdopter, getAccessToken } = await loadModule();

    const adopt = sessionAdopter();
    adopt(session(3600, 'bootstrapped') as never);

    await expect(getAccessToken()).resolves.toBe('bootstrapped');
  });
});

describe('single-flight renewal', () => {
  it('collapses a burst of concurrent reads into one session read', async () => {
    // The bug: 12 parallel requests used to produce 9 separate network
    // refreshes, which is two thirds of Supabase's burst allowance.
    const { getAccessToken } = await loadModule();
    getSession.mockResolvedValue({ data: { session: session(3600) } });

    const tokens = await Promise.all(Array.from({ length: 12 }, () => getAccessToken()));

    expect(tokens).toEqual(Array(12).fill('jwt'));
    expect(getSession).toHaveBeenCalledTimes(1);
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('collapses concurrent 401 retries into one refresh', async () => {
    const { publishSession, refreshAccessToken } = await loadModule();
    // A 401 retry only happens after a request went out, so the cache holds the
    // token the server just refused. Starting from an empty cache would model a
    // 401 on a request that carried no credential at all, which is a different
    // situation with a different right answer.
    publishSession(session(-10) as never);
    getSession.mockResolvedValue({ data: { session: session(-10) } });
    refreshSession.mockResolvedValue({ data: { session: session(3600, 'fresh') } });

    const tokens = await Promise.all(
      Array.from({ length: 12 }, () => refreshAccessToken('jwt')),
    );

    expect(tokens).toEqual(Array(12).fill('fresh'));
    expect(refreshSession).toHaveBeenCalledTimes(1);
    // Storage is consulted first because it rotates an expired session in one
    // request where `refreshSession` costs two. Here it hands back the same
    // refused token, so the forced rotation still has to happen.
    expect(getSession).toHaveBeenCalledTimes(1);
  });

  it('does not run a second retry ladder when the first call already failed', async () => {
    // `getSession()` answers `{ session: null, error }` rather than throwing, so
    // nothing above this stops a `refreshSession()` behind it -- and that is the
    // same request over the same transport auth-js just spent ~25s of backoff
    // on. Every concurrent read is parked on this flight until it settles, so
    // the second ladder is a stall the caller pays for and learns nothing from.
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(-10) as never);
    getSession.mockResolvedValue({
      data: { session: null },
      error: new Error('Request rate limit reached'),
    });

    await expect(refreshAccessToken('jwt')).resolves.toBeNull();
    expect(getSession).toHaveBeenCalledTimes(1);
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('takes the one-request rotation when storage already has a newer token', async () => {
    // `refreshSession()` re-reads storage before it looks at anything it was
    // handed, so it spends two requests where `getSession()` spends one. When
    // the cheap call comes back with a token that is not the refused one, the
    // rotation has already happened and the expensive one is pure waste.
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(-10) as never);
    getSession.mockResolvedValue({ data: { session: session(3600, 'rotated') } });

    await expect(refreshAccessToken('jwt')).resolves.toBe('rotated');
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('does not re-arm the cache with a session that lands after sign-out', async () => {
    // The renewal is already on the wire when the user signs out. Its reply
    // still carries a live JWT, and nothing about the promise knows that the
    // account it belongs to is gone: without the generation fence it repopulates
    // the cache and every request after it goes out as the user who just left.
    const { publishSession, getAccessToken, clearAuthToken } = await loadModule();
    publishSession(session(-10) as never);
    let release: (v: unknown) => void = () => {};
    getSession.mockReturnValue(new Promise((resolve) => { release = resolve; }));

    const pending = getAccessToken();
    clearAuthToken();
    release({ data: { session: session(3600, 'late') } });

    await expect(pending).resolves.toBeNull();
    // And the cache stays empty, so the next request is unauthenticated too.
    await expect(getAccessToken()).resolves.toBeNull();
  });

  it('stops reading storage once it knows nobody is signed in', async () => {
    // The login screen makes requests too. Answering each one with a session
    // read is how a signed-out tab still spends the token budget.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(null);

    expect(await Promise.all([getAccessToken(), getAccessToken()])).toEqual([null, null]);
    expect(getSession).not.toHaveBeenCalled();
  });

  it('reads storage before asking the network, so a sibling tab rotation is free', async () => {
    // Cookie writes fire no storage event, so the cookie is the only place
    // another tab's rotation shows up.
    const { getAccessToken } = await loadModule();
    getSession.mockResolvedValue({ data: { session: session(3600, 'from-cookie') } });

    expect(await getAccessToken()).toBe('from-cookie');
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('never spends a second call rotating what the read already renewed', async () => {
    // MARGIN_MS (30s) is narrower than auth-js's EXPIRY_MARGIN_MS (90s), so a
    // token this module calls stale is one `getSession()` has already refreshed
    // by the time it answers. A `refreshSession()` behind it would be a second
    // network round trip for a token we were just handed.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(-10, 'expired') as never);
    getSession.mockResolvedValue({ data: { session: session(3600, 'renewed') } });

    expect(await getAccessToken()).toBe('renewed');
    expect(getSession).toHaveBeenCalledTimes(1);
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('renews inside the pre-expiry margin', async () => {
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(20) as never);
    getSession.mockResolvedValue({ data: { session: session(3600, 'renewed') } });

    expect(await getAccessToken()).toBe('renewed');
  });

  it('survives a rejected refresh by keeping what it holds', async () => {
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(10, 'stale') as never);
    getSession.mockRejectedValue(new Error('offline'));
    refreshSession.mockRejectedValue(new Error('offline'));

    expect(await getAccessToken()).toBe('stale');
  });
});

describe('storm bounds', () => {
  it('will not re-derive the session inside the floor', async () => {
    // A session whose expiry never moves is the clock-skew signature. Without
    // this floor every request re-enters the slow path forever; with it, an
    // unstamped or immovable expiry costs one storage read per 10s.
    const { getAccessToken } = await loadModule();
    getSession.mockResolvedValue({ data: { session: session(10, 'skewed') } });

    expect(await getAccessToken()).toBe('skewed');
    expect(getSession).toHaveBeenCalledTimes(1);

    vi.setSystemTime(NOW + 5_000);
    expect(await getAccessToken()).toBe('skewed');
    expect(getSession).toHaveBeenCalledTimes(1);

    vi.setSystemTime(NOW + 11_000);
    expect(await getAccessToken()).toBe('skewed');
    expect(getSession).toHaveBeenCalledTimes(2);
  });

  it('hands back the cached token instead of stalling behind the breaker', async () => {
    // auth-js would spend ~25s on its local retry ladder for a token we hold.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(10, 'held') as never);
    cooldownMs = 45_000;

    expect(await getAccessToken()).toBe('held');
    expect(getSession).not.toHaveBeenCalled();
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('answers a forced rotation with null rather than the refused token', async () => {
    // The caller is a 401 retry. Handing back what we hold would replay the
    // request with the token the server just refused, buying a second 401.
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(10, 'held') as never);
    cooldownMs = 45_000;

    expect(await refreshAccessToken('held')).toBeNull();
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('answers a forced rotation with null when the refresh yields no session', async () => {
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(10, 'held') as never);
    // The cheap call has to answer for the escalation to be reached at all. Left
    // unmocked it resolves `undefined`, the destructure throws, and the rotation
    // returns null from the catch -- passing this test without ever proving the
    // thing it names.
    getSession.mockResolvedValue({ data: { session: session(10, 'held') } });
    refreshSession.mockResolvedValue({ data: { session: null } });

    expect(await refreshAccessToken('held')).toBeNull();
    expect(refreshSession).toHaveBeenCalledTimes(1);
  });

  it('answers a straggling 401 from the cache, without a second rotation', async () => {
    // A burst sent with token T comes back 401 one at a time. The first rotates
    // T to U; every straggler is then holding an answer already. Reading the
    // refused token off the cache instead would make each of them conclude that
    // U had been refused and rotate again, which is the storm arriving through
    // the fix.
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(-10, 'T') as never);
    getSession.mockResolvedValue({ data: { session: session(3600, 'U') } });

    expect(await refreshAccessToken('T')).toBe('U');
    expect(getSession).toHaveBeenCalledTimes(1);

    // The stragglers, all still naming T.
    const late = await Promise.all([
      refreshAccessToken('T'), refreshAccessToken('T'), refreshAccessToken('T'),
    ]);
    expect(late).toEqual(['U', 'U', 'U']);
    expect(getSession).toHaveBeenCalledTimes(1);
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('holds off a second rotation for a token the server keeps refusing', async () => {
    // Neither the single flight nor the straggler check catches this one: the
    // retries do not overlap, and each genuinely names the token it still holds.
    // Without a floor that counts failures, a revoked token buys one rotation
    // per reconnect attempt for as long as the tab is open.
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(3600, 'revoked') as never);
    getSession.mockResolvedValue({ data: { session: session(3600, 'revoked') } });
    refreshSession.mockResolvedValue({ data: { session: session(3600, 'revoked') } });

    expect(await refreshAccessToken('revoked')).toBeNull();
    expect(refreshSession).toHaveBeenCalledTimes(1);

    // The stream backs off and tries again well before the floor is up.
    vi.setSystemTime(NOW + 6_000);
    expect(await refreshAccessToken('revoked')).toBeNull();
    expect(refreshSession).toHaveBeenCalledTimes(1);

    // Past the floor it is allowed to ask once more.
    vi.setSystemTime(NOW + 11_000);
    expect(await refreshAccessToken('revoked')).toBeNull();
    expect(refreshSession).toHaveBeenCalledTimes(2);
  });

  it('serves a read the token we hold when the rotation behind it comes back empty', async () => {
    // A rotation answers null without asking the network whenever the cool-off
    // is open or the force floor is closed. A read riding on that promise must
    // not inherit the null: the margin is what rejected the held token, so it
    // can still be minutes from actually expiring, and sending no header at all
    // turns a request that would have worked into a guaranteed 401.
    const { publishSession, getAccessToken, refreshAccessToken } = await loadModule();
    publishSession(session(20, 'nearly-stale') as never);
    getSession.mockResolvedValue({ data: { session: null } });

    // Spend the force floor, so the rotation below answers null on its own. A
    // cool-off would do it too, but then the read is served by the short-circuit
    // that checks it and never reaches the fallback this test is named for.
    await refreshAccessToken('nearly-stale');
    expect(getSession).toHaveBeenCalledTimes(1);

    const rotation = refreshAccessToken('nearly-stale');
    const read = getAccessToken();

    expect(await rotation).toBeNull();
    expect(await read).toBe('nearly-stale');
    expect(getSession).toHaveBeenCalledTimes(1);
  });

  it('does not re-arm the cache with a late reply after an account switch', async () => {
    // The fence's other half. Sign-out is the case with a test; this is the one
    // AuthContext explicitly supports -- switching accounts with no sign-out in
    // between -- and it is the dangerous one, because the stale reply carries a
    // live token rather than nothing. Adopting it would send every subsequent
    // request out authenticated as the user who just left.
    const { publishSession, getAccessToken } = await loadModule();
    publishSession(session(-10, 'stale-A') as never);

    let landReply: (value: unknown) => void = () => {};
    getSession.mockReturnValue(new Promise((resolve) => { landReply = resolve; }));
    const pending = getAccessToken();

    publishSession({
      access_token: 'token-B',
      user: { id: 'u2' },
      expires_at: Math.floor((NOW + 3_600_000) / 1000),
    } as never);

    landReply({ data: { session: session(3600, 'late-A') } });

    // The read that started as A answers with nothing rather than B's token.
    // Refusing the stale reply is only half of it: the axios interceptor stamps
    // whatever comes back with the CURRENT generation, so a token handed over
    // here would look correctly attributed while carrying a request A's UI
    // built -- a mutation included.
    expect(await pending).toBeNull();
    expect(await getAccessToken()).toBe('token-B');
  });

  it('answers a rotation with nothing when the account switched under it', async () => {
    // Same fence, the forced path, and the worse half: this caller is a 401
    // retry holding a config already built for A. Replaying it with B's token
    // is A's request executed as B.
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(session(-10, 'stale-A') as never);

    let landReply: (value: unknown) => void = () => {};
    getSession.mockReturnValue(new Promise((resolve) => { landReply = resolve; }));
    const rotation = refreshAccessToken('stale-A');

    publishSession({
      access_token: 'token-B',
      user: { id: 'u2' },
      expires_at: Math.floor((NOW + 3_600_000) / 1000),
    } as never);

    landReply({ data: { session: session(3600, 'late-A') } });
    expect(await rotation).toBeNull();
    // And it stops there. `refreshSession()` takes no argument, so escalating
    // would rotate B's stored session because A's request was refused, spending
    // a token request out of the budget the whole module is conserving.
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('never reads the session in OSS mode, however the env is configured', async () => {
    // `supabase` is built from the env vars alone, so an OSS build carrying
    // them gets a real client while AuthProvider serves the static local
    // identity. Client existence is not the mode switch (web/AGENTS.md).
    platformMode = false;
    const { getAccessToken, refreshAccessToken } = await loadModule();

    expect(await getAccessToken()).toBeNull();
    expect(await refreshAccessToken('whatever')).toBeNull();
    expect(getSession).not.toHaveBeenCalled();
    expect(refreshSession).not.toHaveBeenCalled();
  });

  it('does not ask the network to rotate for a signed-out tab', async () => {
    const { publishSession, refreshAccessToken } = await loadModule();
    publishSession(null);

    expect(await refreshAccessToken('anything')).toBeNull();
    expect(getSession).not.toHaveBeenCalled();
    expect(refreshSession).not.toHaveBeenCalled();
  });
});

describe('OSS mode', () => {
  it('resolves null when no Supabase client exists', async () => {
    vi.resetModules();
    vi.doMock('../supabase', () => ({ supabase: null }));

    const { getAccessToken } = await import('../authToken');

    expect(await getAccessToken()).toBeNull();
    vi.doUnmock('../supabase');
  });
});
