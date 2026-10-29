/**
 * The session cookie is a contract with every build that ever wrote one.
 *
 * lib/supabase builds the auth client by hand rather than through
 * createBrowserClient, and nothing else says the two write the same cookies. A
 * user signed in on the previous build has to come back signed in, a PKCE
 * sign-in started on one build has to finish on the next, and any other app
 * reading the same cookie name has to keep parsing it. So each case drives one
 * auth call through both clients against the same fake GoTrue and compares what
 * reached `document.cookie`, byte for byte, attributes included.
 */
import { createBrowserClient, type CookieOptions } from '@supabase/ssr';
import type { GoTrueClient } from '@supabase/supabase-js';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

const SUPABASE_URL = 'https://project.supabase.co';
const SUPABASE_KEY = 'sb_publishable_test';
const COOKIE = 'langalpha-auth';
const NOW = new Date('2026-06-01T12:00:00Z');

type Build = 'previous' | 'current';

/**
 * The previous build's client, as lib/supabase constructed it before it stopped
 * using createBrowserClient. `isSingleton: false` is the one addition: the
 * library's page-wide cache would otherwise hand every case the first client,
 * and it changes nothing about storage.
 */
async function previousBuildAuth(): Promise<GoTrueClient> {
  const { authFetch } = await import('../authFetch');
  const parseCookies = () => {
    if (!document.cookie) return [];
    return document.cookie.split('; ').filter(Boolean).map((part) => {
      const eq = part.indexOf('=');
      return eq === -1
        ? { name: part, value: '' }
        : { name: decodeURIComponent(part.slice(0, eq)), value: decodeURIComponent(part.slice(eq + 1)) };
    });
  };
  const writeCookie = (name: string, value: string, options: CookieOptions = {}) => {
    const parts = [`${encodeURIComponent(name)}=${encodeURIComponent(value)}`];
    if (options.maxAge != null) parts.push(`Max-Age=${options.maxAge}`);
    if (options.expires) parts.push(`Expires=${new Date(options.expires).toUTCString()}`);
    parts.push(`Path=${options.path ?? '/'}`);
    if (options.domain) parts.push(`Domain=${options.domain}`);
    parts.push(`SameSite=${options.sameSite ?? 'Lax'}`);
    if (options.secure ?? false) parts.push('Secure');
    document.cookie = parts.join('; ');
  };
  return createBrowserClient(SUPABASE_URL, SUPABASE_KEY, {
    isSingleton: false,
    global: { fetch: authFetch },
    cookieOptions: { name: COOKIE, path: '/', sameSite: 'lax', secure: false },
    cookies: {
      getAll: parseCookies,
      setAll(cookiesToSet) {
        cookiesToSet.forEach(({ name, value, options }) => writeCookie(name, value, options));
      },
    },
  }).auth;
}

async function currentBuildAuth(): Promise<GoTrueClient> {
  const { supabase } = await import('../supabase');
  return supabase!.auth;
}

// ---------------------------------------------------------------------------
// A fake GoTrue that answers deterministically, so two runs are comparable.

interface Seen { method: string; url: string; headers: [string, string][]; body: string }
let seen: Seen[] = [];
/** How large a `user_metadata` the next session carries, to force chunking. */
let metadataSize = 0;

function jwt(sub: string, tag: string) {
  const enc = (o: object) => btoa(JSON.stringify(o)).replace(/=+$/, '');
  const now = Math.floor(NOW.getTime() / 1000);
  return `${enc({ alg: 'HS256', typ: 'JWT' })}.${enc({ sub, role: 'authenticated', iat: now, exp: now + 3600, tag })}.sig`;
}

function sessionBody(tag: string) {
  return {
    access_token: jwt('user-1', tag),
    refresh_token: `refresh-${tag}`,
    token_type: 'bearer',
    expires_in: 3600,
    // authFetch strips this so the expiry is recomputed on the local clock.
    expires_at: Math.floor(NOW.getTime() / 1000) + 3600,
    user: {
      id: 'user-1',
      aud: 'authenticated',
      role: 'authenticated',
      email: 'someone@example.com',
      app_metadata: { provider: 'email' },
      user_metadata: { name: 'Some One', note: 'x'.repeat(metadataSize) },
      created_at: '2026-01-01T00:00:00Z',
    },
  };
}

async function goTrue(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const req = new Request(input, init);
  const body = req.method === 'GET' ? '' : await req.text();
  seen.push({
    method: req.method,
    url: req.url,
    headers: [...req.headers.entries()].sort(([a], [b]) => a.localeCompare(b)),
    body,
  });
  const url = new URL(req.url);
  const json = (payload: unknown) => new Response(JSON.stringify(payload), {
    status: 200, headers: { 'Content-Type': 'application/json' },
  });
  if (url.pathname === '/auth/v1/token') return json(sessionBody(url.searchParams.get('grant_type')!));
  if (url.pathname === '/auth/v1/logout') return new Response(null, { status: 204 });
  return new Response('{}', { status: 404, headers: { 'Content-Type': 'application/json' } });
}

// ---------------------------------------------------------------------------
// Every assignment to document.cookie, as the raw string the browser receives.

let writes: string[] = [];
const cookieAccessor = Object.getOwnPropertyDescriptor(Document.prototype, 'cookie')!;

function clearJar() {
  for (const part of document.cookie.split('; ').filter(Boolean)) {
    const name = part.slice(0, part.indexOf('='));
    cookieAccessor.set!.call(document, `${name}=; Max-Age=0; Path=/`);
  }
}

function jarNames(): string[] {
  return document.cookie.split('; ').filter(Boolean).map((p) => decodeURIComponent(p.slice(0, p.indexOf('=')))).sort();
}

/** Cookie names each write targets, for asserting layout rather than bytes. */
const namesOf = (w: string[]) => w.map((s) => decodeURIComponent(s.slice(0, s.indexOf('='))));

beforeAll(() => {
  Object.defineProperty(document, 'cookie', {
    configurable: true,
    get: () => cookieAccessor.get!.call(document),
    set: (value: string) => {
      writes.push(value);
      cookieAccessor.set!.call(document, value);
    },
  });
});

afterAll(() => {
  delete (document as { cookie?: string }).cookie;
});

const clients: GoTrueClient[] = [];

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(NOW);
  vi.stubEnv('VITE_SUPABASE_URL', SUPABASE_URL);
  vi.stubEnv('VITE_SUPABASE_PUBLISHABLE_KEY', SUPABASE_KEY);
  vi.stubGlobal('fetch', vi.fn(goTrue));
  // A shared channel name would let one case's client hear the next one's
  // events. Storage is what is under test, not cross-tab fan-out.
  vi.stubGlobal('BroadcastChannel', undefined);
  // auth-js counts instances per storage key for the life of the process; each
  // case builds fresh clients on purpose.
  const warn = console.warn;
  vi.spyOn(console, 'warn').mockImplementation((...args: unknown[]) => {
    if (String(args[0]).includes('Multiple GoTrueClient instances')) return;
    warn(...args);
  });
  seen = [];
  metadataSize = 0;
  window.history.replaceState(null, '', '/');
  clearJar();
});

afterEach(async () => {
  await Promise.all(clients.splice(0).map((c) => c.stopAutoRefresh()));
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

/** A client of `build`, fully initialised, on an otherwise fresh module graph. */
async function client(build: Build): Promise<GoTrueClient> {
  vi.resetModules();
  const auth = build === 'previous' ? await previousBuildAuth() : await currentBuildAuth();
  clients.push(auth);
  await auth.initialize();
  return auth;
}

/**
 * Run `drive` against one build from an empty jar and return the cookie writes
 * it caused, plus the requests it made.
 */
async function capture(build: Build, drive: (auth: GoTrueClient) => Promise<unknown>) {
  clearJar();
  const auth = await client(build);
  writes = [];
  seen = [];
  await drive(auth);
  return { writes: [...writes], seen: [...seen] };
}

const signIn = (auth: GoTrueClient) =>
  auth.signInWithPassword({ email: 'someone@example.com', password: 'hunter22' });

describe('the session cookie, previous build vs this one', () => {
  it('a sign-in writes the same cookie and sends the same request', async () => {
    const previous = await capture('previous', signIn);
    const current = await capture('current', signIn);

    expect(current.writes).toEqual(previous.writes);
    expect(current.seen).toEqual(previous.seen);
    expect(namesOf(current.writes)).toEqual([COOKIE]);
    expect(current.writes[0]).toMatch(/^langalpha-auth=base64-[A-Za-z0-9_-]+; Max-Age=34560000; Path=\/; SameSite=lax$/);
    // The request carries the headers supabase-js set on its auth client.
    const headers = Object.fromEntries(current.seen[0].headers);
    expect(headers).toMatchObject({
      apikey: SUPABASE_KEY,
      authorization: `Bearer ${SUPABASE_KEY}`,
      'x-client-info': expect.stringMatching(/^supabase-ssr\/\S+ createBrowserClient$/),
    });
    expect(current.seen[0].url).toBe(`${SUPABASE_URL}/auth/v1/token?grant_type=password`);
  });

  it('a session too large for one cookie chunks into the same .0/.1 pair', async () => {
    metadataSize = 4000;
    const previous = await capture('previous', signIn);
    const current = await capture('current', signIn);

    expect(current.writes).toEqual(previous.writes);
    expect(namesOf(current.writes)).toEqual([`${COOKIE}.0`, `${COOKIE}.1`]);
    expect(current.writes[0]).toMatch(/^langalpha-auth\.0=base64-/);
  });

  it('a refresh that shrinks the session clears the stale chunks the same way', async () => {
    const refreshAfterLargeSignIn = async (auth: GoTrueClient) => {
      metadataSize = 4000;
      await signIn(auth);
      metadataSize = 0;
      writes = [];
      await auth.refreshSession();
    };
    const previous = await capture('previous', refreshAfterLargeSignIn);
    const current = await capture('current', refreshAfterLargeSignIn);

    expect(current.writes).toEqual(previous.writes);
    expect(namesOf(current.writes)).toEqual([`${COOKIE}.0`, `${COOKIE}.1`, COOKIE]);
    expect(jarNames()).toEqual([COOKIE]);
  });

  it('sign-out removes the same cookies', async () => {
    const signOutAfterSignIn = async (auth: GoTrueClient) => {
      metadataSize = 4000;
      await signIn(auth);
      writes = [];
      await auth.signOut();
    };
    const previous = await capture('previous', signOutAfterSignIn);
    const current = await capture('current', signOutAfterSignIn);

    expect(current.writes).toEqual(previous.writes);
    expect(current.writes.every((w) => w.includes('Max-Age=0'))).toBe(true);
    expect(jarNames()).toEqual([]);
  });

  it.each([
    ['previous', 'current'],
    ['current', 'previous'],
  ] as const)('a session the %s build wrote is read back by the %s one', async (writer, reader) => {
    metadataSize = 4000;
    const written = await signIn(await client(writer));
    seen = [];

    const { data } = await (await client(reader)).getSession();

    expect(data.session?.access_token).toBe(written.data.session?.access_token);
    expect(data.session?.user.user_metadata.note).toHaveLength(4000);
    // Read from the cookie, not fetched: nothing was stale.
    expect(seen).toEqual([]);
  });
});

describe('the PKCE verifier, previous build vs this one', () => {
  const startOAuth = (auth: GoTrueClient) =>
    auth.signInWithOAuth({
      provider: 'google',
      options: { redirectTo: 'http://localhost:3000/callback', skipBrowserRedirect: true },
    });

  /** Fixed randomness, so the verifier and flow id match across builds. */
  function deterministicCrypto() {
    vi.spyOn(crypto, 'getRandomValues').mockImplementation(<T extends ArrayBufferView | null>(array: T): T => {
      const bytes = new Uint8Array(array!.buffer, array!.byteOffset, array!.byteLength);
      bytes.forEach((_, i) => { bytes[i] = (i * 37 + 11) & 0xff; });
      return array;
    });
  }

  it('starting a sign-in writes the same verifier cookies', async () => {
    deterministicCrypto();
    const previous = await capture('previous', startOAuth);
    const current = await capture('current', startOAuth);

    expect(current.writes).toEqual(previous.writes);
    expect(namesOf(current.writes)).toEqual([
      expect.stringMatching(/^langalpha-auth-flow-[0-9a-f]{32}-code-verifier$/),
      `${COOKIE}-flows-code-verifier`,
      `${COOKIE}-code-verifier`,
    ]);
  });

  it('a sign-in the previous build started finishes at /callback on this one', async () => {
    const { data } = await startOAuth(await client('previous'));
    const authorize = new URL(data.url!);

    // GoTrue sends the browser back with only the code (the flow id rides the
    // redirect only under an experimental flag this app does not set), so the
    // exchange reads the fixed verifier key. This build loads on that URL and
    // exchanges it while initialising, exactly as /callback does.
    window.history.replaceState(null, '', '/callback?code=auth-code');
    seen = [];
    const auth = await client('current');

    const exchange = seen.find((r) => r.url.endsWith('/auth/v1/token?grant_type=pkce'));
    const { auth_code, code_verifier } = JSON.parse(exchange!.body);
    expect(auth_code).toBe('auth-code');
    // The server's check: the verifier hashes to the challenge sent at sign-in.
    const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(code_verifier)));
    const challenge = btoa(String.fromCharCode(...digest)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    expect(challenge).toBe(authorize.searchParams.get('code_challenge'));

    const { data: after } = await auth.getSession();
    expect(after.session?.access_token).toBe(jwt('user-1', 'pkce'));
    expect(jarNames()).toContain(COOKIE);
    // The verifier it used is spent.
    expect(jarNames()).not.toContain(`${COOKIE}-code-verifier`);
  });
});
