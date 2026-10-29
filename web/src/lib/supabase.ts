import { AuthClient, type GoTrueClient } from '@supabase/supabase-js';
import type { CookieOptions, CookieOptionsWithName } from '@supabase/ssr';
// Not exported from the package root, but @supabase/ssr has no `exports` map,
// so they import by path and a release that moves them fails the build. Reusing
// them keeps the cookie format in step with createBrowserClient's, which any
// other app reading this cookie on the origin also parses.
import { createStorageFromOptions } from '@supabase/ssr/dist/module/cookies.js';
import { VERSION as SSR_VERSION } from '@supabase/ssr/dist/module/version.js';
import { authFetch } from './authFetch';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseKey = import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY;
/**
 * Parent domain for first-party cookies, via the shared VITE_COOKIE_DOMAIN knob
 * (also scopes the locale cookie). Unset → host-only (the default). Set to a
 * parent domain so every subdomain shares one session — SSO.
 */
const cookieDomain = import.meta.env.VITE_COOKIE_DOMAIN as string | undefined;

if (supabaseUrl && !supabaseKey) {
  console.warn('[supabase] VITE_SUPABASE_URL is set but VITE_SUPABASE_PUBLISHABLE_KEY is missing');
} else if (!supabaseUrl && supabaseKey) {
  console.warn('[supabase] VITE_SUPABASE_PUBLISHABLE_KEY is set but VITE_SUPABASE_URL is missing');
}

const isBrowser = typeof window !== 'undefined' && typeof window.document !== 'undefined';
const isHttps = typeof window !== 'undefined' && window.location.protocol === 'https:';

function parseCookies(): { name: string; value: string }[] {
  if (typeof document === 'undefined' || !document.cookie) return [];
  return document.cookie.split('; ').filter(Boolean).map((part) => {
    const eq = part.indexOf('=');
    return eq === -1
      ? { name: part, value: '' }
      : { name: decodeURIComponent(part.slice(0, eq)), value: decodeURIComponent(part.slice(eq + 1)) };
  });
}

function writeCookie(name: string, value: string, options: CookieOptions = {}) {
  if (typeof document === 'undefined') return;
  const parts = [`${encodeURIComponent(name)}=${encodeURIComponent(value)}`];
  if (options.maxAge != null) parts.push(`Max-Age=${options.maxAge}`);
  if (options.expires) parts.push(`Expires=${new Date(options.expires).toUTCString()}`);
  parts.push(`Path=${options.path ?? '/'}`);
  if (options.domain) parts.push(`Domain=${options.domain}`);
  parts.push(`SameSite=${options.sameSite ?? 'Lax'}`);
  if (options.secure ?? isHttps) parts.push('Secure');
  document.cookie = parts.join('; ');
}

/** Also the auth storage key, which is how createBrowserClient derives it. */
const cookieOptions: CookieOptionsWithName = {
  name: 'langalpha-auth',
  path: '/',
  sameSite: 'lax',
  secure: isHttps,
  ...(cookieDomain ? { domain: cookieDomain } : {}),
};

/**
 * Everything the app reaches through Supabase. Only auth, so nothing can
 * reach for `.from()` or `.channel()` and pull the rest of the SDK back in.
 */
export interface SupabaseAuth {
  readonly auth: GoTrueClient;
}

/**
 * The auth client createBrowserClient builds, without the rest of supabase-js.
 *
 * Its postgrest, realtime, storage and functions clients are never called here
 * but rode first paint. These are the options supabase-js's
 * `_initSupabaseAuthClient` passes after createBrowserClient's overrides, so a
 * session either build wrote reads back in the other (pinned by
 * `supabaseCookies.test.ts`). The ones it leaves undefined, `lock` included,
 * keep auth-js on its lockless defaults in both.
 */
function createAuthClient(url: string, key: string): SupabaseAuth {
  const { storage } = createStorageFromOptions(
    {
      cookieEncoding: 'base64url',
      cookieOptions,
      cookies: {
        getAll: parseCookies,
        setAll(cookiesToSet) {
          cookiesToSet.forEach(({ name, value, options }) => writeCookie(name, value, options));
        },
      },
    },
    false,
  );
  const trimmed = url.trim();
  const base = trimmed.endsWith('/') ? trimmed : `${trimmed}/`;
  return {
    auth: new AuthClient({
      url: new URL('auth/v1', base).href,
      headers: {
        Authorization: `Bearer ${key}`,
        apikey: key,
        'X-Client-Info': `supabase-ssr/${SSR_VERSION} createBrowserClient`,
      },
      storageKey: cookieOptions.name,
      flowType: 'pkce',
      autoRefreshToken: isBrowser,
      detectSessionInUrl: isBrowser,
      persistSession: true,
      storage,
      fetch: authFetch,
    }),
  };
}

// Only create a real client when fully configured; otherwise export null.
// AuthContext already short-circuits to local-dev mode when the URL is
// missing, so no code path will call supabase.auth.* in that case. Module
// scope makes it the one instance per page createBrowserClient would cache, and
// `hot` keeps it one when a dev edit re-runs this module, as that cache did.
const hot: { supabase?: SupabaseAuth | null } | undefined = import.meta.hot?.data;
export const supabase: SupabaseAuth | null =
  supabaseUrl && supabaseKey ? (hot?.supabase ?? createAuthClient(supabaseUrl, supabaseKey)) : null;
if (hot) hot.supabase = supabase;
