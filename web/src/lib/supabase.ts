import { createBrowserClient, type CookieOptions } from '@supabase/ssr';
import type { SupabaseClient } from '@supabase/supabase-js';

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

// Only create a real client when fully configured; otherwise export null.
// AuthContext already short-circuits to local-dev mode when the URL is
// missing, so no code path will call supabase.auth.* in that case.
export const supabase: SupabaseClient | null =
  supabaseUrl && supabaseKey
    ? createBrowserClient(supabaseUrl, supabaseKey, {
        cookieOptions: {
          name: 'langalpha-auth',
          path: '/',
          sameSite: 'lax',
          secure: isHttps,
          ...(cookieDomain ? { domain: cookieDomain } : {}),
        },
        cookies: {
          getAll: parseCookies,
          setAll(cookiesToSet) {
            cookiesToSet.forEach(({ name, value, options }) => writeCookie(name, value, options));
          },
        },
      })
    : null;
