// Shared locale helpers — deliberately standalone (no i18next/react-i18next
// import) so hooks/components that are unit-tested with react-i18next mocked can
// import these without booting i18n.
//
// The `locale` cookie is the single client-side carrier for locale, and because
// a cookie is server/edge-readable (unlike localStorage) the same choice can
// also drive server-side routing — e.g. a locale-aware redirect at a reverse
// proxy. Host-only by default; set VITE_COOKIE_DOMAIN — the same knob that
// scopes the auth cookie — to share it across subdomains.
export const SUPPORTED_LOCALES = ['en-US', 'zh-CN'] as const;
export type Locale = (typeof SUPPORTED_LOCALES)[number];

export const isSupported = (v: string | null | undefined): v is Locale =>
  !!v && (SUPPORTED_LOCALES as readonly string[]).includes(v);

const COOKIE_DOMAIN: string = import.meta.env.VITE_COOKIE_DOMAIN ?? '';

export function getLocaleCookie(): Locale | null {
  if (typeof document === 'undefined') return null;
  const m = document.cookie.match(/(?:^|;\s*)locale=([^;]+)/);
  if (!m) return null;
  let v: string;
  try {
    v = decodeURIComponent(m[1]);
  } catch {
    // Malformed %XX — treat as absent. This runs at module load (i18n init),
    // so throwing here would white-screen the app with no recovery path.
    return null;
  }
  return isSupported(v) ? v : null;
}

export function setLocaleCookie(locale: string): void {
  if (typeof document === 'undefined' || !isSupported(locale)) return;
  const attrs = [`locale=${locale}`, 'path=/', 'max-age=31536000', 'samesite=lax'];
  if (COOKIE_DOMAIN) attrs.push(`domain=${COOKIE_DOMAIN}`);
  if (window.location.protocol === 'https:') attrs.push('secure');
  document.cookie = attrs.join('; ');
}

// Resolution order: cookie (explicit / DB-mirrored choice) → browser language
// (exact, then prefix) → English.
export function detectLocale(): string {
  const cookie = getLocaleCookie();
  if (cookie) return cookie;
  const browserLang = typeof navigator !== 'undefined' ? navigator.language : '';
  if (isSupported(browserLang)) return browserLang;
  const prefix = browserLang.split('-')[0];
  const prefixMatch = SUPPORTED_LOCALES.find((l) => l.startsWith(prefix + '-'));
  return prefixMatch || 'en-US';
}
