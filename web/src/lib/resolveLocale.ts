// Dependency-free on purpose: scripts/locale-preload.ts serialises this
// function into index.html's inline preload, so it may not close over anything
// outside its own body.

/** The last resort when neither the cookie nor the browser names a catalog. */
export const DEFAULT_LOCALE = 'en-US';

/**
 * The locale a visitor gets: the `locale` cookie, then the browser language
 * (exact, then by language prefix), then the fallback. Both the app and the
 * preload run this one function, so the catalog the page fetches before the
 * bundle runs is the one the app boots with. The cookie is checked against the
 * list rather than looked up in it, so `locale=constructor` is just unknown.
 */
export function resolveLocale<L extends string>(
  cookie: string,
  language: string,
  locales: readonly L[],
  fallback: L,
): L {
  const exact = (v: string) => locales.find((l) => l === v);
  let fromCookie = '';
  const m = cookie.match(/(?:^|;\s*)locale=([^;]+)/);
  if (m) {
    try {
      fromCookie = decodeURIComponent(m[1]);
    } catch {
      // Malformed %XX reads as no cookie; throwing would white-screen i18n init.
    }
  }
  return (
    exact(fromCookie) ??
    exact(language) ??
    locales.find((l) => l.startsWith(language.split('-')[0] + '-')) ??
    fallback
  );
}
