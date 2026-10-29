/** Stands in for the app's origin, so the check below needs no `window`. */
const IN_APP_BASE = 'http://in-app.invalid';

/**
 * A path inside this app from a URL param, or null for anything that would
 * leave it. Every param that names where to go next (`?redirect=` after sign-in,
 * the market view's `returnTo`) arrives in a URL anyone can craft, so each is an
 * open redirect unless it is read through here.
 *
 * The resolve catches schemes, hosts, and the tabs and newlines a browser
 * strips. The path it yields is checked after it, because dot segments collapse
 * during the resolve: `/..//host` comes out as `//host`, which a browser reads
 * as another host, as it does `/\host`. A consumer that decodes once would do
 * the same with `/%2F%2Fhost`.
 */
export function inAppPath(raw: string | null): string | null {
  if (!raw || !raw.startsWith('/')) return null;
  let url: URL;
  try {
    url = new URL(raw, IN_APP_BASE);
  } catch {
    return null;
  }
  if (url.origin !== IN_APP_BASE) return null;
  const path = url.pathname + url.search + url.hash;
  let decoded: string;
  try {
    decoded = decodeURIComponent(path);
  } catch {
    return null;
  }
  return /^\/[\\/]/.test(path) || /^\/[\\/]/.test(decoded) ? null : path;
}
