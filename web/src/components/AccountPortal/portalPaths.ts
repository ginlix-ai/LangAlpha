import { PLATFORM_URL } from '@/config/supportLinks';
import { inAppPath } from '@/lib/inAppPath';

/**
 * The account portal's pages are sections of Settings:
 * `/settings?tab=console&page=integrations` shows the portal's `/integrations`.
 * `tab=console` is not one of the native sections, so a build with no portal
 * reads it as no section at all.
 */
export const PORTAL_TAB = 'console';

/** A portal path as the app keeps it, or null for one that would leave the
 *  portal. Without the fragment, which never reaches the address it is
 *  compared with. An encoded slash, backslash or dot is refused too: a proxy
 *  that decodes before it routes would read `a%2F..%2F..%2Fx` as a step out
 *  of the portal's base. */
export function portalPathOf(raw: string): string | null {
  const path = inAppPath(raw)?.split('#')[0];
  if (!path || /%(2f|5c|2e)/i.test(path.split('?')[0])) return null;
  return path;
}

/** The Settings address showing the portal's `portalPath`. */
export function portalSettingsPath(portalPath: string): string {
  return `/settings?tab=${PORTAL_TAB}&page=${encodeURIComponent(portalPath.slice(1))}`;
}

/** The portal path Settings' address names, or null when it names a section of its own. */
export function readPortalPage(params: URLSearchParams): string | null {
  if (!PLATFORM_URL || params.get('tab') !== PORTAL_TAB) return null;
  return portalPathOf(`/${params.get('page') ?? ''}`) ?? '/';
}

/** The portal page at `portalPath`, at top level. */
export function portalUrl(portalPath: string): string {
  return `${PLATFORM_URL ?? ''}${portalPath}`;
}

/**
 * The portal path a link's resolved `href` points at, or null for a URL
 * outside the portal. Compared as URLs, so a portal on a path of this origin
 * and one on its own origin read the same, and `/account-x` is not under
 * `/account`. Null too for a link with a fragment: the Settings address
 * cannot carry one, and a page that reads its fragment (an invitation's
 * token) would open without it.
 */
export function portalPathOfHref(href: string, page: string = window.location.href): string | null {
  if (!PLATFORM_URL) return null;
  let url: URL;
  let root: URL;
  try {
    url = new URL(href, page);
    root = new URL(PLATFORM_URL, page);
  } catch {
    return null;
  }
  if (url.origin !== root.origin || url.hash) return null;
  const base = root.pathname.replace(/\/$/, '');
  if (url.pathname !== base && !url.pathname.startsWith(`${base}/`)) return null;
  return portalPathOf(`${url.pathname.slice(base.length) || '/'}${url.search}`);
}

/** Of `paths`, the one `page` is under, the longest where several are: an
 *  organization's overview is the prefix of each of its pages. On a segment
 *  boundary, so `/usage-export` is not under `/usage`. */
export function activePortalPath(paths: readonly string[], page: string): string | null {
  const at = page.split('?')[0];
  let best: string | null = null;
  for (const path of paths) {
    const base = path.split('?')[0];
    const under = at === base || at.startsWith(base.endsWith('/') ? base : `${base}/`);
    if (under && (best === null || base.length > best.split('?')[0].length)) best = path;
  }
  return best;
}
