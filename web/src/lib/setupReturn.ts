/**
 * The page that sent a user into setup and expects them back.
 *
 * The account portal (`PLATFORM_URL`) links to `/setup/method?return=<URL>` so
 * a user can connect a provider here and carry on where they were. The steps
 * navigate with router state and drop the query, so the target is captured once
 * on entry and kept in this tab's sessionStorage, where a reload mid-setup still
 * finds it. It lasts an hour, and reaching any page of the app outside setup
 * ends the visit and clears it, so a later setup opened from Settings behaves as
 * it always has.
 *
 * Only a page under the portal's `/onboarding/` is accepted: the value arrives
 * in a URL anyone can craft, so anything wider is an open redirect.
 */
import { PLATFORM_URL } from '@/config/supportLinks';

/** The query param that names where to go back to. */
const RETURN_PARAM = 'return';
/** The query param added on the way back, naming the provider just connected. */
const CONNECTED_PARAM = 'connected';
/** The sessionStorage key holding `{ url, at }`. */
export const SETUP_RETURN_KEY = 'setup:return';
const TTL_MS = 60 * 60 * 1000;

interface StoredReturn {
  url: string;
  at: number;
}

const WEB_SCHEMES = new Set(['http:', 'https:']);

/**
 * The parsed target when `raw` is a page under `<base>/onboarding/` on the
 * base's origin, otherwise null. `base` is the account portal's as
 * `PLATFORM_URL` gives it (null when there is none), and `origin` is this
 * app's, which a path-shaped base resolves against.
 *
 * `raw` is parsed with no base, so a relative or protocol-relative value fails
 * outright, and the path is compared after parsing, which has already collapsed
 * dot segments (`/onboarding/../x` is `/x` by then, encoded dots included).
 */
export function allowedTarget(raw: string, base: string | null, origin: string): URL | null {
  if (!raw || !base) return null;
  let target: URL;
  let root: URL;
  try {
    target = new URL(raw);
    root = new URL(base, origin);
  } catch {
    return null;
  }
  if (!WEB_SCHEMES.has(target.protocol) || !WEB_SCHEMES.has(root.protocol)) return null;
  if (target.username || target.password) return null;
  if (target.origin !== root.origin) return null;
  const prefix = `${root.pathname.replace(/\/+$/, '')}/onboarding/`;
  return target.pathname.startsWith(prefix) ? target : null;
}

/** `raw` checked against this build's portal from this page. */
function allowedHere(raw: string): URL | null {
  return allowedTarget(raw, PLATFORM_URL, window.location.origin);
}

function parseStored(raw: string): StoredReturn | null {
  try {
    const value: unknown = JSON.parse(raw);
    if (!value || typeof value !== 'object') return null;
    const { url, at } = value as Record<string, unknown>;
    return typeof url === 'string' && typeof at === 'number' && Number.isFinite(at)
      ? { url, at }
      : null;
  } catch {
    return null;
  }
}

export function clearSetupReturn(): void {
  try {
    sessionStorage.removeItem(SETUP_RETURN_KEY);
  } catch {
    // Storage unavailable: nothing was kept to clear.
  }
}

/**
 * Keeps the `return` param of `search` when it is allowed, and gives the return
 * now kept: this one, or one kept earlier when `search` has none or an invalid
 * one, which leaves it in place.
 */
export function captureSetupReturn(search: string): string | null {
  const raw = new URLSearchParams(search).get(RETURN_PARAM);
  const target = raw === null ? null : allowedHere(raw);
  if (target) {
    try {
      sessionStorage.setItem(
        SETUP_RETURN_KEY,
        JSON.stringify({ url: target.href, at: Date.now() } satisfies StoredReturn),
      );
      return target.href;
    } catch {
      // Storage unavailable: nothing new is kept.
    }
  }
  return readSetupReturn();
}

/**
 * The kept return, or null. The stored URL is checked again on the way out,
 * since anything on this origin can write the key, and an expired or unreadable
 * one is removed.
 */
export function readSetupReturn(): string | null {
  let raw: string | null;
  try {
    raw = sessionStorage.getItem(SETUP_RETURN_KEY);
  } catch {
    return null;
  }
  if (raw === null) return null;
  const stored = parseStored(raw);
  const target = stored && Date.now() - stored.at < TTL_MS ? allowedHere(stored.url) : null;
  if (!target) {
    clearSetupReturn();
    return null;
  }
  return target.href;
}

/**
 * `search` without the `return` param, other params kept, or null when it had
 * none. For taking the param out of the address bar once it is captured.
 */
export function withoutSetupReturnParam(search: string): string | null {
  const params = new URLSearchParams(search);
  if (!params.has(RETURN_PARAM)) return null;
  params.delete(RETURN_PARAM);
  const rest = params.toString();
  return rest ? `?${rest}` : '';
}

/**
 * Sends the user back to the kept return, naming `connected` in its query when
 * a provider was just connected (the rest of the query and the fragment kept),
 * and says whether there was one to go to. The return is removed first, so it
 * is followed at most once. A full page load, since the portal is another app
 * even on this origin.
 */
export function returnFromSetup(connected?: string | null): boolean {
  const url = readSetupReturn();
  if (url === null) return false;
  clearSetupReturn();
  const target = new URL(url);
  if (connected) target.searchParams.set(CONNECTED_PARAM, connected);
  window.location.assign(target.href);
  return true;
}
