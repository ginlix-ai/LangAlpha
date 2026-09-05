import i18n from '@/i18n';
import { toast } from '@/components/ui/use-toast';
import { ToastAction } from '@/components/ui/toast';

/**
 * Detection and reporting for "this tab is running a build the server no longer
 * serves". The pre-boot half lives inline in index.html, because by the time
 * anything here could run, the bundle that failed to load is the bundle this
 * module is in. This half owns everything after the app has mounted, where a
 * surprise reload would destroy an agent turn in flight.
 */

/** Why index.html thinks the build is dead. Set there, read here. */
type StaleReason = 'resource' | 'preload';

/**
 * The one channel from the pre-boot half to this one. index.html is plain HTML
 * and never reaches tsc, so this augmentation is the only thing that makes a
 * rename on this side visible; the round-trip test covers the other side.
 */
export const STALE_BUILD_EVENT = 'la:stale-build';

declare global {
  interface Window {
    /** Set once React has mounted; index.html reads it to suppress auto-reload. */
    __LA_BOOTED__?: boolean;
    /** Set by index.html when it detects a dead build asset post-boot. */
    __LA_STALE_BUILD__?: StaleReason;
  }
  interface WindowEventMap {
    'la:stale-build': CustomEvent<StaleReason>;
  }
}

/**
 * Where the build writes content-hashed output. Two other copies of this exist
 * and neither is typechecked with it: the pre-boot classifier in index.html,
 * and the serving config that decides whether a miss 404s at all. All three
 * have to move together.
 *
 * Same-origin `/assets/` is a hard precondition, not a default. Setting
 * `VITE_CDN_BASE` moves the build off this origin, and every layer below then
 * classifies a real dead asset as somebody else's problem. See web/AGENTS.md.
 */
const BUILD_PREFIX = '/assets/';

/**
 * The build manifest, resolved against the base the build actually shipped
 * under rather than assumed at the root.
 *
 * Its serving guarantees belong to whatever serves this build, not to this
 * repo: a miss must 404 rather than fall through to the SPA shell, the hit
 * must carry `application/json`, and no edge may cache it. Break any of them
 * and this layer fails closed and silent by design — which is the right
 * failure for a user and the wrong one for an operator, hence the one-shot
 * warn below.
 */
const VERSION_URL = `${import.meta.env.BASE_URL}version.json`;
const POLL_THROTTLE_MS = 60_000;

// The first three are Chrome/Edge, Firefox and Safari failing a dynamic import.
// Lowercased at comparison time.
//
// The fourth is Vite's own, and it only became reachable once a miss started
// answering with a real 404: a route's stylesheet is preloaded through a
// <link>, which fires `load` for the SPA fallback's 200 text/html but `error` for
// a 404, and __vitePreload turns that into a throw. It names the dep as a bare
// path, not a URL, which is why mentionsBuildAsset has to match both forms.
const CHUNK_ERROR_PATTERNS = [
  'failed to fetch dynamically imported module',
  'error loading dynamically imported module',
  'importing a module script failed',
  'unable to preload css for',
];

/**
 * How long the ambient toast waits for a boundary to claim the same failure.
 *
 * index.html dispatches its event synchronously inside Vite's preloadError
 * handler — before Vite rethrows — so for a lazy chunk the toast is always
 * scheduled before React can catch the error and mount the card. Raising it
 * immediately means one event produces two notices and `silent` arrives too
 * late to stop it. Waiting is only correct because the toast is ambient by
 * definition: nothing is waiting on it, and not every dead chunk reaches a
 * boundary (Main.tsx prefetches one and swallows the rejection), so it still
 * has to fire on its own when no card claims it.
 */
const BOUNDARY_CLAIM_MS = 250;

let notified = false;
let warned = false;
let lastChecked = 0;
let pendingToast: ReturnType<typeof setTimeout> | null = null;
let activeToast: { dismiss: () => void } | null = null;

export function markBooted(): void {
  window.__LA_BOOTED__ = true;
  // Booting is the only proof a reload worked, so it is what resets the
  // pre-boot attempt counter. Without this the count is cumulative over the
  // life of the tab and the second unrelated deploy in one session lands on a
  // dead end that the first one already used up.
  try {
    sessionStorage.removeItem('__la_asset_recovery__');
  } catch {
    // Storage disabled. index.html already declines to recover in that case.
  }
}

/** The entry chunk this document actually loaded, e.g. `index-ChLW29p_.js`. */
function currentBuild(): string | null {
  // Read from the DOM rather than a compile-time constant: the entry cannot know
  // its own content hash (the hash is computed from the bundle that would carry
  // the constant). Taking the first match is only safe because the build asserts
  // there is exactly one module script in index.html — scripts/check-critical-path.mjs
  // fails the build otherwise, since a second one (an analytics or polyfill
  // loader in <head>) would be read as the entry and never match version.json,
  // giving every user a permanent "new version" prompt.
  const el = document.querySelector<HTMLScriptElement>('script[type="module"][src]');
  if (!el?.src) return null;
  try {
    return new URL(el.src, window.location.href).pathname.split('/').pop() || null;
  } catch {
    return null;
  }
}

const ABSOLUTE_URL = /https?:\/\/[^\s'")]+/g;

function mentionsBuildAsset(message: string): boolean {
  const urls = message.match(ABSOLUTE_URL) ?? [];
  const sameOrigin = urls.some((raw) => {
    try {
      const u = new URL(raw);
      return u.origin === window.location.origin && u.pathname.startsWith(BUILD_PREFIX);
    } catch {
      return false;
    }
  });
  if (sameOrigin) return true;

  // Vite names a failed CSS dep as a path (`Unable to preload CSS for
  // /assets/Chat-a1b2.css`). A bare path is same-origin by definition, so it
  // needs no origin check — but absolute URLs are stripped first, or a
  // cross-origin CDN asset would match here through its pathname.
  return new RegExp(`(^|[\\s'"(])${BUILD_PREFIX}`).test(message.replace(ABSOLUTE_URL, ''));
}

/**
 * True only for a failed build-asset load. Everything else must stay a real
 * error: a boundary that swallows any exception and offers a Reload button
 * trains everyone to reload on every crash, and deterministic render bugs then
 * get filed as "stale build".
 */
export function isStaleBuildError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? '');
  const lower = message.toLowerCase();
  if (!CHUNK_ERROR_PATTERNS.some((p) => lower.includes(p))) return false;
  // Pattern alone also matches a plain network outage, which is a different bug
  // with a different remedy, so a same-origin build URL is normally required.
  // index.html's flag stands in when the browser's message carries no URL
  // (Safari's does not) — but only here, after the message already looks like a
  // chunk failure. Checking it first would mean one flagged resource silently
  // reclassifies every later render error as a stale build, and the boundary
  // would swallow real bugs behind a reload prompt for the rest of the session.
  return mentionsBuildAsset(message) || !!window.__LA_STALE_BUILD__;
}

/**
 * Surface the stale build once. Never reloads on its own: a turn may have been
 * streaming for minutes, so the choice is the user's.
 */
export function reportStaleBuild(reason: string, options?: { silent?: boolean }): void {
  // Ahead of the latch on purpose. The boundary renders a full-pane card with
  // this same copy, and it has to be able to claim the failure even though
  // something else always reports it first — see BOUNDARY_CLAIM_MS.
  //
  // Both a pending and an already-raised toast, because the claim is not
  // bounded by that timer. App.tsx warms the route chunk on mount while the
  // setup gate is still resolving, so the import can reject seconds before the
  // boundary exists to catch it; the deferral spares the common case a visible
  // toast, and this spares the slow one a duplicate that outlives it.
  if (options?.silent) {
    if (pendingToast !== null) {
      clearTimeout(pendingToast);
      pendingToast = null;
    }
    activeToast?.dismiss();
    activeToast = null;
  }
  if (notified) return;
  notified = true;
  // Logged even though this is the expected path. Without it a genuine
  // /assets/* 404 regression is absorbed into a friendly toast and never
  // investigated.
  console.error(`[staleBuild] running a build the server no longer serves (${reason})`);

  if (options?.silent) return;

  pendingToast = setTimeout(() => {
    pendingToast = null;
    activeToast =
      toast({
        title: i18n.t('common.staleBuild.title'),
        description: i18n.t('common.staleBuild.description'),
        // Overrides the Toaster's 3s default (spread onto the Radix Toast). A
        // notice that disappears before the user looks up is not a notice.
        duration: Infinity,
        // And exempt from the toast cap, which keeps only the newest few. This
        // one is the oldest by construction, so it was always first out — three
        // later notices of any kind took the app's only Reload control with
        // them, and the latch above guarantees nothing raises it a second time.
        pinned: true,
        action: (
          <ToastAction
            altText={i18n.t('common.staleBuild.reload')}
            onClick={() => window.location.reload()}
          >
            {i18n.t('common.staleBuild.reload')}
          </ToastAction>
        ),
      }) ?? null;
  }, BOUNDARY_CLAIM_MS);
}

/**
 * Ask the server which build it is serving. Anything unclear (offline, a
 * proxy's HTML error page, a dev server with no version.json) resolves to
 * "unknown" and does nothing — never to "you are behind".
 */
export async function checkForNewBuild(): Promise<void> {
  const mine = currentBuild();
  if (!mine || notified) return;

  const now = Date.now();
  if (now - lastChecked < POLL_THROTTLE_MS) return;
  lastChecked = now;

  let res: Response;
  try {
    res = await fetch(VERSION_URL, { cache: 'no-store' });
  } catch {
    // Offline or blocked. Not evidence of anything. Scoped to the fetch alone:
    // wrapped around the parse as well, an answered-but-unreadable manifest
    // would land here and look like a tab with no network, which is the one
    // case that has to reach warnUnrecognized instead.
    return;
  }
  if (!res.ok) return;
  // A miss that fell through to the SPA shell answers 200 text/html, and the
  // shell's own <script src> contains the entry name — so a body match would
  // pass on it. Require real JSON, and drop the body rather than buffering a
  // shell we are about to discard.
  if (!(res.headers.get('content-type') ?? '').includes('application/json')) {
    void res.body?.cancel();
    return warnUnrecognized('not JSON');
  }

  let data: unknown;
  try {
    data = await res.json();
  } catch {
    return warnUnrecognized('unparseable body');
  }
  const build = (data as { build?: unknown } | null)?.build;
  if (typeof build !== 'string' || !build) return warnUnrecognized('no build id');
  if (build !== mine) reportStaleBuild('version');
}

/**
 * The one case worth a line in the console: the manifest answered, and we could
 * not read it. Everything else this module declines to act on is genuinely
 * ambiguous, but an unreadable manifest means the version layer is dead — and
 * without this it is indistinguishable from "you are up to date", forever.
 */
function warnUnrecognized(why: string): void {
  if (warned) return;
  warned = true;
  console.warn(`[staleBuild] ${VERSION_URL} is unreadable (${why}); version checks are off`);
}

/**
 * Wire the post-boot signals: index.html's event for a chunk that already died,
 * and a version check when the tab comes back to the foreground. Returns a
 * cleanup for the caller's effect.
 */
export function watchStaleBuild(): () => void {
  const onStale = (e: Event) => {
    const detail = (e as CustomEvent<string>).detail;
    reportStaleBuild(typeof detail === 'string' ? detail : 'resource');
  };
  const onVisibility = () => {
    if (document.visibilityState === 'visible') void checkForNewBuild();
  };
  // visibilitychange alone misses the two ways a tab comes back without ever
  // having been hidden: clicking into a window that stayed visible behind
  // another app, and a BFCache restore, which can run a document that has been
  // parked for days. All three land on the same 60s throttle.
  const onResume = () => void checkForNewBuild();

  window.addEventListener(STALE_BUILD_EVENT, onStale);
  document.addEventListener('visibilitychange', onVisibility);
  window.addEventListener('pageshow', onResume);
  window.addEventListener('focus', onResume);

  // index.html may have fired before React mounted and the listener attached.
  if (window.__LA_STALE_BUILD__) reportStaleBuild(window.__LA_STALE_BUILD__);

  return () => {
    window.removeEventListener(STALE_BUILD_EVENT, onStale);
    document.removeEventListener('visibilitychange', onVisibility);
    window.removeEventListener('pageshow', onResume);
    window.removeEventListener('focus', onResume);
  };
}

/** Test seam — the module-level dedupe and throttle outlive a test file. */
export function __resetStaleBuildForTests(): void {
  notified = false;
  warned = false;
  lastChecked = 0;
  if (pendingToast !== null) clearTimeout(pendingToast);
  pendingToast = null;
  activeToast = null;
  delete window.__LA_STALE_BUILD__;
}
