/**
 * Sandbox stats, packages and preview endpoints.
 */
import { api, ApiError } from '@/api/client';
import { apiErrorCoded, apiErrorDetail } from '@/api/errors';
import type { BrowserRelease } from '../../components/filePanel/browserProtocol';

export async function getSandboxStats(workspaceId: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/sandbox/stats`);
  return data;
}

export async function installSandboxPackages(workspaceId: string, packages: string[]) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/packages`, { packages });
  return data;
}

export async function refreshWorkspace(workspaceId: string) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/refresh`);
  return data;
}

/**
 * Why a preview could not be shown. `portInUse` is another program holding
 * the port, which starting the app again cannot fix; anything else reads as
 * an app that is not answering.
 */
export type PreviewFailure = 'offline' | 'portInUse';

/** The failure a preview route's error stands for, by the code the server gives it. */
export function previewFailure(err: unknown): PreviewFailure {
  return apiErrorCoded(err, 'port_in_use') ? 'portInUse' : 'offline';
}

export async function getPreviewUrl(workspaceId: string, port: number, command?: string, force?: boolean) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/preview-url`, {
    port,
    ...(command && { command }),
    ...(force && { force: true }),
  });
  return data;
}

/** Only a running browser has somewhere to watch it. */
export type BrowserViewAnswer =
  | {
    state: 'running';
    /** Where the live stream opens; a short-lived key, held in memory only. */
    url: string;
    /** The same stream from an app on the computer itself, past the server:
     *  a machine of the user's own, to its owner. */
    local_url?: string | null;
  }
  /** `low_memory`: an open the computer refused because its memory is short.
   *  `no_chrome`: a machine of the user's own with no Chrome-family browser,
   *  and `installing` one downloading Chrome for Testing. */
  | { state: 'idle' | 'low_memory' | 'stopped' | 'outdated' | 'no_chrome' | 'installing' | 'unavailable' };

/** A server that has no browser routes yet, as a web deployed ahead of its backend
 *  meets. Asking again cannot help there, so it reads as a browser not on offer.
 *  A route the server has answers a workspace it cannot find in words of its own,
 *  which stay the caller's to read. */
function routeMissing(err: unknown): boolean {
  return err instanceof ApiError && err.status === 404 && apiErrorDetail(err) === 'Not Found';
}

/** How long a view request may take before the tab gives up and offers to try
 *  again: a look asks the machine once, while a start may first bring a stopped
 *  computer up and then wait for Chrome. */
const BROWSER_LOOK_TIMEOUT_MS = 45_000;
const BROWSER_START_TIMEOUT_MS = 240_000;
/** A hold the server never answers about ends in the session's failure path: a
 *  renewal under the lease's minute, a navigation past the machine's own wait. */
const BROWSER_CONTROL_TIMEOUT_MS = 20_000;
const BROWSER_NAVIGATE_TIMEOUT_MS = 75_000;

/** What a view request asks of the computer's browser: its state, to open it
 *  (starting a stopped computer to do so), or, on a machine of the user's own
 *  with no browser, to download Chrome for Testing. */
export type BrowserViewWant = 'look' | 'start' | 'chrome';

const BROWSER_VIEW_BODY: Record<BrowserViewWant, Record<string, true>> = {
  look: {},
  start: { start: true },
  chrome: { get_chrome: true },
};

/** The state of the computer's browser, after doing what `want` asks. Any
 *  failure but a missing route is the caller's to read. */
export async function getBrowserView(workspaceId: string, want: BrowserViewWant = 'look'): Promise<BrowserViewAnswer> {
  try {
    const { data } = await api.post<BrowserViewAnswer>(
      `/api/v1/workspaces/${workspaceId}/sandbox/browser-view`,
      BROWSER_VIEW_BODY[want],
      { timeout: want === 'start' ? BROWSER_START_TIMEOUT_MS : BROWSER_LOOK_TIMEOUT_MS },
    );
    return data;
  } catch (err) {
    if (routeMissing(err)) return { state: 'unavailable' };
    throw err;
  }
}

/**
 * What one viewer does with its hold on the page. Each names its holder, so a
 * renewal only extends a hold that still stands and a release ends only its own.
 */
export type BrowserControlAction = 'take' | 'renew' | 'release';

/** A viewer's view of the page, in page pixels. */
export interface BrowserFit {
  width: number;
  height: number;
}

/** Act on `holder`'s hold on the page; resolves to whether it holds the page now.
 *  While held the agent's commands that act on it are refused. A hand-back lets
 *  go of `releases` first. */
export async function setBrowserControl(
  workspaceId: string,
  holder: string,
  action: BrowserControlAction,
  releases: BrowserRelease[] = [],
): Promise<boolean> {
  try {
    const { data } = await api.post<{ held: boolean }>(
      `/api/v1/workspaces/${workspaceId}/sandbox/browser-control`,
      {
        holder,
        action,
        ...(releases.length > 0 ? { releases } : {}),
      },
      { timeout: BROWSER_CONTROL_TIMEOUT_MS },
    );
    return data.held;
  } catch (err) {
    if (routeMissing(err)) return false;
    throw err;
  }
}

/** What the page did with a view's size. */
export interface BrowserFitAnswer {
  /** The size it took, in page pixels; null where it was left as it is for now
   *  (another viewer holds it, a command runs, no browser is open). */
  size: BrowserFit | null;
  /** It keeps a size of its own until its browser closes (the agent's, or a
   *  window's), so asking again changes nothing. */
  kept: boolean;
}

/** Size the page to `viewer`'s view, so it fills the panel. A server with no
 *  such route never sizes it, which reads as a page keeping its own. */
export async function fitBrowser(workspaceId: string, viewer: string, size: BrowserFit): Promise<BrowserFitAnswer> {
  try {
    const { data } = await api.post<Partial<BrowserFitAnswer>>(
      `/api/v1/workspaces/${workspaceId}/sandbox/browser-fit`,
      { viewer, width: size.width, height: size.height },
      { timeout: BROWSER_CONTROL_TIMEOUT_MS },
    );
    return { size: data.size ?? null, kept: data.kept === true };
  } catch (err) {
    if (routeMissing(err)) return { size: null, kept: true };
    throw err;
  }
}

/** Where the person holding the page takes it: to what they typed, which the
 *  computer reads as an address bar does (an address, or a search for the
 *  words), or back, forward or the same page again. */
export type BrowserNavigation =
  | { action: 'open'; url: string }
  | { action: 'back' | 'forward' | 'reload' };
/** Why a navigation did nothing: the hold lapsed, the browser closed, the address
 *  is not a web page's, or the page could not be reached. */
export type BrowserNavigateRefusal = 'not_held' | 'not_running' | 'bad_address' | 'failed';

/** Take the page where `holder`, who holds it, asked; resolves to why nothing was
 *  done, or null once it was. A request that fails is the caller's to read. */
export async function navigateBrowser(
  workspaceId: string,
  holder: string,
  navigation: BrowserNavigation,
): Promise<BrowserNavigateRefusal | null> {
  const { data } = await api.post<{ done: boolean; reason?: BrowserNavigateRefusal | null }>(
    `/api/v1/workspaces/${workspaceId}/sandbox/browser-navigate`,
    { holder, ...navigation },
    { timeout: BROWSER_NAVIGATE_TIMEOUT_MS },
  );
  return data.done ? null : data.reason ?? 'failed';
}

/** Why a close left the browser open: someone else holds its page, or it did not end. */
export type BrowserCloseRefusal = 'held' | 'failed';

/** Close the computer's browser for `viewer`; resolves to why it is still open,
 *  or null once it is closed. A request that fails is the caller's to read. */
export async function closeBrowser(workspaceId: string, viewer: string): Promise<BrowserCloseRefusal | null> {
  try {
    const { data } = await api.post<{ done: boolean; reason?: BrowserCloseRefusal | null }>(
      `/api/v1/workspaces/${workspaceId}/sandbox/browser-close`,
      { viewer },
      { timeout: BROWSER_NAVIGATE_TIMEOUT_MS },
    );
    return data.done ? null : data.reason ?? 'failed';
  } catch (err) {
    if (routeMissing(err)) return 'failed';
    throw err;
  }
}

/**
 * A signed preview URL points at the app's root; a tool result may name a
 * page inside it, e.g. "/timeline.html?tab=2#top". The suffix is split the
 * way a browser reads it: the pathname setter would percent-encode a `?` or
 * `#` handed to it, and the app would be asked for a file by that name.
 */
export function appendPathSuffix(baseUrl: string, path?: string): string {
  if (!path) return baseUrl;
  try {
    const parsed = new URL(baseUrl);
    const hashAt = path.indexOf('#');
    const hash = hashAt >= 0 ? path.slice(hashAt) : '';
    const beforeHash = hashAt >= 0 ? path.slice(0, hashAt) : path;
    const queryAt = beforeHash.indexOf('?');
    const query = queryAt >= 0 ? beforeHash.slice(queryAt + 1) : '';
    const pathname = queryAt >= 0 ? beforeHash.slice(0, queryAt) : beforeHash;
    if (pathname) {
      parsed.pathname = parsed.pathname.replace(/\/+$/, '') + (pathname.startsWith('/') ? pathname : `/${pathname}`);
    }
    // Appended, not replaced: the signed URL's own query is what admits the request.
    for (const [k, v] of new URLSearchParams(query)) parsed.searchParams.append(k, v);
    if (hash) parsed.hash = hash;
    return parsed.toString();
  } catch {
    return baseUrl;
  }
}

export async function checkPreviewHealth(workspaceId: string, port: number) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/preview-health`, { port });
  return data as { reachable: boolean; checked_at: number };
}

// --- Thread Sharing ---
