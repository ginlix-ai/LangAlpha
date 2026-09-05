/**
 * Bridge to the Electron desktop shell, injected by its preload script.
 *
 * Undefined in every browser build, so treat desktop as an enhancement and keep
 * the browser path alive at each call site. The shell updates on its own slow
 * cadence while this app deploys continuously, so a new web build must never
 * require a new shell: feature-detect each method, never the version.
 *
 * Signing in needs nothing from this side: the shell intercepts that authorize
 * navigation itself and hands the code back through this app's own /callback
 * route. `beginMcpOAuth` is a different flow with a different shape, and the
 * comment on it says why it cannot work the same way.
 */
/** What the shell will accept for a PDF export; everything is optional. */
export interface SavePdfOptions {
  /** Suggested filename. The shell sanitizes it and appends `.pdf`. */
  fileName?: string;
  landscape?: boolean;
  /** 0.1 to 2. Clamped by the shell rather than rejected. */
  scale?: number;
  pageSize?:
    | 'A0' | 'A1' | 'A2' | 'A3' | 'A4' | 'A5' | 'A6'
    | 'Legal' | 'Letter' | 'Tabloid' | 'Ledger';
  /** Comma-separated 1-based pages or ranges, e.g. `1-3, 7`. */
  pageRanges?: string;
  /** Defaults to true; false drops backgrounds the way a browser does. */
  printBackground?: boolean;
  /** Tagged reading order, defaults to true. */
  tagged?: boolean;
  /** PDF outline built from the heading tree, defaults to true. */
  outline?: boolean;
}

/**
 * Deliberately three outcomes, not a boolean. A caller falls back to browser
 * print when the method is absent, but must NOT fall back on `canceled`:
 * reopening a print dialog the user just dismissed is the one response that
 * reads as the app ignoring them.
 */
export type SavePdfResult =
  | { saved: true }
  | { canceled: true }
  | { error: string };

/**
 * An armed connector flow. `flowId` names it in every later call, so a page that
 * starts a second connect while the first is still going cannot bind or cancel
 * the wrong one -- which the Brokerages tab makes ordinary, since it leaves the
 * other rows clickable on purpose.
 */
export interface McpOAuthFlow {
  redirectUri: string;
  flowId: string;
}

export interface DesktopBridge {
  readonly version: string;
  /** Node's process.platform: 'darwin' | 'win32' | 'linux'. */
  readonly platform: string;
  /**
   * Whether the shell hid this window's titlebar. Absent before shell 0.1.1.
   *
   * Per window, not per platform: the same install opens a frameless main
   * window and a framed account window, and a first launch is framed on macOS
   * too. Only the shell knows.
   */
  readonly windowChrome?: 'hidden' | 'native';
  /**
   * The custom scheme this edition registers, without the `://`. Absent before
   * shell 0.2.1, and empty rather than absent on a shell that passes the switch
   * without a value.
   *
   * Needed because the two editions install side by side and answer on
   * different schemes, so anything addressing the app through the OS has to ask
   * which build this is instead of assuming.
   */
  readonly scheme?: string;
  /** Tells the shell which theme the page settled on. Added in shell 0.1.0. */
  setTheme?(theme: 'light' | 'dark'): void;
  openExternal?(url: string): Promise<void>;
  /**
   * Render this page to a PDF and let the user choose where it lands, with no
   * print dialog in between. Added in shell 0.1.2; feature-detect it.
   */
  savePdf?(options?: SavePdfOptions): Promise<SavePdfResult>;
  /**
   * Ask the shell to catch a connector's OAuth code on a loopback listener, and
   * answer with the `redirect_uri` to start the flow against. `returnUrl` is the
   * backend callback the shell drives this window to once a code lands.
   *
   * This one cannot be hidden in the shell the way sign-in is. A connector's
   * `redirect_uri` is bound into the flow when the backend mints it and checked
   * again at the token exchange, so it has to be settled before the request goes
   * out, not swapped in the navigation afterwards.
   *
   * Null is the ordinary answer when the shell has no listener free, and means
   * carry on exactly as a browser would. Added in shell 0.1.3.
   */
  beginMcpOAuth?(returnUrl: string): Promise<McpOAuthFlow | null>;
  /**
   * Hand the shell the `state` the backend minted for this flow, so it can tell
   * this flow's callback from anything else that reaches the loopback port.
   * Until it is called the armed flow accepts nothing. Answers whether the flow
   * was still there to bind. Added in shell 0.1.3, with `beginMcpOAuth`.
   */
  bindMcpOAuth?(flowId: string, state: string): Promise<boolean>;
  /**
   * Say that the flow just armed is not happening after all. Answers whether
   * there was one to stand down. Added in shell 0.1.3, with `beginMcpOAuth`.
   */
  cancelMcpOAuth?(flowId: string): Promise<boolean>;
}

declare global {
  interface Window {
    langalphaDesktop?: DesktopBridge;
  }
}

export const desktop: DesktopBridge | undefined =
  typeof window === 'undefined' ? undefined : window.langalphaDesktop;

/**
 * Whether this page is running inside the desktop shell at all.
 *
 * The bridge, and nothing about what it can do. Every version of the shell has
 * one, which is what makes this the question to ask when the consequence
 * follows from being in the app rather than from any particular capability:
 * an external navigation leaves for the system browser wherever this is true,
 * and the shell has been that way since long before any of these methods.
 */
export function isDesktopShell(): boolean {
  return desktop !== undefined;
}

/**
 * Whether this shell can catch a connector's OAuth code on a loopback listener.
 *
 * The method, never the bridge: a shell old enough to predate the channel is
 * present and still cannot complete one of these. Pairs with `isDesktopShell`,
 * and the two are not interchangeable -- reading this one as "are we in the
 * app" makes an outdated shell answer the way a browser does, which is the one
 * place where the browser's answer is the dangerous one.
 */
export function canBeginMcpOAuth(): boolean {
  return typeof desktop?.beginMcpOAuth === 'function';
}

/**
 * Ask the shell to catch a connector's OAuth code, and answer with the
 * `redirect_uri` to mint the flow against. `returnUrl` is the backend callback
 * the shell drives this window to once a code lands.
 *
 * Every way of not getting one is the same answer, `undefined`: a browser, a
 * shell too old to know the channel, a shell with no listener free, a channel
 * that threw. Asking costs nothing anywhere.
 *
 * What the absence MEANS is the caller's to decide and is not the same in both
 * places. In a browser it means carry on with the hosted callback. In the shell
 * it means this connect cannot complete at all, because an authorize URL opens
 * in the system browser and the callback comes home to a cookie jar that never
 * saw the flow. `startMcpOauth` is where that is judged.
 */
export async function beginMcpOAuth(returnUrl: string): Promise<McpOAuthFlow | undefined> {
  const begin = desktop?.beginMcpOAuth;
  if (!begin) return undefined;
  try {
    const flow = await begin.call(desktop, returnUrl);
    // Both halves or neither. A shell that answered without one of them cannot
    // complete the flow, and treating that as 'no listener' puts the caller on
    // the browser path, which still works.
    return flow?.redirectUri && flow.flowId ? flow : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Tell the shell which `state` the flow it armed will come back with.
 *
 * The two calls cannot be one: the redirect_uri has to reach the backend before
 * it will mint a flow, and the state does not exist until it has. The shell
 * refuses every callback for an unbound flow, so this is what turns an armed
 * listener into one that can actually complete, and the window between them is
 * closed rather than merely short.
 *
 * Answers whether the flow is now bound. False means the shell is no longer
 * holding this one -- a second connect in the same window supersedes the first,
 * and the loser must not go on to open a consent screen whose reply the
 * listener will refuse. Never throws: a shell too old to know the channel armed
 * nothing anyway, and reports the same false.
 */
export async function bindMcpOAuth(flowId: string, state: string): Promise<boolean> {
  try {
    return (await desktop?.bindMcpOAuth?.(flowId, state)) === true;
  } catch {
    return false;
  }
}

/**
 * Release a listener armed for a flow that never launched.
 *
 * `beginMcpOAuth` has to be asked before the start request, because its answer
 * travels with it — so a start that fails leaves the shell holding a flow for a
 * code nobody is going to send. Saying so frees it now rather than at the
 * timeout, which arrives minutes later and reloads the window.
 *
 * Never throws and never reports: a shell that cannot hear this still recovers
 * on its own, just later, and there is nothing the page would do differently.
 */
export async function cancelMcpOAuth(flowId: string): Promise<void> {
  try {
    await desktop?.cancelMcpOAuth?.(flowId);
  } catch {
    // A shell too old to know the channel, or one already gone.
  }
}
