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
import type { ErrorLinkSpec } from '@/utils/rateLimitError';

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

/**
 * Remote control: the shell's background helper, which lets the agent use
 * this Mac as one of the user's computers, reached through a tunnel. Where it
 * cannot run, the shell reads nothing of the helper's and says why.
 */
export type RemoteControlStatus =
  | {
      supported: false;
      reason: 'platform' | 'os_version' | 'not_packaged';
      service: 'off';
      helper: null;
      paired: false;
      fingerprint: null;
      account: string;
    }
  | {
      supported: true;
      /** The login item: on, off (never registered, or paused), or waiting for the user in System Settings. */
      service: 'on' | 'off' | 'needs_approval';
      /** The helper's own state; null when it has none or the service is off. */
      helper: 'unpaired' | 'connecting' | 'connected' | 'refused' | null;
      /** Whether the helper holds a paired tunnel. */
      paired: boolean;
      /** The helper's SSH host key fingerprint, as a computer's `ssh.fingerprint` reports it; null before pairing. */
      fingerprint: string | null;
      /** The macOS account the agent acts as. */
      account: string;
    };

export type RemoteControlError =
  | 'unsupported'
  | 'bad_url'
  /** The user said no in the shell's own confirmation. */
  | 'cancelled'
  | 'needs_approval'
  | 'not_started'
  | 'failed'
  /** A Mac paired before was left running as it was: it could not be taken down, or a
   *  helper replacing it in place kept it when the new pairing failed. */
  | 'not_replaced'
  | 'timeout'
  /** This side's, not the shell's: no method, a channel that threw, or no answer. */
  | 'unavailable';

export type RemoteControlResult =
  | { ok: true; status: RemoteControlStatus }
  | {
      ok: false;
      error: RemoteControlError;
      /** For `failed`, and `not_replaced` where the pairing failed: the deployment's own sentence. */
      message?: string;
      /** Null only for `unavailable`. */
      status: RemoteControlStatus | null;
    };

/** What a desktop notification is about; the shell filters by the user's per-kind switches. */
export type DesktopNotificationKind = 'turn' | 'needs_input' | 'wakeup' | 'automation' | 'started';

export interface DesktopNotification {
  /** The thread id. One notification per thread, so a run's start is replaced by its result. */
  id: string;
  kind: DesktopNotificationKind;
  title: string;
  /** macOS shows it on its own line; elsewhere the shell folds it into the body. */
  subtitle?: string;
  body: string;
  /** The app path a click opens in the main window. */
  path: string;
  silent?: boolean;
}

export interface DesktopNotifyResult {
  shown: boolean;
  reason?: 'disabled' | 'watching' | 'unsupported';
}

export interface AttentionThread {
  id: string;
  title: string;
  path: string;
  state: 'running' | 'unread' | 'needs_input';
}

/** A symbol the quote API can price; `index` symbols are asked for as indices (GSPC, not ^GSPC). */
export interface QuoteRef {
  symbol: string;
  kind: 'index' | 'stock';
}

/** One thing beside the menu bar icon: the portfolio's day, or one symbol. */
export type MenubarItem = { kind: 'portfolio' } | QuoteRef;

export type PanelSectionId = 'market' | 'portfolio' | 'watchlist' | 'agent';

/** A formatted line for the menu bar, e.g. `NVDA ▼5.60%`. The shell lays them out; the page formats them. */
export interface TrayItemText {
  text: string;
}

export interface DesktopSettings {
  notifications: {
    turn: boolean;
    needs_input: boolean;
    wakeup: boolean;
    automation: boolean;
    started: boolean;
  };
  menubar: {
    enabled: boolean;
    /** What sits beside the icon, in order. Empty for the icon alone. At most 8. */
    items: MenubarItem[];
    /** With more than one item: one slot cycling, or up to three side by side. */
    style: 'rotate' | 'side';
    /** 2 to 30. */
    rotateSeconds: number;
    show: 'change' | 'price' | 'both';
    /** Show the pre-market or after-hours move, tagged PM/AH, when there is one. */
    extendedHours: boolean;
  };
  panel: {
    /** All four, in display order. */
    sections: Array<{ id: PanelSectionId; visible: boolean }>;
    /** The market tiles, at most 4. */
    tiles: QuoteRef[];
    /** Null for the user's default watchlist. */
    watchlistId: string | null;
    /** 1 to 10. */
    watchlistRows: number;
  };
  openAtLogin: boolean;
  /** An Electron accelerator, or '' for none. */
  shortcut: string;
  /** Windows only: closing the last window leaves the app in the tray. */
  keepRunningInTray: boolean;
}

/** One level deep: each group may be given in part. */
export type DesktopSettingsPatch = {
  [K in keyof DesktopSettings]?: DesktopSettings[K] extends object
    ? Partial<DesktopSettings[K]>
    : DesktopSettings[K];
};

/** The settings plus what this platform can do, so a page offers only switches that work here. */
export interface DesktopSettingsView extends DesktopSettings {
  support: {
    menubarPanel: boolean;
    trayTitle: boolean;
    openAtLogin: boolean;
    shortcut: boolean;
    keepRunningInTray: boolean;
    notificationActions: boolean;
  };
  /** Set when the shortcut could not be registered, usually because another app holds it. */
  shortcutError?: string;
}

/** Commands the shell's menus send to the main window. */
export type DesktopMenuCommand =
  | 'new-thread'
  | 'new-workspace'
  | 'toggle-sidebar'
  | 'export-pdf'
  | 'open-settings'
  | 'go-chief-of-staff'
  | 'go-market'
  | 'go-automations'
  | 'go-orders'
  | 'open';

export interface DesktopBridge {
  readonly version: string;
  /**
   * Which of the shell's windows this page is in. `menubar` is the hidden
   * window behind the menu bar icon, the only page that posts notifications.
   * Absent on a shell older than the menu bar.
   */
  readonly windowRole?: 'main' | 'menubar' | 'other';
  notify?(notification: DesktopNotification): Promise<DesktopNotifyResult>;
  /** Remove these threads' notifications, once read anywhere. */
  clearNotifications?(ids: string[]): void;
  /** Dock badge and dock menu / jump list. `count` is unread plus needs-input threads. */
  setAttention?(attention: { count: number; threads: AttentionThread[] }): void;
  /** A notification's "Mark as read" button. Opening is handled by the shell. */
  onNotificationAction?(
    callback: (action: { id: string; action: 'mark-read'; path: string }) => void,
  ): () => void;
  /** Declare which menu commands this page handles; the shell enables only those. */
  setMenuCommands?(commands: DesktopMenuCommand[]): void;
  onMenuCommand?(callback: (command: { command: DesktopMenuCommand; path?: string }) => void): () => void;
  getDesktopSettings?(): Promise<DesktopSettingsView>;
  setDesktopSettings?(patch: DesktopSettingsPatch): Promise<DesktopSettingsView>;
  onDesktopSettings?(callback: (settings: DesktopSettingsView) => void): () => void;
  /**
   * Menu bar page only: the lines beside the icon. The shell rotates or joins
   * them (macOS), puts them in the tooltip (Windows) or atop the tray menu
   * (Linux), by the user's own layout settings. Empty for the icon alone.
   */
  setTrayItems?(items: TrayItemText[], options: { refreshMs: number }): void;
  /**
   * Menu bar page only: the shell asks for fresh lines every `refreshMs`. A
   * hidden page's own timers are throttled to once a minute, IPC is not.
   */
  onTrayRefresh?(callback: () => void): () => void;
  /** Menu bar page only: show the main window at `path` and close the panel. */
  openInMain?(path: string): void;
  /** Menu bar page only: close the panel. */
  hideMenubar?(): void;
  /** Node's process.platform: 'darwin' | 'win32' | 'linux'. */
  readonly platform: string;
  /**
   * Whether the shell hid this window's titlebar. Absent before shell 0.1.1.
   *
   * Per window, not per platform: the same install opens a frameless main
   * window and a framed account window, and a first launch is framed on macOS
   * too. Only the shell knows.
   *
   * Not to be trusted alongside `version` 0.0.0: a shell through 0.3.0 whose
   * page landed in a renderer without its window's switches reports `native`
   * whatever the frame (see the stamp in `index.html`).
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
  /**
   * What the shell does that has no method to feature-detect, such as
   * `'integration-login'`. Ask with `includes`; absent on a shell older than
   * the list, which means none of them.
   */
  readonly capabilities?: readonly string[];
  /**
   * Tells the shell which theme the page settled on, and the user's choice
   * behind it, which the menu bar panel's material follows. Added in shell
   * 0.1.0; a shell older than the menu bar ignores the choice.
   */
  setTheme?(theme: 'light' | 'dark', preference?: 'light' | 'dark' | 'auto'): void;
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
  /**
   * Where remote control stands on this Mac. Every platform answers; only a
   * packaged macOS 13+ app is `supported`. Like the two below, null to a page
   * the shell does not count as the app's.
   */
  getRemoteControl?(): Promise<RemoteControlStatus | null>;
  /**
   * Pair this Mac with the pairing at `scriptUrl` and turn the helper on.
   * Settles once the helper has paired, failed, or given up waiting.
   */
  enableRemoteControl?(scriptUrl: string): Promise<RemoteControlResult | null>;
  /** Turn the helper off and delete its folder, keys and tunnel with it. Idempotent. */
  disableRemoteControl?(): Promise<RemoteControlStatus | null>;
}

declare global {
  interface Window {
    langalphaDesktop?: DesktopBridge;
  }
}


export const desktop: DesktopBridge | undefined =
  typeof window === 'undefined' ? undefined : window.langalphaDesktop;

// Every helper below is safe to call anywhere. A browser has no bridge, an
// older shell lacks the method, and a channel can throw once its window is
// going; each of those is the same quiet answer, so no call site needs a guard.

type Methods = {
  [K in keyof DesktopBridge as NonNullable<DesktopBridge[K]> extends (...args: never[]) => unknown ? K : never]-?: NonNullable<
    DesktopBridge[K]
  >;
};
type Method = keyof Methods;
type Args<K extends Method> = Methods[K] extends (...args: infer A) => unknown ? A : never;
type Answer<K extends Method> = Methods[K] extends (...args: never[]) => infer R ? R : never;

function has(name: Method): boolean {
  return typeof desktop?.[name] === 'function';
}

function call<K extends Method>(name: K, args: Args<K>): Answer<K> | undefined {
  const fn = desktop?.[name] as ((this: DesktopBridge, ...a: Args<K>) => Answer<K>) | undefined;
  return fn?.call(desktop!, ...args);
}

/** Whether the shell took the call. */
function fire<K extends Method>(name: K, ...args: Args<K>): boolean {
  if (!has(name)) return false;
  try {
    call(name, args);
    return true;
  } catch {
    return false;
  }
}

function listen<K extends Method>(name: K, ...args: Args<K>): () => void {
  try {
    return (call(name, args) as (() => void) | undefined) ?? (() => {});
  } catch {
    return () => {};
  }
}

async function ask<K extends Method>(name: K, ...args: Args<K>): Promise<Awaited<Answer<K>> | null> {
  try {
    return (await call(name, args)) ?? null;
  } catch {
    return null;
  }
}

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
 * Open a page of the account console: in this window in the desktop app,
 * which keeps the console beside the app, and in a new tab in a browser, so
 * the page left behind (a chat with a message half typed) survives. A page
 * with nothing to lose (`inPlace`) gives way in a browser too, so the way
 * back from the console is one tab.
 */
export function openConsolePage(url: string, { inPlace = false }: { inPlace?: boolean } = {}) {
  if (inPlace || isDesktopShell()) window.location.assign(url);
  else window.open(url, '_blank', 'noopener,noreferrer');
}

/** Follows an error's link from code rather than an anchor: a console page
 *  as `openConsolePage` opens one, a destination outside this SPA in a new
 *  tab, and anything else as a route. */
export function followErrorLink(link: ErrorLinkSpec, navigate: (to: string) => void) {
  if (link.console) openConsolePage(link.url);
  else if (link.external) window.open(link.url, '_blank', 'noopener,noreferrer');
  else navigate(link.url);
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
  return has('beginMcpOAuth');
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
  const flow = await ask('beginMcpOAuth', returnUrl);
  // Both halves or neither. A shell that answered without one of them cannot
  // complete the flow, and treating that as 'no listener' puts the caller on
  // the browser path, which still works.
  return flow?.redirectUri && flow.flowId ? flow : undefined;
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
 * listener will refuse. A shell too old to know the channel armed nothing
 * anyway, and reports the same false.
 */
export async function bindMcpOAuth(flowId: string, state: string): Promise<boolean> {
  return (await ask('bindMcpOAuth', flowId, state)) === true;
}

/**
 * Release a listener armed for a flow that never launched.
 *
 * `beginMcpOAuth` has to be asked before the start request, because its answer
 * travels with it — so a start that fails leaves the shell holding a flow for a
 * code nobody is going to send. Saying so frees it now rather than at the
 * timeout, which arrives minutes later and reloads the window. Never reports: a
 * shell that cannot hear this still recovers on its own, just later.
 */
export async function cancelMcpOAuth(flowId: string): Promise<void> {
  await ask('cancelMcpOAuth', flowId);
}

/**
 * Whether this shell can turn remote control on and off again. All three
 * methods: offering a switch the shell could not turn back off would leave the
 * user with no way out but System Settings.
 */
export function canRemoteControl(): boolean {
  return has('getRemoteControl') && has('enableRemoteControl') && has('disableRemoteControl');
}

export function getRemoteControl(): Promise<RemoteControlStatus | null> {
  return ask('getRemoteControl');
}

/** A shell that cannot be asked, or gave no answer, fails as `unavailable`. */
export async function enableRemoteControl(scriptUrl: string): Promise<RemoteControlResult> {
  return (await ask('enableRemoteControl', scriptUrl)) ?? { ok: false, error: 'unavailable', status: null };
}

/** The status it leaves, or null when the shell could not be asked. */
export function disableRemoteControl(): Promise<RemoteControlStatus | null> {
  return ask('disableRemoteControl');
}

/**
 * Whether this page is the hidden window behind the menu bar icon, the one
 * page that posts notifications and sets the dock badge. False on a shell
 * older than the menu bar, which reports no role at all.
 */
export function isMenubarWindow(): boolean {
  return desktop?.windowRole === 'menubar';
}

/**
 * Whether this page is the app's main window. A shell older than window roles
 * reports none, and is taken as main when it can send menu commands at all,
 * since that shell has only the one window that could.
 */
export function isMainWindow(): boolean {
  const role = desktop?.windowRole;
  return role === 'main' || (role === undefined && has('onMenuCommand'));
}

export function canNotify(): boolean {
  return has('notify');
}

/** Posting nothing (no method, a channel that threw) answers `unsupported` rather than rejecting. */
export async function desktopNotify(notification: DesktopNotification): Promise<DesktopNotifyResult> {
  return (await ask('notify', notification)) ?? { shown: false, reason: 'unsupported' };
}

export function clearDesktopNotifications(ids: string[]): void {
  if (ids.length) fire('clearNotifications', ids);
}

export function desktopSetAttention(attention: { count: number; threads: AttentionThread[] }): void {
  fire('setAttention', attention);
}

export function onNotificationAction(
  callback: (action: { id: string; action: 'mark-read'; path: string }) => void,
): () => void {
  return listen('onNotificationAction', callback);
}

export function canReceiveMenuCommands(): boolean {
  return has('onMenuCommand');
}

export function setMenuCommands(commands: DesktopMenuCommand[]): void {
  fire('setMenuCommands', commands);
}

export function onMenuCommand(callback: (command: { command: DesktopMenuCommand; path?: string }) => void): () => void {
  return listen('onMenuCommand', callback);
}

/**
 * Whether Settings shows its Desktop app section. Reading and writing both: a
 * section that could show the values but not change them would only mislead.
 */
export function canDesktopSettings(): boolean {
  return has('getDesktopSettings') && has('setDesktopSettings');
}

export function getDesktopSettings(): Promise<DesktopSettingsView | null> {
  return ask('getDesktopSettings');
}

/** The settings the shell holds after the change, or null when it could not be asked. */
export function setDesktopSettings(patch: DesktopSettingsPatch): Promise<DesktopSettingsView | null> {
  return ask('setDesktopSettings', patch);
}

/** Settings changed anywhere: another window, the tray menu. */
export function onDesktopSettings(callback: (settings: DesktopSettingsView) => void): () => void {
  return listen('onDesktopSettings', callback);
}

export function canSetTrayItems(): boolean {
  return has('setTrayItems');
}

/** Menu bar page only: the lines beside the icon, [] for the icon alone. */
export function setTrayItems(items: TrayItemText[], options: { refreshMs: number }): void {
  fire('setTrayItems', items, options);
}

export function onTrayRefresh(callback: () => void): () => void {
  return listen('onTrayRefresh', callback);
}

/**
 * Show the main window at `path`, closing the menu bar panel. A shell without
 * the call still sends any app address but its own page to the main window,
 * so a plain navigation reaches the same place there, and in a browser it
 * simply goes.
 */
export function openInMain(path: string): void {
  if (!fire('openInMain', path)) window.location.assign(path);
}

/** Menu bar page only: close the panel. */
export function hideMenubar(): void {
  fire('hideMenubar');
}
