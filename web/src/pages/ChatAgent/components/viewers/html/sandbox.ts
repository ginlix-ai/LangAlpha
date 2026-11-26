/**
 * Served workspace HTML runs its scripts in an opaque origin: without
 * `allow-same-origin` a report cannot read the app's cookies or storage. It
 * keeps everything else a page does (forms, dialogs, downloads, links that
 * open a new tab), since a page missing those just looks broken. The server's
 * header carries the same tokens (`utils/html_policy`), and the two intersect.
 */
export const SERVED_HTML_SANDBOX = 'allow-scripts allow-forms allow-modals allow-downloads allow-popups allow-popups-to-escape-sandbox';

/**
 * A ShowWidget frame (inline, fullscreen, print, export shell, headless
 * render) runs agent-written HTML as a report does, so it gets a report's
 * tokens: its own opaque origin keeps it from the app's cookies and storage,
 * and the rest is what a page needs to not look broken. Never
 * `allow-same-origin` (a srcdoc frame would then BE the app's origin), never
 * top navigation.
 */
export const WIDGET_SANDBOX = SERVED_HTML_SANDBOX;

/**
 * The page the Publish review loads out of sight: its code runs, and nothing it
 * does on load reaches the owner as a dialog, a download or a new window.
 */
export const SCOUT_SANDBOX = 'allow-scripts';

/** What a framed page may ask of the browser beyond loading: a copy button, full screen. */
export const SERVED_HTML_PERMISSIONS = 'clipboard-write *; fullscreen *';

/** A widget asks what a served page may. */
export const WIDGET_PERMISSIONS = SERVED_HTML_PERMISSIONS;

/**
 * A widget frame's attributes, for JSX: `<iframe {...WIDGET_FRAME} />`. The
 * marker has focus taken without the user's gesture given back
 * (`frameActivation.ts`).
 */
export const WIDGET_FRAME = { sandbox: WIDGET_SANDBOX, allow: WIDGET_PERMISSIONS, 'data-widget-frame': '' } as const;

/** A widget frame made outside React (the print copy, the headless render). */
export function createWidgetFrame(): HTMLIFrameElement {
  const frame = document.createElement('iframe');
  frame.setAttribute('sandbox', WIDGET_FRAME.sandbox);
  frame.setAttribute('allow', WIDGET_FRAME.allow);
  frame.setAttribute('data-widget-frame', '');
  return frame;
}

/**
 * A running app is served from its own origin, not the app's, so it keeps
 * `allow-same-origin` for its own storage, plus the forms and dialogs an app
 * needs.
 */
export const APP_PREVIEW_SANDBOX = 'allow-scripts allow-same-origin allow-forms allow-popups allow-modals';

/**
 * A published page on a host of its own keeps `allow-same-origin`, which
 * there is the page's origin, not the app's: its storage lasts between visits
 * and cannot reach the app's. Without a host of its own a page is framed as
 * served HTML is (`SERVED_HTML_SANDBOX`).
 */
export const ISOLATED_PAGE_SANDBOX = 'allow-scripts allow-same-origin allow-forms allow-modals allow-downloads allow-popups allow-popups-to-escape-sandbox';
