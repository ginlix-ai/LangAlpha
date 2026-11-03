/**
 * Served workspace HTML runs its scripts in an opaque origin: without
 * `allow-same-origin` a report cannot read the app's cookies or storage. It
 * keeps everything else a page does (forms, dialogs, downloads, links that
 * open a new tab), since a page missing those just looks broken. The server's
 * header carries the same tokens (`utils/html_policy`), and the two intersect.
 */
export const SERVED_HTML_SANDBOX = 'allow-scripts allow-forms allow-modals allow-downloads allow-popups allow-popups-to-escape-sandbox';

/**
 * The page the Publish review loads out of sight: its code runs, and nothing it
 * does on load reaches the owner as a dialog, a download or a new window.
 */
export const SCOUT_SANDBOX = 'allow-scripts';

/** What a framed page may ask of the browser beyond loading: a copy button, full screen. */
export const SERVED_HTML_PERMISSIONS = 'clipboard-write *; fullscreen *';

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
