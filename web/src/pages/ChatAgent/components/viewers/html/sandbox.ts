/**
 * Served workspace HTML runs its scripts in an opaque origin: without
 * `allow-same-origin` a report cannot read the app's cookies or storage, and
 * its links may still open a new tab.
 */
export const SERVED_HTML_SANDBOX = 'allow-scripts allow-popups allow-popups-to-escape-sandbox';

/**
 * A running app is served from its own origin, not the app's, so it keeps
 * `allow-same-origin` for its own storage, plus the forms and dialogs an app
 * needs.
 */
export const APP_PREVIEW_SANDBOX = 'allow-scripts allow-same-origin allow-forms allow-popups allow-modals';

/**
 * A published page on a host of its own keeps `allow-same-origin`, which
 * there is the page's origin, not the app's: its storage lasts between visits
 * and cannot reach the app's. Forms, since a page may carry one. Without a
 * host of its own a page is framed as served HTML is (`SERVED_HTML_SANDBOX`).
 */
export const ISOLATED_PAGE_SANDBOX = 'allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox allow-forms';
