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
