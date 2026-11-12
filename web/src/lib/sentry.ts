/**
 * Error tracking in the browser: exceptions only, no tracing or replay.
 *
 * Off unless the build sets `VITE_SENTRY_DSN`. The SDK loads after first paint
 * as its own chunk, so it costs the critical path nothing; an error thrown
 * before it arrives is missed, which the stale-build recovery covers for the
 * failures that matter at boot.
 *
 * Replay stays off: it records the screen, and the screen is the user's
 * research. Breadcrumbs keep no console text and no request bodies.
 */

import { pageIsTranslated } from './translatedDom';

type SentryModule = typeof import('@sentry/react');

// Paths that are their own credential: a share page, a public share, a file
// grant, a share-link code, a helper's pairing. The segment after each goes
// as a placeholder, in the page's URL and in every breadcrumb's.
const CAPABILITY_PATH = /(\/(?:api\/v1\/public\/shared|wsfiles\/g|artifacts\/g|share-links|pairings)\/)[^/?#\s"'()<>]+/g;
// A share page is `/s/` or `/a/` at the root of a path: at the start of the
// text or after a separator, as in an error's message, never inside another
// path (`/assets/s/x`).
const SHARE_PAGE = /(^|[\s"'(=])((?:https?:\/\/[^/\s"']+)?\/[sa]\/)[^/?#\s"'()<>]+/g;

export function redactCapabilities(text: string): string {
  return text.replace(SHARE_PAGE, '$1$2[redacted]').replace(CAPABILITY_PATH, '$1[redacted]');
}

type Crumb = { category?: string; message?: string; data?: Record<string, unknown> };

function redactCrumb<T extends Crumb>(crumb: T): T | null {
  if (crumb.category === 'console') return null;
  if (crumb.message) crumb.message = redactCapabilities(crumb.message);
  if (crumb.data) {
    for (const [key, value] of Object.entries(crumb.data)) {
      if (typeof value === 'string') crumb.data[key] = redactCapabilities(value);
    }
  }
  return crumb;
}

type EventShape = {
  message?: string;
  transaction?: string;
  request?: { url?: string };
  exception?: { values?: Array<{ value?: string }> };
  breadcrumbs?: Crumb[];
};

/** An event as it leaves: no capability in its address, its route, its text
 *  (an error's message can quote the address that failed) or its crumbs. */
export function redactEvent<T extends EventShape>(event: T): T {
  if (event.request?.url) event.request.url = redactCapabilities(event.request.url);
  if (event.transaction) event.transaction = redactCapabilities(event.transaction);
  if (typeof event.message === 'string') event.message = redactCapabilities(event.message);
  for (const exc of event.exception?.values ?? []) {
    if (exc.value) exc.value = redactCapabilities(exc.value);
  }
  event.breadcrumbs = event.breadcrumbs?.map((crumb) => redactCrumb(crumb)).filter((crumb) => crumb !== null);
  return event;
}

/**
 * The release, named after the entry chunk the page loaded (`web@index-Bsv8HSB4`).
 * Its hash moves exactly when the shipped code does. A commit id baked into the
 * bundle would move the entry's hash on every deploy, and the stale-build check
 * would then offer every open tab a reload for a backend-only release.
 */
export function releaseFromEntry(entryUrl: string): string | undefined {
  const name = /\/assets\/([^/?#]+)\.js(?:[?#]|$)/.exec(entryUrl)?.[1];
  return name ? `web@${name}` : undefined;
}

let sentry: SentryModule | null = null;
// The account, kept for an SDK that has not loaded yet.
let userId: string | null = null;
// The translated-page guard reports once per page load for each kind, so a
// translation earlier on cannot hide a later absorb on an untranslated page.
// What it absorbs before the SDK loads waits in pendingAbsorbed.
const absorbedReported = { translated: false, untranslated: false };
let pendingAbsorbed: Array<{ error: DOMException; translated: boolean }> = [];

/** `isStaleBuildError` is the stale-build recovery's, handed in so this
 *  module stays free of that one's UI. */
export function initSentry(entryUrl: string, isStaleBuildError: (error: unknown) => boolean): void {
  const dsn = import.meta.env.VITE_SENTRY_DSN as string | undefined;
  // Not from the dev server: it reads the same `.env` a release build does,
  // so its errors would land in the release's environment.
  if (!dsn || import.meta.env.DEV) return;
  const start = (): void => {
    void import('@sentry/react').then((mod) => {
      mod.init({
        dsn,
        environment: (import.meta.env.VITE_SENTRY_ENVIRONMENT as string | undefined) || import.meta.env.MODE,
        release: releaseFromEntry(entryUrl),
        // The user is set by id alone (setSentryUser); nothing else about the
        // request leaves: a URL's query can name a ticker or a search.
        dataCollection: { userInfo: false, cookies: false, httpHeaders: false, httpBodies: [], urlQueryParams: false },
        tracesSampleRate: 0,
        integrations: (defaults) => defaults.filter((i) => i.name !== 'BrowserSession'),
        beforeBreadcrumb: (crumb) => redactCrumb(crumb),
        // A translated page rewrites the DOM under React, so an error from one
        // is told apart from a bug in our own markup.
        beforeSend: (event, hint) => {
          event.tags = { page_translated: pageIsTranslated() ? 'yes' : 'no', ...event.tags };
          // A chunk a deploy removed, met by a tab still on the build before
          // it: every deploy brings some, and the tab was offered a reload. One
          // warning issue, whose rate still shows a build missing its own
          // chunks, rather than a new error issue per chunk per deploy.
          if (isStaleBuildError(hint.originalException)) {
            event.level = 'warning';
            event.fingerprint = ['stale-build-chunk'];
          }
          return redactEvent(event);
        },
      });
      if (userId) mod.setUser({ id: userId });
      sentry = mod;
      for (const { error, translated } of pendingAbsorbed) sendAbsorbed(mod, error, translated);
      pendingAbsorbed = [];
    }).catch(() => {
      // An ad blocker refusing the chunk or the ingest host is the user's call.
    });
  };
  if ('requestIdleCallback' in window) window.requestIdleCallback(start, { timeout: 5000 });
  else setTimeout(start, 2000);
}

/** Tags the user on later events once the account is known. */
export function setSentryUser(id: string | null): void {
  userId = id;
  sentry?.setUser(id ? { id } : null);
}

/** For errors React caught in a boundary, which never reach the global handler. */
export function captureException(error: unknown): void {
  sentry?.captureException(error);
}

/**
 * Reports the first DOM call the translated-page guard absorbed on this page
 * load, on a translated page and on an untranslated one. On a translated page
 * that is the guard doing its job, kept as one warning issue so its rate stays
 * visible. On any other page nothing should have moved React's nodes, so it is
 * an error: our own DOM code or an extension.
 */
export function reportAbsorbedDomError(error: DOMException): void {
  const translated = pageIsTranslated();
  const kind = translated ? 'translated' : 'untranslated';
  if (absorbedReported[kind]) return;
  absorbedReported[kind] = true;
  if (sentry) sendAbsorbed(sentry, error, translated);
  else pendingAbsorbed.push({ error, translated });
}

function sendAbsorbed(mod: SentryModule, error: DOMException, translated: boolean): void {
  mod.withScope((scope) => {
    scope.setTag('dom_guard', 'absorbed');
    scope.setTag('page_translated', translated ? 'yes' : 'no');
    scope.setLevel(translated ? 'warning' : 'error');
    if (translated) scope.setFingerprint(['dom-guard-translated']);
    mod.captureException(error);
  });
}
