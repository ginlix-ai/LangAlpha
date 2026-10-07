/** The sections the app shell routes; `components/Main/Main.tsx` holds a chunk for each. */
export const APP_SECTIONS = [
  'dashboard',
  'chat',
  'market',
  'news',
  'automations',
  'orders',
  'plugins',
  'settings',
  'connectors',
] as const;

export type AppSection = (typeof APP_SECTIONS)[number];

const SECTIONS: ReadonlySet<string> = new Set(APP_SECTIONS);

/**
 * The router path of `href` when it is a page of this app, otherwise null.
 *
 * A link in prose is usually absolute, since the same text is read in a
 * channel outside the app. Opened as a web link it starts a second copy of the
 * app in a new tab, which loses what this tab holds (a skipped setup is kept
 * per tab) and, in the desktop shell, leaves the app for the browser. Same
 * origin alone does not make it ours: with a path-shaped `VITE_PLATFORM_URL`
 * the account portal answers on `/account/*` from this origin as another app.
 */
export function appRoutePath(href: string | undefined, origin = window.location.origin): string | null {
  if (!href) return null;
  let url: URL;
  try {
    url = new URL(href, origin);
  } catch {
    return null;
  }
  if (url.origin !== origin || !SECTIONS.has(url.pathname.split('/')[1])) return null;
  return `${url.pathname}${url.search}${url.hash}`;
}
