import { isDesktopShell } from './desktop';

/**
 * Where the desktop app opens an `/a/` link in a reply: as a tab in the file
 * panel, or in the browser. Per device, like the font size, since it is a
 * question about this install.
 *
 * The browser keeps its new tab, which is what a browser user expects of a
 * link. The desktop shell has no tabs and hands it to the system browser,
 * where the owner may not be signed in and an app link answers 404, so the
 * panel is the default there.
 */
export type ShareLinkOpen = 'panel' | 'browser';

const KEY = 'shareLinkOpen';

export function getShareLinkOpen(): ShareLinkOpen {
  return localStorage.getItem(KEY) === 'browser' ? 'browser' : 'panel';
}

export function setShareLinkOpen(value: ShareLinkOpen): void {
  localStorage.setItem(KEY, value);
}

/** Read at click time, so a change in Settings applies to a transcript already rendered. */
export function opensShareLinksInPanel(): boolean {
  return isDesktopShell() && getShareLinkOpen() === 'panel';
}
