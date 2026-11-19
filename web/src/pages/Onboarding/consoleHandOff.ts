import { isDesktopShell, openConsolePage } from '@/lib/desktop';

/** The longest a hand-off to a hosted page waits for its task's stamp. */
export const STAMP_WAIT_MS = 1500;

/**
 * Opens a hosted page that completes a task, with the task's stamp
 * (`stamp`) under way. In the desktop app the page shares this window and
 * leaving unloads this one, so it waits for the stamp, at most
 * `STAMP_WAIT_MS`; in a browser it opens a tab at once, inside the click,
 * where a popup blocker would hold one opened later.
 */
export function openAfterStamp(stamp: Promise<unknown>, url: string): void {
  const stamped = Promise.resolve(stamp).catch(() => {});
  if (!isDesktopShell()) {
    openConsolePage(url);
    return;
  }
  const waited = new Promise<void>((resolve) => setTimeout(resolve, STAMP_WAIT_MS));
  void Promise.race([stamped, waited]).then(() => openConsolePage(url));
}
