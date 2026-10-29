import { isPlatformMode } from '@/config/hostMode';
import { isDesktopShell } from '@/lib/desktop';
import { PAGE_INTROS } from './pageIntros';
import { GETTING_STARTED_TASKS } from './gettingStarted';
import type { GettingStartedTaskDef, PageIntroDef } from './types';

export { ANNOUNCEMENTS } from './announcements';
export { PAGE_INTROS, GETTING_STARTED_TASKS };
export * from './types';

/**
 * Tasks offered in this deployment and this app: channel integrations are
 * platform-hosted, the desktop app is offered in a browser, and remote control
 * in the app (whether this Mac can be a computer is the provider's to read).
 */
export const DEPLOYED_TASKS: GettingStartedTaskDef[] = GETTING_STARTED_TASKS.filter(
  (t) =>
    (!t.platformOnly || isPlatformMode) &&
    (t.offeredOn !== 'browser' || !isDesktopShell()) &&
    (t.offeredOn !== 'thisMac' || isDesktopShell())
);

/** Intros as this deployment shows them: platform-only steps drop off a
 *  self-hosted build. A first step cannot be platform-only, so none empties. */
export const DEPLOYED_INTROS: PageIntroDef[] = PAGE_INTROS.map(
  ({ steps: [first, ...rest], ...intro }) => ({
    ...intro,
    steps: [first, ...rest.filter((s) => !s.platformOnly || isPlatformMode)],
  })
);
