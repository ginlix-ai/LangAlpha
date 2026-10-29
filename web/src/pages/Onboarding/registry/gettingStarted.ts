import { DESKTOP_DOWNLOAD_URL, PLATFORM_URL } from '@/config/supportLinks';
import { openComputersPanel } from '@/pages/ChatAgent/hooks/computerPanelStore';
import { WELCOME_PATH } from '../welcome/paths';
import type { GettingStartedTaskDef } from './types';

/**
 * The getting-started checklist, in presentation order: tour the pages first,
 * then the investing profile, then create a PTC workspace and run a first
 * research in it, then model configuration, then the desktop app (from the
 * browser) or remote control of this Mac (in the app), then channel
 * integrations. Tasks
 * complete via a route visit (stamped into prefs), a derived signal
 * (`doneWhen`), or, for an external destination with neither, the click
 * itself; clicking a pending task navigates to `to`.
 */
export const GETTING_STARTED_TASKS: GettingStartedTaskDef[] = [
  {
    id: 'dashboard',
    titleKey: 'onboarding.gettingStarted.tasks.dashboard.title',
    descKey: 'onboarding.gettingStarted.tasks.dashboard.desc',
    to: '/dashboard',
    visitRoute: (p) => p === '/dashboard',
  },
  {
    id: 'market',
    titleKey: 'onboarding.gettingStarted.tasks.market.title',
    descKey: 'onboarding.gettingStarted.tasks.market.desc',
    to: '/market',
    visitRoute: (p) => p === '/market',
  },
  {
    id: 'profile',
    titleKey: 'onboarding.gettingStarted.tasks.profile.title',
    descKey: 'onboarding.gettingStarted.tasks.profile.desc',
    to: WELCOME_PATH,
    doneWhen: 'onboardingCompleted',
  },
  {
    id: 'createWorkspace',
    titleKey: 'onboarding.gettingStarted.tasks.createWorkspace.title',
    descKey: 'agents.tour.createWorkspaceTask',
    to: '/chat',
    doneWhen: 'hasWorkspace',
  },
  {
    id: 'firstChat',
    titleKey: 'onboarding.gettingStarted.tasks.firstChat.title',
    descKey: 'onboarding.gettingStarted.tasks.firstChat.desc',
    to: '/chat',
    visitRoute: (p) => /^\/chat\/t\/(?!__default__)/.test(p),
  },
  {
    id: 'models',
    titleKey: 'onboarding.gettingStarted.tasks.models.title',
    descKey: 'onboarding.gettingStarted.tasks.models.desc',
    to: '/settings?tab=model',
    visitRoute: (p, s) => p === '/settings' && new URLSearchParams(s).get('tab') === 'model',
  },
  {
    id: 'desktopApp',
    titleKey: 'onboarding.gettingStarted.tasks.desktopApp.title',
    descKey: 'onboarding.gettingStarted.tasks.desktopApp.desc',
    to: DESKTOP_DOWNLOAD_URL,
    external: true,
    offeredOn: 'browser',
    doneWhen: 'inDesktopApp',
  },
  {
    id: 'remoteControl',
    titleKey: 'onboarding.gettingStarted.tasks.remoteControl.title',
    descKey: 'onboarding.gettingStarted.tasks.remoteControl.desc',
    to: '/chat',
    // The Computers panel mounts with the chat page and reads its store as it
    // does, so it opens onto the form however the page is reached.
    open: () => openComputersPanel({ view: 'create', pick: 'thisMac' }),
    offeredOn: 'thisMac',
    doneWhen: 'thisMacAdded',
  },
  {
    id: 'integrations',
    titleKey: 'onboarding.gettingStarted.tasks.integrations.title',
    descKey: 'onboarding.gettingStarted.tasks.integrations.desc',
    // Platform only, so the portal is configured wherever this task shows.
    to: `${PLATFORM_URL ?? ''}/integrations`,
    external: true,
    platformOnly: true,
  },
];
