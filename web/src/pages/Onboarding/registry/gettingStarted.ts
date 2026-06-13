import type { GettingStartedTaskDef } from './types';

const ACCOUNT_URL = (import.meta.env.VITE_ACCOUNT_URL as string | undefined) || '/account';

/**
 * The getting-started checklist, in presentation order: tour the pages first,
 * then the Flash interview, then create a PTC workspace and run a first
 * research in it, then model configuration, then channel integrations. Tasks
 * complete via a route visit (stamped into prefs), a derived signal
 * (`doneWhen`), or — for external destinations — the click itself; clicking a
 * pending task navigates to `to`.
 *
 * The two interview tasks both open the Flash personalization chat: it covers
 * watchlist/portfolio and preferences in one conversation, but each half has
 * its own completion signal so a partial interview shows what's still missing.
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
    id: 'stocks',
    titleKey: 'onboarding.gettingStarted.tasks.stocks.title',
    descKey: 'onboarding.gettingStarted.tasks.stocks.desc',
    to: '/chat/t/__default__',
    interview: true,
    doneWhen: 'hasStocks',
  },
  {
    id: 'preferences',
    titleKey: 'onboarding.gettingStarted.tasks.preferences.title',
    descKey: 'onboarding.gettingStarted.tasks.preferences.desc',
    to: '/chat/t/__default__',
    interview: true,
    doneWhen: 'hasPreferences',
  },
  {
    id: 'createWorkspace',
    titleKey: 'onboarding.gettingStarted.tasks.createWorkspace.title',
    descKey: 'onboarding.gettingStarted.tasks.createWorkspace.desc',
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
    id: 'integrations',
    titleKey: 'onboarding.gettingStarted.tasks.integrations.title',
    descKey: 'onboarding.gettingStarted.tasks.integrations.desc',
    to: `${ACCOUNT_URL}/integrations`,
    external: true,
    platformOnly: true,
  },
];
