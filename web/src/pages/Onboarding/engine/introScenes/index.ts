import { use } from 'react';
import type { IntroVisualId } from '../../registry';
import type { IntroScene } from './types';

export type { IntroScene } from './types';

type SceneMap = Partial<Record<IntroVisualId, IntroScene>>;

// Each loader is a lazy chunk. An intro fetches the chunks its steps map to
// and no others, so it downloads only the scenes it plays, and a What's New
// downloads none. Most intros take one module; the chat intro takes two and
// the thread intro four, one for each part of the app they show.
const chat = () => import('./chat').then((m): SceneMap => m.chatScenes);
const share = () => import('./share').then((m): SceneMap => m.shareScenes);
const workspaces = () => import('./workspaces').then((m): SceneMap => m.workspaceScenes);
const threadFiles = () => import('./threadFiles').then((m): SceneMap => m.threadFileScenes);
const threadChart = () => import('./threadChart').then((m): SceneMap => m.threadChartScenes);
const thread = () => import('./thread').then((m): SceneMap => m.threadScenes);
const dashboard = () => import('./dashboard').then((m): SceneMap => m.dashboardScenes);
const market = () => import('./market').then((m): SceneMap => m.marketScenes);
const automations = () => import('./automations').then((m): SceneMap => m.automationScenes);

const MODULES: Record<IntroVisualId, () => Promise<SceneMap>> = {
  twoModes: chat,
  chiefOfStaffAnswer: chat,
  ptcSandbox: chat,
  shareThread: share,
  workspaceGrid: workspaces,
  createWorkspace: workspaces,
  filePanel: threadFiles,
  chartPanel: threadChart,
  memory: thread,
  memo: thread,
  dashboardGrid: dashboard,
  dashboardCustomize: dashboard,
  dashboardAttach: dashboard,
  marketAsk: market,
  marketDraw: market,
  automationTrigger: automations,
  automationAsk: automations,
  automationFeed: automations,
  automationDelivery: automations,
};

export const INTRO_VISUALS = Object.keys(MODULES) as IntroVisualId[];

const loaded = new Map<IntroVisualId, IntroScene>();
const loads = new Map<string, Promise<void>>();

async function loadScene(visual: IntroVisualId): Promise<void> {
  try {
    const scene = (await MODULES[visual]())[visual];
    if (scene) loaded.set(visual, scene);
  } catch {
    // Left out rather than thrown: the intro still opens, with its copy and an
    // empty visual panel. A deploy's dead chunk is caught by the page's
    // stale-build recovery, which watches the failed request itself.
  }
}

/** Fetches the scenes for these visuals; one promise per list, kept for `use`. */
export function loadIntroScenes(visuals: readonly IntroVisualId[]): Promise<void> {
  const key = visuals.join();
  let load = loads.get(key);
  if (!load) {
    load = Promise.all(visuals.map(loadScene)).then(() => {});
    loads.set(key, load);
  }
  return load;
}

/** The scenes for these visuals, in order, suspending until they are in. */
export function useIntroScenes(visuals: readonly IntroVisualId[]): (IntroScene | undefined)[] {
  // Read straight from the cache once loaded, so a remount never suspends.
  if (!visuals.every((v) => loaded.has(v))) use(loadIntroScenes(visuals));
  return visuals.map((v) => loaded.get(v));
}
