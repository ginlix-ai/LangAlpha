import type { PageIntroDef } from './types';

/**
 * One contextual intro per page area, shown the first time the user lands
 * there (not all at once). Order matters only when several match one route —
 * the first unseen match wins.
 */
export const PAGE_INTROS: PageIntroDef[] = [
  {
    id: 'chat',
    // Workspace gallery + a workspace's thread gallery.
    matchRoute: (p) => p === '/chat' || /^\/chat\/(?!t\/)[^/]+$/.test(p),
    steps: [
      { id: 'modes', visual: 'twoModes' },
      {
        id: 'chiefOfStaff',
        visual: 'chiefOfStaffAnswer',
        // It answers from iMessage, and chat apps link through the hosted platform.
        platformOnly: true,
      },
      { id: 'ptc', visual: 'ptcSandbox' },
      { id: 'workspace', visual: 'workspaceGrid' },
      { id: 'create', visual: 'createWorkspace' },
    ],
  },
  {
    id: 'thread',
    // Any real thread; the personalization chat (__default__) keeps its own flow.
    matchRoute: (p) => /^\/chat\/t\/(?!__default__)/.test(p),
    steps: [
      { id: 'filePanel', visual: 'filePanel' },
      { id: 'chart', visual: 'chartPanel' },
      { id: 'memory', visual: 'memory' },
      { id: 'memo', visual: 'memo' },
      { id: 'share', visual: 'shareThread' },
    ],
  },
  {
    id: 'dashboard',
    matchRoute: (p) => p === '/dashboard',
    steps: [
      { id: 'overview', visual: 'dashboardGrid' },
      { id: 'customize', visual: 'dashboardCustomize' },
      { id: 'attach', visual: 'dashboardAttach' },
    ],
  },
  {
    id: 'market',
    matchRoute: (p) => p === '/market',
    steps: [
      { id: 'ask', visual: 'marketAsk' },
      { id: 'draw', visual: 'marketDraw' },
    ],
  },
  {
    id: 'automations',
    matchRoute: (p) => p === '/automations',
    steps: [
      { id: 'trigger', visual: 'automationTrigger' },
      { id: 'ask', visual: 'automationAsk' },
      { id: 'feed', visual: 'automationFeed' },
      {
        id: 'delivery',
        visual: 'automationDelivery',
        // Chat apps link through the hosted platform.
        platformOnly: true,
      },
    ],
  },
];
