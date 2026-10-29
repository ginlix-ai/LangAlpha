/**
 * A versioned feature announcement. Drives the "What's New" modal only — copy
 * is i18n keys under `onboarding.announce.*`. Adding one = this object + i18n
 * keys; no engine changes.
 */
export interface AnnouncementDef {
  /** Stable identifier (React keys, analytics). Unseen-ness is version-granular. */
  key: string;
  /**
   * CalVer ship date — 'YYYY.MM.DD' with an optional '.N' for same-day releases
   * (e.g. '2026.04.26'); just the date the feature ships, no git tag or GitHub
   * release required. The sole driver of unseen-ness: anything newer than the
   * user's lastSeenReleaseVersion shows, so a new entry must be strictly newer
   * than the current max or it will NOT surface (users are already stamped at
   * the max).
   */
  releaseVersion: string;
  modalTitleKey: string;
  modalBodyKey: string;
}

/**
 * Illustration scene rendered in the intro modal's visual panel. Implemented
 * in `engine/introScenes/index.ts` — the Record there is typed against this union,
 * so adding an id without a scene is a compile error.
 */
export type IntroVisualId =
  | 'twoModes'
  | 'workspaceGrid'
  | 'chiefOfStaffAnswer'
  | 'ptcSandbox'
  | 'shareThread'
  | 'createWorkspace'
  | 'filePanel'
  | 'chartPanel'
  | 'memory'
  | 'memo'
  | 'dashboardGrid'
  | 'dashboardCustomize'
  | 'dashboardAttach'
  | 'marketAsk'
  | 'marketDraw'
  | 'automationTrigger'
  | 'automationAsk'
  | 'automationFeed'
  | 'automationDelivery';

/**
 * One stage of a page intro. Its copy is `<intro id>.<step id>.title` and
 * `.body` in the `intros` namespace (locales/intros/), which loads with the
 * first intro rather than on every page.
 */
export interface PageIntroStepDef {
  /** Stable id — React key + the copy's key segment. */
  id: string;
  visual: IntroVisualId;
  /**
   * Only on the hosted platform, where the step's feature exists (chat app
   * delivery, say); a self-hosted build skips it. Never on a first step, which
   * `PageIntroDef.steps` rejects.
   */
  platformOnly?: boolean;
}

/**
 * A one-time contextual intro, shown the first time the user lands on a
 * matching page. Multi-stage: each step pairs copy with a mockup scene.
 * Seen-state is per-intro — closing at any step marks the whole intro seen.
 */
export interface PageIntroDef {
  /** Stable id — keys the per-user seen map in prefs. */
  id: string;
  /** Which pages this intro belongs to. */
  matchRoute: (pathname: string) => boolean;
  /**
   * At least one step — the modal indexes steps[0], so empty would crash. The
   * first shows on every deployment, so dropping platform-only steps never
   * empties an intro.
   */
  steps: [PageIntroStepDef & { platformOnly?: false }, ...PageIntroStepDef[]];
}

/** A task on the getting-started list in the sidebar. Copy is i18n keys. */
export interface GettingStartedTaskDef {
  id: string;
  titleKey: string;
  descKey: string;
  /** Where clicking the task navigates (router path, or a full URL when external). */
  to: string;
  /**
   * Cross-app destination, stamped done on click: a new tab in the browser,
   * this window in the desktop app.
   */
  external?: boolean;
  /** Only offered on the hosted platform (e.g. channel integrations). */
  platformOnly?: boolean;
  /**
   * Where else it is offered. `browser`: outside the desktop app. `thisMac`:
   * in the desktop app on a Mac this server can take as a computer (remote
   * control), the task kept once done there.
   */
  offeredOn?: 'browser' | 'thisMac';
  /** Opens a surface before following `to`, one the page at `to` shows however it is reached. */
  open?: () => void;
  /** Auto-completes when a visited location matches (stamped into prefs). */
  visitRoute?: (pathname: string, search: string) => boolean;
  /**
   * Derived completion signal, stamped once true so later sessions skip its
   * lookup; a task with one is not stamped by its click. `hasWorkspace`: a
   * workspace other than Home exists. `thisMacAdded`: a computer is this Mac.
   * `inDesktopApp`: the desktop app is open, which the browser's list then
   * shows too. `onboardingCompleted`: the user row says the investing profile
   * is saved, never stamped, so a reset un-checks it.
   */
  doneWhen?: DoneSignal;
}

export type DoneSignal = 'hasWorkspace' | 'onboardingCompleted' | 'thisMacAdded' | 'inDesktopApp';
