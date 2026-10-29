/**
 * Where the welcome flow lives, and the URLs that open it. Kept apart from the
 * page so the eager code that links to it (the shell, the onboarding provider,
 * the connect landing) never pulls the flow itself onto the first load.
 */

export const WELCOME_PATH = '/welcome';

/** Every screen the flow can show, in the order a new user meets them. */
export const WELCOME_STEPS = ['brokerage', 'experience', 'risk', 'markets', 'style', 'agent'] as const;
export type WelcomeStepId = (typeof WELCOME_STEPS)[number];

/** The query parameter that opens the flow on one screen. Read once, on
 *  mount: the brokerage sign-in comes back to the URL it was given. */
export const WELCOME_STEP_PARAM = 'step';

/** Opens the flow as an editor of the saved profile, from Settings. */
export const WELCOME_EDIT_PARAM = 'edit';

/** Opened from the dashboard (or by itself) as a first walk-through, or from
 *  Settings to change the saved answers. */
export type WelcomeMode = 'new' | 'edit';

/** The screens a visit walks through. Editing from Settings is the profile
 *  questions alone. */
export function welcomeSteps(mode: WelcomeMode): WelcomeStepId[] {
  if (mode === 'edit') return ['experience', 'risk', 'markets', 'style', 'agent'];
  return [...WELCOME_STEPS];
}

export function isWelcomeStep(value: unknown): value is WelcomeStepId {
  return typeof value === 'string' && (WELCOME_STEPS as readonly string[]).includes(value);
}

/**
 * The screen a visit opens on: the first of `wanted` (the draft's screen, then
 * the URL's) this visit has, else the first.
 *
 * Never past a question left unanswered: each screen's Continue is the only
 * thing that asks for its answers, and a typed `?step=` or a draft an older
 * build left (one that never asked a question added since) would walk
 * straight past it. It opens at that question instead. `answered` comes from
 * the caller, so this file stays clear of the profile rules, and it is true
 * for a screen with nothing to answer.
 */
export function openingStep(
  steps: readonly WelcomeStepId[],
  wanted: ReadonlyArray<WelcomeStepId | null | undefined>,
  answered: (step: WelcomeStepId) => boolean = () => true,
): WelcomeStepId {
  const step = wanted.find((s): s is WelcomeStepId => s != null && steps.includes(s)) ?? steps[0];
  return steps.slice(0, steps.indexOf(step)).find((s) => !answered(s)) ?? step;
}

export function welcomeUrl({ step, edit = false }: { step?: WelcomeStepId; edit?: boolean } = {}): string {
  const params = new URLSearchParams();
  if (edit) params.set(WELCOME_EDIT_PARAM, '1');
  if (step) params.set(WELCOME_STEP_PARAM, step);
  const search = `${params}`;
  return search ? `${WELCOME_PATH}?${search}` : WELCOME_PATH;
}

/**
 * The parameter the brokerage sheet used to open on, before this flow
 * replaced it. Links and sign-ins started from an older build still carry it,
 * so the shell sends them on to the flow's brokerage screen.
 */
const LEGACY_PARAM = 'onboarding';
const LEGACY_VALUE = 'connect';

export function isLegacyOnboardingSearch(search: string): boolean {
  return new URLSearchParams(search).get(LEGACY_PARAM) === LEGACY_VALUE;
}

/** The legacy URL rewritten onto the flow, keeping whatever else it carries
 *  (a sign-in's callback answer among it) for the reader that wants it. */
export function legacyOnboardingTarget(search: string): string {
  const params = new URLSearchParams(search);
  params.delete(LEGACY_PARAM);
  params.set(WELCOME_STEP_PARAM, 'brokerage');
  return `${WELCOME_PATH}?${params}`;
}
