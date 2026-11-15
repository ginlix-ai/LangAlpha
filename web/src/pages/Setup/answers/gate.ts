/**
 * Where onboarding stands for an account, as one decision every consumer
 * reads (`onboardingRoute`), and what two consumers make of it: the app
 * shell, which forces a visit elsewhere only for setup this surface owns
 * (`shellTarget`), and an explicit visit to /setup, which always goes on to
 * the step setup resumes on (`setupStartTarget`). Pure, so each case is read
 * in one place.
 */
import type { OnboardingSetupOn as SetupOn } from '@/api/onboarding';
import { nextSetupPath, type SetupStep } from './setupSteps';

export interface OnboardingRouteInput {
  platform: boolean;
  /** Any sign the account was onboarded before: the hosted onboarding's
   *  account setup stamp, a saved default model, the onboarding engine's first-run or
   *  welcome-skip stamps, setup finished, or a computer or thread of its own. */
  onboarded: boolean;
  hasAnswers: boolean;
  setupCompleted: boolean;
  setupOn: SetupOn | null;
  /** The step to resume on, or done; null while unknown. */
  next: SetupStep | 'done' | null;
  /** The older provider setup's question: no key, plan, deployment key or
   *  Claude Code token runs a turn yet. */
  needsProvider: boolean;
  /** The record was read and the pair prompt was never asked there. */
  unpaired: boolean;
}

/**
 * - platform: a hosted account not through the hosted onboarding yet,
 *   answered or not, since it comes before setup.
 * - questions: a self-hosted account never set up, asked the two questions.
 * - answers: setup runs on the two answers and is not finished; `owner` is
 *   the surface that has it (null while no surface has claimed it).
 * `unpaired` on questions and answers: the pair prompt was never asked.
 * - provider: an account without answers that has no model to run a turn.
 * - done: nothing to send the account to.
 */
export type OnboardingRoute =
  | { kind: 'loading' }
  | { kind: 'platform' }
  | { kind: 'questions'; unpaired: boolean }
  | { kind: 'answers'; next: SetupStep; nextPath: string; owner: SetupOn | null; unpaired: boolean }
  | { kind: 'provider' }
  | { kind: 'done' };

export function onboardingRoute(input: OnboardingRouteInput): OnboardingRoute {
  if (input.platform && !input.onboarded) return { kind: 'platform' };
  if (input.hasAnswers) {
    if (input.next === null) return { kind: 'loading' };
    if (input.setupCompleted || input.next === 'done') return { kind: 'done' };
    return { kind: 'answers', next: input.next, nextPath: nextSetupPath(input.next), owner: input.setupOn, unpaired: input.unpaired };
  }
  if (!input.platform && !input.onboarded) return { kind: 'questions', unpaired: input.unpaired };
  return input.needsProvider ? { kind: 'provider' } : { kind: 'done' };
}

/** Where the app shell sends a visit; null lets the page through. */
export type ShellTarget = { kind: 'platform' } | { kind: 'path'; to: string } | null;

/**
 * The shell forces only what this surface owns. In the Mac app that is the
 * pair prompt while it was never asked (where the shell can pair, `canPair`;
 * the prompt skips itself where the server cannot take this Mac), setup the
 * app took, and the continue prompt while a browser hands it over; a browser
 * follows setup to /setup/mac once the Mac has it, and resumes setup it has
 * or nobody claimed yet. "Exit setup" lets the session through everything
 * but the hosted onboarding.
 */
export function shellTarget(
  route: OnboardingRoute,
  { desktop, skipped, canPair = false }: { desktop: boolean; skipped: boolean; canPair?: boolean },
): ShellTarget {
  if (route.kind === 'platform') return { kind: 'platform' };
  if (skipped) return null;
  if (desktop && canPair && (route.kind === 'questions' || route.kind === 'answers') && route.unpaired) {
    return { kind: 'path', to: '/setup/this-mac' };
  }
  if (route.kind === 'questions') return { kind: 'path', to: '/setup/use' };
  if (route.kind === 'provider') return { kind: 'path', to: '/setup/method' };
  if (route.kind !== 'answers') return null;
  if (desktop) {
    if (route.owner === 'app') return { kind: 'path', to: route.nextPath };
    if (route.owner === 'mac_pending') return { kind: 'path', to: '/setup/continue' };
    return null;
  }
  if (route.owner === 'app' || route.owner === 'mac_pending') return { kind: 'path', to: '/setup/mac' };
  return { kind: 'path', to: route.nextPath };
}

/**
 * An explicit visit to /setup (a hand-off from the hosted onboarding, the questions'
 * submit, Settings) goes where onboarding stands on any surface: the step
 * setup resumes on, whoever claimed it; the ways to connect a provider for
 * an account with no model; with nothing left to do, the app, which opens
 * /welcome itself on a new account's first visit.
 */
export function setupStartTarget(route: Exclude<OnboardingRoute, { kind: 'loading' }>): ShellTarget {
  if (route.kind === 'platform') return { kind: 'platform' };
  if (route.kind === 'questions') return { kind: 'path', to: '/setup/use' };
  if (route.kind === 'answers') return { kind: 'path', to: route.nextPath };
  if (route.kind === 'provider') return { kind: 'path', to: '/setup/method' };
  return { kind: 'path', to: '/' };
}
