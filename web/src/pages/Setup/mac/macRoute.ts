/**
 * The Mac route's decisions, kept pure so each page reads one rule: which
 * `then` a page may be sent on to, where the pair prompt goes once answered,
 * and what the continue prompt says is left.
 */
import type { OnboardingSetupOn, OnboardingThisMac } from '@/api/onboarding';
import type { SetupAnswers, SetupStep } from '../answers/setupSteps';

/**
 * `raw` when it is a page of this app or on the origin of `PLATFORM_URL`,
 * else null. It arrives in a URL anyone can craft, so anything wider is an
 * open redirect. `platform` is that base as `PLATFORM_URL` gives it, a path
 * on this origin or an absolute URL.
 */
export function safeThen(raw: string | null, origin: string, platform: string | null): string | null {
  if (!raw) return null;
  let url: URL;
  try {
    url = new URL(raw, origin);
  } catch {
    return null;
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return null;
  const allowed = new Set([origin]);
  if (platform) {
    try {
      allowed.add(new URL(platform, origin).origin);
    } catch {
      // A base that does not parse adds nothing.
    }
  }
  return allowed.has(url.origin) ? url.href : null;
}

export type ThisMacOutcome = OnboardingThisMac;

export interface AfterThisMacInput {
  outcome: ThisMacOutcome;
  answers: SetupAnswers | null;
  setupOn: OnboardingSetupOn | null;
  setupCompleted: boolean;
  /** A `then` already checked by `safeThen`. */
  then: string | null;
  /** The hosted onboarding questions, or null where this app asks them. */
  platformUse: string | null;
  /** Whether a computer of the user's own is connected. */
  ownComputer: boolean;
}

/**
 * Where the pair prompt goes once answered. With answers, setup started in a
 * browser hands over through the continue prompt, and setup already running
 * in this app carries on its computer step (`finishComputer`) or goes back
 * to it after Not now. Without answers (desktop first), the questions come
 * next: at `then` where one was given, else where the questions are asked
 * (`platformUse`, or this app's page).
 */
export type AfterThisMac =
  | { kind: 'path'; to: string }
  | { kind: 'external'; to: string }
  | { kind: 'finishComputer' };

export function afterThisMac(input: AfterThisMacInput): AfterThisMac {
  if (input.answers) {
    if (input.setupCompleted) return { kind: 'path', to: '/' };
    // Unclaimed setup is this app's: its own step claims it as it opens.
    if (input.setupOn !== null && input.setupOn !== 'app') return { kind: 'path', to: '/setup/continue' };
    if (input.outcome === 'paired' && input.answers.runsOn === 'own_computer') return { kind: 'finishComputer' };
    if (input.answers.runsOn === 'own_computer' && !input.ownComputer) return { kind: 'path', to: '/setup/computer' };
    return { kind: 'path', to: '/setup' };
  }
  if (input.then) return { kind: 'external', to: input.then };
  if (input.platformUse) return { kind: 'external', to: input.platformUse };
  return { kind: 'path', to: '/setup/use' };
}

/** One line of what the continue prompt says is left. */
export type LeftItem = 'computer' | 'tokenThisMac' | 'token' | 'subscription' | 'default' | 'invest' | 'firstChat';

/**
 * What is left from the step setup resumes on: the steps on the rail from
 * there, in the words of the answers, then the welcome questions and the
 * first chat, which come after setup on every route.
 */
export function leftItems(rail: readonly SetupStep[], next: SetupStep | 'done', answers: SetupAnswers, paired: boolean): LeftItem[] {
  const from = next === 'done' ? rail.length : Math.max(0, rail.indexOf(next));
  const steps: LeftItem[] = rail.slice(from).map((step) => {
    if (step === 'computer') return 'computer';
    if (step === 'default') return 'default';
    if (answers.models === 'claude_code') return paired && answers.runsOn === 'own_computer' ? 'tokenThisMac' : 'token';
    return 'subscription';
  });
  return [...steps, 'invest', 'firstChat'];
}
