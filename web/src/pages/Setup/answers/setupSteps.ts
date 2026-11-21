/**
 * Which setup steps the two onboarding answers call for, and which one comes
 * next. Pure, so the gate, the wizard's rail and its resume read one rule.
 *
 * Setup runs only what the answers need, in this order: on a self-hosted
 * server not set up yet, the server's own step (where it runs agent code, the
 * keys its tools use); a computer of the user's own when tasks run on one and
 * none is connected yet; then the model
 * (Claude Code's setup token, or a subscription or key; nothing for the
 * deployment's own models), then the default model.
 */
import type { OnboardingModels, OnboardingProgress, OnboardingProgressPatch, OnboardingRunsOn, OnboardingSetupStep } from '@/api/onboarding';
import { WELCOME_PATH } from '@/pages/Onboarding/welcome/paths';

export type ModelsAnswer = OnboardingModels;
export type RunsOnAnswer = OnboardingRunsOn;
/** The account's steps, which its onboarding record keeps, and `server`, the
 *  deployment's, which the server's setup status keeps instead: it is
 *  answered once for every account there. */
export type SetupStep = OnboardingSetupStep | 'server';

export interface SetupAnswers {
  models: ModelsAnswer;
  runsOn: RunsOnAnswer;
}

/** Where the server's own step stands: `owed` until it is answered, `done`
 *  after, null where there is none (hosted, or a server set up before it). */
export type ServerStepState = 'owed' | 'done' | null;

export interface SetupFacts {
  answers: SetupAnswers;
  server: ServerStepState;
  /** A computer of the user's own is connected; a paired this Mac counts. */
  ownComputer: boolean;
  /** Home lives on a cloud computer, the primary. */
  homeOnCloud: boolean;
  /** A Claude Code setup token is in place where the answers run it: on the
   *  own computer, or saved for the cloud computer. */
  claudeCodeToken: boolean;
  /** A subscription or key of the user's own is connected. */
  providerConnected: boolean;
  /** The last setup step the record says was finished. */
  lastStep: SetupStep | null;
}

export const SETUP_STEP_PATHS: Readonly<Record<SetupStep, string>> = {
  server: '/setup/server',
  computer: '/setup/computer',
  model: '/setup/model',
  default: '/setup/default',
};

/** The page a resume goes to: the step's own, or /welcome once setup is done. */
export function nextSetupPath(next: SetupStep | 'done'): string {
  return next === 'done' ? WELCOME_PATH : SETUP_STEP_PATHS[next];
}

/** Both answers, or null while either is missing. */
export function readAnswers(progress: Pick<OnboardingProgress, 'models' | 'runs_on'> | null): SetupAnswers | null {
  if (!progress?.models || !progress.runs_on) return null;
  return { models: progress.models, runsOn: progress.runs_on };
}

/** Whether the answers need a model connected in setup. */
export const needsModelStep = (answers: SetupAnswers): boolean => answers.models !== 'langalpha_models';

/**
 * The steps the rail shows, from the answers alone: a step whose outcome is
 * already in place stays on it as a finished one (`nextSetupStep` goes past
 * it), so connecting a computer never changes the rail under the user.
 */
export function setupRail(answers: SetupAnswers, server: ServerStepState): SetupStep[] {
  return [
    ...(server ? (['server'] as const) : []),
    ...(answers.runsOn === 'own_computer' ? (['computer'] as const) : []),
    ...(needsModelStep(answers) ? (['model'] as const) : []),
    'default',
  ];
}

/**
 * The step to resume on, or `done` once the default step was finished. A step
 * whose outcome is in place is behind the user whatever the record says, and
 * one whose outcome is gone (a computer removed, a token taken off) is not.
 */
export function nextSetupStep(facts: SetupFacts): SetupStep | 'done' {
  if (facts.lastStep === 'default') return 'done';
  if (facts.server === 'owed') return 'server';
  const { models, runsOn } = facts.answers;
  // One already connected while Home is on the cloud is offered as Home once.
  if (runsOn === 'own_computer' && (!facts.ownComputer || (facts.homeOnCloud && facts.lastStep === null))) return 'computer';
  if (models === 'claude_code' && !facts.claudeCodeToken) return 'model';
  if (models === 'subscription_or_key' && !facts.providerConnected) return 'model';
  return 'default';
}

/**
 * What finishing `step` writes to the account's record: the step, and on the
 * last one that setup is done. Nothing for the server's own step, which the
 * server's setup status keeps (the step saves it there itself).
 */
export function stepRecord(step: SetupStep): OnboardingProgressPatch | null {
  switch (step) {
    case 'server': return null;
    case 'default': return { setup_step: 'default', setup_completed: true };
    default: return { setup_step: step };
  }
}

/** The step before `step` on the rail, for Back; null on the first. */
export function previousSetupStep(rail: readonly SetupStep[], step: SetupStep): SetupStep | null {
  const at = rail.indexOf(step);
  return at > 0 ? rail[at - 1] : null;
}

/** The step after `step` on the rail; null after the last. */
export function followingSetupStep(rail: readonly SetupStep[], step: SetupStep): SetupStep | null {
  const at = rail.indexOf(step);
  return at >= 0 && at < rail.length - 1 ? rail[at + 1] : null;
}
