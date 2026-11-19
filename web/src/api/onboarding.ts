/**
 * The onboarding progress record: the two onboarding answers, the hosted
 * onboarding's own progress (`plan_step_done_at`), the pair prompt's answer
 * and where setup stands. The hosted onboarding writes the answers and its
 * own progress; the app writes the rest.
 */
import { api } from '@/api/client';

export type OnboardingModels = 'claude_code' | 'subscription_or_key' | 'langalpha_models';
export type OnboardingRunsOn = 'cloud' | 'own_computer';
export type OnboardingThisMac = 'paired' | 'declined' | 'unavailable';
export type OnboardingSetupOn = 'browser' | 'mac_pending' | 'app';
export type OnboardingSetupStep = 'computer' | 'model' | 'default';

/** Every field null for a user who has not started. */
export interface OnboardingProgress {
  models: OnboardingModels | null;
  runs_on: OnboardingRunsOn | null;
  plan_step_done_at: string | null;
  /** The account setup stamp the hosted onboarding sets, read fresh with the
   *  record; null where there is none (self-hosted, or not yet stamped). */
  account_setup_completed_at?: string | null;
  this_mac: OnboardingThisMac | null;
  setup_on: OnboardingSetupOn | null;
  setup_step: OnboardingSetupStep | null;
  setup_completed_at: string | null;
  created_at: string | null;
  updated_at: string | null;
}

/**
 * Only the keys sent are written; null clears one. `plan_step_done` and
 * `setup_completed` stamp their time the first time they are true and keep
 * that stamp; false clears it. Any other key is a 422.
 */
export interface OnboardingProgressPatch {
  models?: OnboardingModels | null;
  runs_on?: OnboardingRunsOn | null;
  this_mac?: OnboardingThisMac | null;
  setup_on?: OnboardingSetupOn | null;
  setup_step?: OnboardingSetupStep | null;
  plan_step_done?: boolean | null;
  setup_completed?: boolean | null;
}

export async function getOnboardingProgress(): Promise<OnboardingProgress> {
  const { data } = await api.get<OnboardingProgress>('/api/v1/users/me/onboarding');
  return data;
}

export async function patchOnboardingProgress(body: OnboardingProgressPatch): Promise<OnboardingProgress> {
  const { data } = await api.patch<OnboardingProgress>('/api/v1/users/me/onboarding', body);
  return data;
}
