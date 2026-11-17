// @vitest-environment node
import { describe, expect, it } from 'vitest';
import {
  onboardingRoute,
  setupStartTarget,
  shellTarget,
  type OnboardingRoute,
  type OnboardingRouteInput,
} from '../gate';

const input = (over: Partial<OnboardingRouteInput>): OnboardingRouteInput => ({
  platform: true,
  onboarded: true,
  hasAnswers: false,
  setupCompleted: false,
  setupOn: null,
  next: null,
  needsProvider: false,
  unpaired: false,
  ...over,
});

const answers = (
  owner: OnboardingRouteInput['setupOn'],
  next: 'computer' | 'model' | 'default' = 'model',
  unpaired = false,
): OnboardingRoute => onboardingRoute(input({ hasAnswers: true, setupOn: owner, next, unpaired }));

describe('onboardingRoute', () => {
  it('sends a hosted account that was never onboarded to the platform first, answered or not', () => {
    expect(onboardingRoute(input({ onboarded: false }))).toEqual({ kind: 'platform' });
    expect(onboardingRoute(input({ onboarded: false, hasAnswers: true, next: 'model' }))).toEqual({ kind: 'platform' });
  });

  it('says setup runs on the answers, with its next step and whoever has it', () => {
    expect(answers(null, 'computer')).toEqual({ kind: 'answers', next: 'computer', nextPath: '/setup/computer', owner: null, unpaired: false });
    expect(answers('app', 'default')).toEqual({ kind: 'answers', next: 'default', nextPath: '/setup/default', owner: 'app', unpaired: false });
    expect(answers('browser', 'model', true)).toMatchObject({ kind: 'answers', unpaired: true });
  });

  it('is done once setup finished, and waits while the step is unknown', () => {
    expect(onboardingRoute(input({ hasAnswers: true, setupCompleted: true, next: 'done' }))).toEqual({ kind: 'done' });
    expect(onboardingRoute(input({ hasAnswers: true, next: 'done' }))).toEqual({ kind: 'done' });
    expect(onboardingRoute(input({ hasAnswers: true, next: null }))).toEqual({ kind: 'loading' });
  });

  it('asks a self-hosted account that was never set up the two questions, and leaves one that was alone', () => {
    expect(onboardingRoute(input({ platform: false, onboarded: false }))).toEqual({ kind: 'questions', unpaired: false });
    expect(onboardingRoute(input({ platform: false, onboarded: true }))).toEqual({ kind: 'done' });
  });

  it('sends an account without answers and without a model to the provider setup, never an answered one', () => {
    expect(onboardingRoute(input({ needsProvider: true }))).toEqual({ kind: 'provider' });
    expect(onboardingRoute(input({ platform: false, needsProvider: true }))).toEqual({ kind: 'provider' });
    expect(onboardingRoute(input({ hasAnswers: true, setupCompleted: true, next: 'done', needsProvider: true }))).toEqual({ kind: 'done' });
  });
});

describe('shellTarget', () => {
  const browser = { desktop: false, skipped: false };
  const desktop = { desktop: true, skipped: false };

  it('resumes setup in the browser that has it or that nobody claimed', () => {
    expect(shellTarget(answers(null, 'computer'), browser)).toEqual({ kind: 'path', to: '/setup/computer' });
    expect(shellTarget(answers('browser', 'default'), browser)).toEqual({ kind: 'path', to: '/setup/default' });
  });

  it('shows the browser where setup went once the Mac took it', () => {
    expect(shellTarget(answers('mac_pending'), browser)).toEqual({ kind: 'path', to: '/setup/mac' });
    expect(shellTarget(answers('app'), browser)).toEqual({ kind: 'path', to: '/setup/mac' });
  });

  it('in the Mac app, forces only setup the app took and the hand-over', () => {
    expect(shellTarget(answers('app'), desktop)).toEqual({ kind: 'path', to: '/setup/model' });
    expect(shellTarget(answers('mac_pending'), desktop)).toEqual({ kind: 'path', to: '/setup/continue' });
    expect(shellTarget(answers('browser'), desktop)).toBeNull();
    expect(shellTarget(answers(null), desktop)).toBeNull();
  });

  it('in a Mac app that can pair, asks the pair prompt first while it was never asked and setup is not finished', () => {
    const canPair = { ...desktop, canPair: true };
    expect(shellTarget(answers('browser', 'model', true), canPair)).toEqual({ kind: 'path', to: '/setup/this-mac' });
    expect(shellTarget(answers(null, 'model', true), canPair)).toEqual({ kind: 'path', to: '/setup/this-mac' });
    expect(shellTarget({ kind: 'questions', unpaired: true }, canPair)).toEqual({ kind: 'path', to: '/setup/this-mac' });
    expect(shellTarget(answers('browser', 'model', false), canPair)).toBeNull();
    expect(shellTarget({ kind: 'done' }, canPair)).toBeNull();
    expect(shellTarget(answers('browser', 'model', true), desktop)).toBeNull();
    expect(shellTarget(answers('browser', 'model', true), { ...browser, canPair: true })).toEqual({ kind: 'path', to: '/setup/model' });
    expect(shellTarget(answers('browser', 'model', true), { ...canPair, skipped: true })).toBeNull();
  });

  it('sends the questions, the provider setup and the platform, and lets done through', () => {
    expect(shellTarget({ kind: 'questions', unpaired: false }, browser)).toEqual({ kind: 'path', to: '/setup/use' });
    expect(shellTarget({ kind: 'provider' }, desktop)).toEqual({ kind: 'path', to: '/setup/method' });
    expect(shellTarget({ kind: 'platform' }, browser)).toEqual({ kind: 'platform' });
    expect(shellTarget({ kind: 'done' }, browser)).toBeNull();
  });

  it('after Exit setup, holds the session back only for the platform', () => {
    const skipped = { desktop: false, skipped: true };
    expect(shellTarget(answers(null), skipped)).toBeNull();
    expect(shellTarget({ kind: 'questions', unpaired: false }, skipped)).toBeNull();
    expect(shellTarget({ kind: 'provider' }, skipped)).toBeNull();
    expect(shellTarget({ kind: 'platform' }, skipped)).toEqual({ kind: 'platform' });
  });
});

describe('setupStartTarget', () => {
  it('goes on to the next answer step whoever claimed setup, the Mac app with nothing claimed included', () => {
    expect(setupStartTarget(answers(null, 'computer') as Exclude<OnboardingRoute, { kind: 'loading' }>)).toEqual({ kind: 'path', to: '/setup/computer' });
    expect(setupStartTarget(answers('browser', 'model') as Exclude<OnboardingRoute, { kind: 'loading' }>)).toEqual({ kind: 'path', to: '/setup/model' });
  });

  it('asks the questions, sends the platform, opens the provider setup for no model, and the app once done', () => {
    expect(setupStartTarget({ kind: 'questions', unpaired: false })).toEqual({ kind: 'path', to: '/setup/use' });
    expect(setupStartTarget({ kind: 'platform' })).toEqual({ kind: 'platform' });
    expect(setupStartTarget({ kind: 'provider' })).toEqual({ kind: 'path', to: '/setup/method' });
    expect(setupStartTarget({ kind: 'done' })).toEqual({ kind: 'path', to: '/' });
  });
});
