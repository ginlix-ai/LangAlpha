// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { afterThisMac, leftItems, safeThen, type AfterThisMacInput } from '../macRoute';

const ORIGIN = 'https://app.example';

describe('safeThen', () => {
  it.each([
    ['/account/onboarding/use', '/account', 'https://app.example/account/onboarding/use'],
    ['https://console.example/onboarding/use', 'https://console.example', 'https://console.example/onboarding/use'],
    ['https://app.example/setup', null, 'https://app.example/setup'],
  ])('takes %s', (raw, platform, want) => {
    expect(safeThen(raw, ORIGIN, platform)).toBe(want);
  });

  it.each([
    ['https://evil.example/onboarding/use'],
    ['//evil.example/x'],
    ['javascript:alert(1)'],
    [''],
  ])('refuses %s', (raw) => {
    expect(safeThen(raw, ORIGIN, 'https://console.example')).toBeNull();
  });
});

const base: AfterThisMacInput = {
  outcome: 'paired',
  answers: { models: 'claude_code', runsOn: 'own_computer' },
  setupOn: 'mac_pending',
  setupCompleted: false,
  then: null,
  platformUse: 'https://console.example/onboarding/use',
  ownComputer: true,
};

describe('afterThisMac', () => {
  it('hands setup a browser started to the continue prompt, paired or not', () => {
    expect(afterThisMac(base)).toEqual({ kind: 'path', to: '/setup/continue' });
    expect(afterThisMac({ ...base, outcome: 'declined', setupOn: 'browser' })).toEqual({ kind: 'path', to: '/setup/continue' });
  });

  it('takes unclaimed setup as the app\'s own, which its step just claimed', () => {
    expect(afterThisMac({ ...base, setupOn: null })).toEqual({ kind: 'finishComputer' });
  });

  it('carries on the computer step where setup already runs in the app', () => {
    expect(afterThisMac({ ...base, setupOn: 'app' })).toEqual({ kind: 'finishComputer' });
    expect(afterThisMac({ ...base, setupOn: 'app', outcome: 'declined', ownComputer: false }))
      .toEqual({ kind: 'path', to: '/setup/computer' });
  });

  it('sends a user with no answers where the funnel said, else to the questions', () => {
    const none = { ...base, answers: null };
    expect(afterThisMac({ ...none, then: 'https://console.example/onboarding/use?x=1' }))
      .toEqual({ kind: 'external', to: 'https://console.example/onboarding/use?x=1' });
    expect(afterThisMac(none)).toEqual({ kind: 'external', to: 'https://console.example/onboarding/use' });
    expect(afterThisMac({ ...none, platformUse: null })).toEqual({ kind: 'path', to: '/setup/use' });
  });

  it('leaves for the app once setup is done', () => {
    expect(afterThisMac({ ...base, setupCompleted: true })).toEqual({ kind: 'path', to: '/' });
  });
});

describe('leftItems', () => {
  it('names the steps from the one setup resumes on, then the welcome and the first chat', () => {
    expect(leftItems(['computer', 'model', 'default'], 'model', { models: 'claude_code', runsOn: 'own_computer' }, true))
      .toEqual(['tokenThisMac', 'default', 'invest', 'firstChat']);
    expect(leftItems(['computer', 'model', 'default'], 'computer', { models: 'claude_code', runsOn: 'own_computer' }, false))
      .toEqual(['computer', 'token', 'default', 'invest', 'firstChat']);
    expect(leftItems(['model', 'default'], 'model', { models: 'subscription_or_key', runsOn: 'cloud' }, false))
      .toEqual(['subscription', 'default', 'invest', 'firstChat']);
    expect(leftItems(['default'], 'default', { models: 'langalpha_models', runsOn: 'cloud' }, true))
      .toEqual(['default', 'invest', 'firstChat']);
  });
});
