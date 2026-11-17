// @vitest-environment node
import { describe, expect, it } from 'vitest';
import {
  followingSetupStep,
  nextSetupPath,
  nextSetupStep,
  previousSetupStep,
  readAnswers,
  setupRail,
  type SetupFacts,
} from '../setupSteps';

const facts = (over: Partial<SetupFacts> & { answers: SetupFacts['answers'] }): SetupFacts => ({
  ownComputer: false,
  homeOnCloud: false,
  claudeCodeToken: false,
  providerConnected: false,
  lastStep: null,
  ...over,
});

describe('readAnswers', () => {
  it('needs both answers', () => {
    expect(readAnswers({ models: 'claude_code', runs_on: 'cloud' })).toEqual({ models: 'claude_code', runsOn: 'cloud' });
    expect(readAnswers({ models: 'claude_code', runs_on: null })).toBeNull();
    expect(readAnswers(null)).toBeNull();
  });
});

describe('setupRail', () => {
  it('runs only the steps the answers need', () => {
    expect(setupRail({ models: 'claude_code', runsOn: 'cloud' })).toEqual(['model', 'default']);
    expect(setupRail({ models: 'langalpha_models', runsOn: 'cloud' })).toEqual(['default']);
    expect(setupRail({ models: 'subscription_or_key', runsOn: 'own_computer' })).toEqual(['computer', 'model', 'default']);
    expect(setupRail({ models: 'langalpha_models', runsOn: 'own_computer' })).toEqual(['computer', 'default']);
  });
});

describe('nextSetupStep', () => {
  it('starts with a computer of their own where tasks run on one', () => {
    expect(nextSetupStep(facts({ answers: { models: 'claude_code', runsOn: 'own_computer' } }))).toBe('computer');
  });

  it('offers a computer of their own already connected as Home once, where Home is on the cloud', () => {
    const own = { models: 'langalpha_models', runsOn: 'own_computer' } as const;
    expect(nextSetupStep(facts({ answers: own, ownComputer: true, homeOnCloud: true }))).toBe('computer');
    expect(nextSetupStep(facts({ answers: own, ownComputer: true, homeOnCloud: true, lastStep: 'computer' }))).toBe('default');
  });

  it('asks for the model until its token or connection is in place', () => {
    const own = { models: 'claude_code', runsOn: 'own_computer' } as const;
    expect(nextSetupStep(facts({ answers: own, ownComputer: true }))).toBe('model');
    expect(nextSetupStep(facts({ answers: own, ownComputer: true, claudeCodeToken: true }))).toBe('default');
    const sub = { models: 'subscription_or_key', runsOn: 'cloud' } as const;
    expect(nextSetupStep(facts({ answers: sub }))).toBe('model');
    expect(nextSetupStep(facts({ answers: sub, providerConnected: true }))).toBe('default');
  });

  it('goes straight to the default for the deployment\'s models on a cloud computer', () => {
    expect(nextSetupStep(facts({ answers: { models: 'langalpha_models', runsOn: 'cloud' } }))).toBe('default');
  });

  it('goes back to a step whose outcome is gone, whatever the record says', () => {
    expect(nextSetupStep(facts({ answers: { models: 'claude_code', runsOn: 'own_computer' }, lastStep: 'model' }))).toBe('computer');
  });

  it('is done once the default step was finished', () => {
    expect(nextSetupStep(facts({ answers: { models: 'claude_code', runsOn: 'cloud' }, lastStep: 'default' }))).toBe('done');
    expect(nextSetupPath('done')).toBe('/welcome');
    expect(nextSetupPath('model')).toBe('/setup/model');
  });
});

describe('moving along the rail', () => {
  const rail = ['computer', 'model', 'default'] as const;
  it('names the steps before and after', () => {
    expect(previousSetupStep(rail, 'computer')).toBeNull();
    expect(previousSetupStep(rail, 'default')).toBe('model');
    expect(followingSetupStep(rail, 'computer')).toBe('model');
    expect(followingSetupStep(rail, 'default')).toBeNull();
  });
});
