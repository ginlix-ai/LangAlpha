// @vitest-environment node
import { describe, it, expect } from 'vitest';
import {
  deriveQuickAccessModels,
  modelList,
  ownModelNames,
  type QuickAccessParams,
} from '../quickAccessModels';

function params(overrides: Partial<QuickAccessParams> = {}): QuickAccessParams {
  return {
    preferredModel: null,
    preferredFlashModel: null,
    starredModels: [],
    ownModels: [],
    hiddenModels: [],
    validModelNames: new Set(),
    ...overrides,
  };
}

describe('deriveQuickAccessModels', () => {
  it('surfaces the current primary + flash defaults even when not starred', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        preferredFlashModel: 'claude-sonnet',
        starredModels: ['gpt-5'],
      })),
    ).toEqual(['claude-opus', 'claude-sonnet', 'gpt-5']);
  });

  it('dedupes when defaults overlap each other or a star', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        preferredFlashModel: 'claude-opus',
        starredModels: ['claude-opus', 'gpt-5'],
      })),
    ).toEqual(['claude-opus', 'gpt-5']);
  });

  it('leaves no stale entry after switching a default (old default not in result)', () => {
    // User switched primary from claude-opus → claude-sonnet; claude-opus was
    // never starred, so it simply isn't passed in and never appears.
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-sonnet',
        starredModels: ['gpt-5'],
      })),
    ).toEqual(['claude-sonnet', 'gpt-5']);
  });

  it('drops models the user can no longer access once the list has loaded', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        starredModels: ['gpt-5', 'revoked-model'],
        validModelNames: new Set(['claude-opus', 'gpt-5']),
      })),
    ).toEqual(['claude-opus', 'gpt-5']);
  });

  it('skips the availability gate while the model list is still loading (empty set)', () => {
    expect(
      deriveQuickAccessModels(params({
        starredModels: ['some-model'],
        validModelNames: new Set(),
      })),
    ).toEqual(['some-model']);
  });

  it('offers models from any provider, whatever the thread already used', () => {
    // Reasoning payloads are sanitized per-provider server-side
    // (ReasoningCompatibilityMiddleware), so a mid-thread switch to a foreign
    // provider is no longer a 400 and the menu must not hide it.
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'gpt-5',
        starredModels: ['claude-sonnet', 'gemini-3'],
      })),
    ).toEqual(['gpt-5', 'claude-sonnet', 'gemini-3']);
  });

  it('applies the availability gate to defaults and stars alike', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        starredModels: ['claude-sonnet', 'gpt-5', 'revoked-model'],
        validModelNames: new Set(['claude-opus', 'claude-sonnet', 'gpt-5']),
      })),
    ).toEqual(['claude-opus', 'claude-sonnet', 'gpt-5']);
  });

  it('excludes models already shown in the primary section (no duplicate rows)', () => {
    // preferredModel is the selected/thread model, so it must not also appear
    // in the quick-access submenu.
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        preferredFlashModel: 'claude-sonnet',
        starredModels: ['gpt-5'],
        excludeModels: ['claude-opus'],
      })),
    ).toEqual(['claude-sonnet', 'gpt-5']);
  });

  it('adds the models of connected accounts and keys after the stars', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        starredModels: ['gpt-5'],
        ownModels: ['claude-opus-oauth', 'gpt-5', 'glm-custom'],
      })),
    ).toEqual(['claude-opus', 'gpt-5', 'claude-opus-oauth', 'glm-custom']);
  });

  it('keeps a removed model out even when a default, a star or an account lists it', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        starredModels: ['gpt-5'],
        ownModels: ['claude-opus-oauth', 'glm-custom'],
        hiddenModels: ['claude-opus', 'gpt-5', 'claude-opus-oauth'],
      })),
    ).toEqual(['glm-custom']);
  });

  it('returns an empty list when there are no defaults or stars', () => {
    expect(deriveQuickAccessModels(params())).toEqual([]);
  });

  it('drops non-string entries from a malformed starred_models pref', () => {
    // A corrupt pref could carry non-string truthy values; they must not reach
    // getModelDisplayName (key.startsWith) and crash the composer.
    expect(
      deriveQuickAccessModels(params({
        starredModels: [123, '', null, {}, 'gpt-5'] as unknown as string[],
      })),
    ).toEqual(['gpt-5']);
  });
});

describe('ownModelNames', () => {
  const models = {
    platform: { models: ['served'] },
    oauth: { models: ['connected'] },
    key: { models: ['keyed', 'connected'] },
  };

  it('takes the models reached through a key or a connected account', () => {
    expect(ownModelNames(models, { served: 'platform', connected: 'oauth', keyed: 'byok' })).toEqual(['connected', 'keyed']);
  });

  it('takes every listed model with no platform, since the list then only carries the user\'s own', () => {
    expect(ownModelNames(models, undefined)).toEqual(['served', 'connected', 'keyed']);
  });
});

describe('modelList', () => {
  it('keeps the model names of a stored list', () => {
    expect(modelList(['gpt-5', 'claude-opus'])).toEqual(['gpt-5', 'claude-opus']);
  });

  it('reads anything but a list of names as no list', () => {
    expect(modelList(undefined)).toEqual([]);
    expect(modelList('gpt-5')).toEqual([]);
    expect(modelList([123, '', null, 'gpt-5'])).toEqual(['gpt-5']);
  });
});
