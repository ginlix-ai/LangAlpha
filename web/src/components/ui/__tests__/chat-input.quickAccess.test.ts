import { describe, it, expect } from 'vitest';
import {
  derivePrimaryModels,
  deriveQuickAccessModels,
  type QuickAccessParams,
} from '../chat-input.models';

function params(overrides: Partial<QuickAccessParams> = {}): QuickAccessParams {
  return {
    preferredModel: null,
    preferredFlashModel: null,
    starredModels: [],
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

describe('derivePrimaryModels', () => {
  it('lists the thread models, then the current selection', () => {
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: ['claude-opus', 'claude-sonnet'],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus', 'claude-sonnet', 'gpt-5']);
  });

  it('dedupes a selection the thread already used', () => {
    expect(
      derivePrimaryModels({
        selectedModel: 'claude-opus',
        threadModels: ['claude-opus'],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus']);
  });

  it('drops a thread model the user can no longer reach', () => {
    // A model can be revoked (BYOK key removed, plan downgrade) long after a
    // turn used it. Leaving it clickable fails only once the user sends.
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: ['claude-opus', 'revoked-model'],
        validModelNames: new Set(['claude-opus', 'gpt-5']),
      }),
    ).toEqual(['claude-opus', 'gpt-5']);
  });

  it('keeps the current selection even when it is not in the valid set', () => {
    // The trigger renders the selection; gating it would leave the menu unable
    // to show what is currently selected.
    expect(
      derivePrimaryModels({
        selectedModel: 'not-yet-loaded',
        threadModels: ['claude-opus'],
        validModelNames: new Set(['claude-opus']),
      }),
    ).toEqual(['claude-opus', 'not-yet-loaded']);
  });

  it('skips the availability gate while the model list is still loading', () => {
    expect(
      derivePrimaryModels({
        selectedModel: null,
        threadModels: ['claude-opus', 'gpt-5'],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus', 'gpt-5']);
  });

  it('offers thread models from any provider, not just the selection\'s', () => {
    // The point of the change: history spanning providers stays reachable.
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: ['claude-opus', 'glm-5.2'],
        validModelNames: new Set(['claude-opus', 'glm-5.2', 'gpt-5']),
      }),
    ).toEqual(['claude-opus', 'glm-5.2', 'gpt-5']);
  });

  it('returns an empty list with no selection and no history', () => {
    expect(
      derivePrimaryModels({
        selectedModel: null,
        threadModels: [],
        validModelNames: new Set(),
      }),
    ).toEqual([]);
  });

  it('drops malformed thread-model entries', () => {
    expect(
      derivePrimaryModels({
        selectedModel: 'gpt-5',
        threadModels: [123, '', null, 'claude-opus'] as unknown as string[],
        validModelNames: new Set(),
      }),
    ).toEqual(['claude-opus', 'gpt-5']);
  });
});
