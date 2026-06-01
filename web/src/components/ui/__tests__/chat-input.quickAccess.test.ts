import { describe, it, expect } from 'vitest';
import { deriveQuickAccessModels, areModelsCompatible, type QuickAccessParams, type ModelMetadata } from '../chat-input.models';

const META: ModelMetadata = {
  'claude-opus': { sdk: 'anthropic', provider: 'anthropic' },
  'claude-sonnet': { sdk: 'anthropic', provider: 'anthropic' },
  'gpt-5': { sdk: 'openai', provider: 'openai' },
  'gpt-5-azure': { sdk: 'openai', provider: 'azure' },
  'codex-openai': { sdk: 'codex', provider: 'openai' },
  'codex-openai-2': { sdk: 'codex', provider: 'openai' },
  'codex-azure': { sdk: 'codex', provider: 'azure' },
};

function params(overrides: Partial<QuickAccessParams> = {}): QuickAccessParams {
  return {
    preferredModel: null,
    preferredFlashModel: null,
    starredModels: [],
    validModelNames: new Set(),
    initialModel: null,
    selectedModel: null,
    modelMetadata: META,
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

  it('mid-thread, drops models from a different SDK than the thread model', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'gpt-5', // openai → incompatible with anthropic thread
        starredModels: ['claude-sonnet'], // anthropic → compatible
        initialModel: 'claude-opus',
        selectedModel: 'claude-opus',
      })),
    ).toEqual(['claude-sonnet']);
  });

  it('mid-thread, drops same-SDK openai models from a different provider', () => {
    expect(
      deriveQuickAccessModels(params({
        starredModels: ['gpt-5', 'gpt-5-azure'],
        initialModel: 'gpt-5',
        selectedModel: 'gpt-5',
      })),
    ).toEqual(['gpt-5']);
  });

  it('in a fresh thread (no initialModel), keeps incompatible models', () => {
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'gpt-5',
        starredModels: ['claude-opus'],
        initialModel: null,
        selectedModel: 'claude-opus',
      })),
    ).toEqual(['gpt-5', 'claude-opus']);
  });

  it('anchors the compatibility filter on selectedModel, not initialModel', () => {
    // initialModel is anthropic but the user switched selectedModel to gpt-5.
    // The gpt-5 star (compatible with the current selection) survives; the
    // anthropic star (compatible with initialModel, not the selection) drops.
    expect(
      deriveQuickAccessModels(params({
        starredModels: ['gpt-5', 'claude-sonnet'],
        initialModel: 'claude-opus',
        selectedModel: 'gpt-5',
      })),
    ).toEqual(['gpt-5']);
  });

  it('mid-thread with a null selectedModel, keeps everything (null anchor is compatible)', () => {
    expect(
      deriveQuickAccessModels(params({
        starredModels: ['gpt-5'], // incompatible with the anthropic thread
        initialModel: 'claude-opus',
        selectedModel: null,
      })),
    ).toEqual(['gpt-5']);
  });

  it('applies the availability and compatibility gates together', () => {
    // revoked-anthropic dropped by availability; gpt-5 dropped by SDK mismatch.
    expect(
      deriveQuickAccessModels(params({
        preferredModel: 'claude-opus',
        starredModels: ['claude-sonnet', 'gpt-5', 'revoked-anthropic'],
        validModelNames: new Set(['claude-opus', 'claude-sonnet', 'gpt-5']),
        initialModel: 'claude-opus',
        selectedModel: 'claude-opus',
      })),
    ).toEqual(['claude-opus', 'claude-sonnet']);
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

describe('areModelsCompatible', () => {
  it('treats a null model as compatible', () => {
    expect(areModelsCompatible(null, 'claude-opus', META)).toBe(true);
    expect(areModelsCompatible('claude-opus', null, META)).toBe(true);
  });

  it('allows unknown models (missing metadata), either side', () => {
    expect(areModelsCompatible('claude-opus', 'mystery-model', META)).toBe(true);
    expect(areModelsCompatible('mystery-model', 'claude-opus', META)).toBe(true);
  });

  it('matches on SDK for non-openai SDKs', () => {
    expect(areModelsCompatible('claude-opus', 'claude-sonnet', META)).toBe(true);
  });

  it('rejects across different SDKs', () => {
    expect(areModelsCompatible('claude-opus', 'gpt-5', META)).toBe(false);
  });

  it('requires the same provider for openai SDKs', () => {
    expect(areModelsCompatible('gpt-5', 'gpt-5-azure', META)).toBe(false);
    expect(areModelsCompatible('gpt-5', 'gpt-5', META)).toBe(true);
  });

  it('requires the same provider for codex SDK', () => {
    expect(areModelsCompatible('codex-openai', 'codex-azure', META)).toBe(false);
    expect(areModelsCompatible('codex-openai', 'codex-openai-2', META)).toBe(true);
  });
});
