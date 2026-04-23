import { describe, it, expect } from 'vitest';
import { computeSlotCleanup } from '../modelSlotCleanup';

describe('computeSlotCleanup', () => {
  const allModels = ['gpt-5.4', 'gpt-4o', 'gpt-3.5']; // models visible in this step

  it('returns empty when nothing is orphaned', () => {
    const out = computeSlotCleanup({
      otherPref: {
        preferred_model: 'gpt-5.4',
        preferred_flash_model: 'gpt-4o',
      },
      allModels,
      mergedConfigured: ['gpt-5.4', 'gpt-4o'],
    });

    expect(out.nulls).toEqual({});
    expect(out.fallback_models).toBeUndefined();
  });

  it('nulls preferred_model when its model was deselected', () => {
    const out = computeSlotCleanup({
      otherPref: { preferred_model: 'gpt-3.5' },
      allModels,
      mergedConfigured: ['gpt-5.4'], // user deselected gpt-3.5
    });

    expect(out.nulls).toEqual({ preferred_model: null });
  });

  it('leaves preferred_model alone when it belongs to another provider', () => {
    const out = computeSlotCleanup({
      otherPref: { preferred_model: 'claude-5' }, // not in allModels (different provider)
      allModels,
      mergedConfigured: ['gpt-5.4'],
    });

    expect(out.nulls.preferred_model).toBeUndefined();
  });

  it('nulls every scalar slot that is orphaned', () => {
    const out = computeSlotCleanup({
      otherPref: {
        preferred_model: 'gpt-5.4',
        preferred_flash_model: 'gpt-4o',
        compaction_model: 'gpt-3.5',
        fetch_model: 'gpt-3.5',
      },
      allModels,
      mergedConfigured: [], // user deselected everything
    });

    expect(out.nulls).toEqual({
      preferred_model: null,
      preferred_flash_model: null,
      compaction_model: null,
      fetch_model: null,
    });
  });

  it('shrinks fallback_models only when something was removed', () => {
    const out = computeSlotCleanup({
      otherPref: {
        fallback_models: ['gpt-5.4', 'gpt-3.5', 'claude-5'],
      },
      allModels,
      mergedConfigured: ['gpt-5.4'], // gpt-3.5 deselected, claude-5 is other-provider
    });

    expect(out.fallback_models).toEqual(['gpt-5.4', 'claude-5']);
  });

  it('does not touch fallback_models when nothing in this provider was orphaned', () => {
    const out = computeSlotCleanup({
      otherPref: { fallback_models: ['gpt-5.4', 'claude-5'] },
      allModels,
      mergedConfigured: ['gpt-5.4'], // claude-5 is from another provider — stays
    });

    expect(out.fallback_models).toBeUndefined();
  });
});
