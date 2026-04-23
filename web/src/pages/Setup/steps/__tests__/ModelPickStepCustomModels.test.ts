/**
 * Regression: custom entries must key off ``name``, not ``model_id``.
 *
 * When the user uses the "+ Add model" form, ``name`` (display identifier)
 * and ``model_id`` (upstream id) can diverge. ``starred_models``,
 * ``preferred_model``, etc. all store the ``name``. If the ModelPickStep
 * derives ``existingCustomModels`` from ``cm.model_id``, a round-trip
 * through save + reload drops the custom row because the name in
 * ``starred_models`` no longer appears in ``allModels``.
 *
 * Extracting the filter shape lets us pin this behavior without mounting
 * the full component (same pattern as MethodStep.test.tsx).
 */
import { describe, it, expect } from 'vitest';
import { slugifyModelName } from '../slugifyModelName';

interface CustomModelEntry {
  name?: string;
  model_id: string;
  provider: string;
}

function existingCustomModelsFor(
  preferences: unknown,
  provider: string,
  brandKey: string,
): string[] {
  if (!preferences) return [];
  const prefs = preferences as Record<string, unknown>;
  const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
  const customModels = (otherPref.custom_models ?? []) as CustomModelEntry[];
  return customModels
    .filter((cm) => cm.provider === provider || cm.provider === brandKey)
    .map((cm) => cm.name ?? cm.model_id);
}

describe('ModelPickStep.existingCustomModels', () => {
  it('returns name when name and model_id differ (new form)', () => {
    const prefs = {
      other_preference: {
        custom_models: [
          { name: 'glm-6-preview', model_id: 'glm-6', provider: 'z-ai' },
        ],
      },
    };
    expect(existingCustomModelsFor(prefs, 'z-ai', 'z-ai')).toEqual(['glm-6-preview']);
  });

  it('falls back to model_id for legacy entries without name', () => {
    const prefs = {
      other_preference: {
        custom_models: [
          { model_id: 'legacy-model', provider: 'z-ai' } as CustomModelEntry,
        ],
      },
    };
    expect(existingCustomModelsFor(prefs, 'z-ai', 'z-ai')).toEqual(['legacy-model']);
  });

  it('filters by provider or brandKey', () => {
    const prefs = {
      other_preference: {
        custom_models: [
          { name: 'a', model_id: 'a', provider: 'z-ai-coding' },
          { name: 'b', model_id: 'b', provider: 'z-ai' },
          { name: 'c', model_id: 'c', provider: 'moonshot-coding' },
        ],
      },
    };
    expect(existingCustomModelsFor(prefs, 'z-ai-coding', 'z-ai').sort()).toEqual(['a', 'b']);
  });

  it('returns empty when preferences is null', () => {
    expect(existingCustomModelsFor(null, 'z-ai', 'z-ai')).toEqual([]);
  });
});

describe('slugifyModelName', () => {
  it('lowercases and replaces spaces with dashes', () => {
    expect(slugifyModelName('Kimi k2.6')).toBe('kimi-k2.6');
  });

  it('keeps allowed special characters (. _ : / -)', () => {
    expect(slugifyModelName('gpt-4o.mini_pro:latest')).toBe('gpt-4o.mini_pro:latest');
    expect(slugifyModelName('openai/gpt-4o')).toBe('openai/gpt-4o');
  });

  it('strips disallowed characters (unicode, emoji, symbols)', () => {
    expect(slugifyModelName('Claude™ Haiku!')).toBe('claude-haiku');
    expect(slugifyModelName('gpt-4o ✨')).toBe('gpt-4o-');
  });

  it('collapses leading non-alphanumeric chars', () => {
    expect(slugifyModelName('   -k2.6')).toBe('k2.6');
    expect(slugifyModelName('...foo')).toBe('foo');
  });

  it('enforces 63-char max', () => {
    const input = 'a'.repeat(100);
    expect(slugifyModelName(input)).toBe('a'.repeat(63));
  });

  it('returns empty string when input has no alphanumerics', () => {
    expect(slugifyModelName('!!!')).toBe('');
    expect(slugifyModelName('   ')).toBe('');
  });

  it('passes already-slug input through unchanged', () => {
    expect(slugifyModelName('kimi-k2.6')).toBe('kimi-k2.6');
    expect(slugifyModelName('glm-5.1')).toBe('glm-5.1');
  });
});
