// @vitest-environment node
/**
 * The read contract: the new `model_preference` column wins, the legacy
 * `other_preference` still answers for a row (or a tab) the migration has not
 * reached. Mirrors the server-side fallback in `get_model_preference`.
 *
 * The write contract is the same split read backwards, and it is the half with
 * no safety net: a key sent to the wrong column still reads back today, because
 * the reader merges both, and only goes missing the day that fallback is
 * dropped.
 */
import { describe, it, expect } from 'vitest';
import {
  pruneModelReferences,
  starredModels,
  MODEL_PREF_KEYS,
  BACKGROUND_AUTO,
  backgroundDefaultChoice,
  slotDefaultModel,
  modelPrefs,
  modelProfile,
  readDefaultModelScope,
  splitPreferenceWrite,
} from '../modelPreferences';

describe('modelPrefs', () => {
  it('prefers the new column over the legacy one', () => {
    const prefs = {
      other_preference: { preferred_model: 'old', starred_models: ['a'] },
      model_preference: { preferred_model: 'new' },
    };
    expect(modelPrefs(prefs).preferred_model).toBe('new');
  });

  it('falls back to the legacy column when the new one is empty', () => {
    const prefs = { other_preference: { preferred_model: 'old' }, model_preference: {} };
    expect(modelPrefs(prefs).preferred_model).toBe('old');
  });

  it('still ignores a null in the new column, which no write can put there', () => {
    const prefs = {
      other_preference: {},
      model_preference: { preferred_model: null },
    };
    // Both merges read a null as a delete rather than storing it, so a row
    // reaches this function with the key gone; a null here could only come
    // from a hand-written row, and reading it as a value is worse than
    // ignoring it.
    expect(modelPrefs(prefs).preferred_model).toBeUndefined();
  });

  it('reads only keys that have a pre-move copy out of the legacy column', () => {
    const prefs = {
      other_preference: { profiles: { m1: { fast_mode: true } }, prompt_guidance: 'lean' },
      model_preference: {},
    };
    // Neither ever lived there. They route to the model column on a write, but
    // routing a write is not the same question as what may be found underneath.
    expect(modelPrefs(prefs)).toEqual({});
  });

  it('reads only the moved keys out of the legacy column', () => {
    const prefs = {
      other_preference: { preferred_model: 'old', starred_models: ['a'], search_provider: 'x' },
      model_preference: {},
    };
    // Other surfaces write to that column too, and their keys are theirs.
    expect(modelPrefs(prefs)).toEqual({ preferred_model: 'old' });
  });

  it('never returns null, so call sites can index the result', () => {
    expect(modelPrefs(null)).toEqual({});
  });
});

describe('modelProfile', () => {
  const prefs = {
    other_preference: { profiles: { 'model-a': { reasoning_effort: 'low' } } },
    model_preference: { profiles: { 'model-a': { reasoning_effort: 'high' } } },
  };

  it('reads one model’s overrides', () => {
    expect(modelProfile(prefs, 'model-a')).toEqual({ reasoning_effort: 'high' });
  });

  it('does not fall back to the legacy column — profiles never lived there', () => {
    expect(modelProfile({ other_preference: prefs.other_preference }, 'model-a')).toEqual({});
  });

  it('reads an unset model, a missing map and a null model as no overrides', () => {
    expect(modelProfile(prefs, 'model-b')).toEqual({});
    expect(modelProfile({ model_preference: {} }, 'model-a')).toEqual({});
    expect(modelProfile(prefs, null)).toEqual({});
  });

  it('ignores a malformed entry rather than handing back a non-object', () => {
    expect(modelProfile({ model_preference: { profiles: { 'model-a': 'high' } } }, 'model-a')).toEqual({});
    expect(modelProfile({ model_preference: { profiles: [] } }, 'model-a')).toEqual({});
  });
});

describe('splitPreferenceWrite', () => {
  it('routes model configuration to its own column and everything else to the legacy one', () => {
    expect(
      splitPreferenceWrite({
        preferred_model: 'm',
        fallback_models: ['a'],
        starred_models: ['b'],
        search_provider: 'tavily',
      }),
    ).toEqual({
      model_preference: { preferred_model: 'm', fallback_models: ['a'] },
      other_preference: { starred_models: ['b'], search_provider: 'tavily' },
    });
  });

  it('omits a column with nothing in it, since an empty object is still a write', () => {
    expect(splitPreferenceWrite({ reasoning_effort: 'high' })).toEqual({
      model_preference: { reasoning_effort: 'high' },
    });
    expect(splitPreferenceWrite({ starred_models: null })).toEqual({
      other_preference: { starred_models: null },
    });
    expect(splitPreferenceWrite({})).toEqual({});
  });

  it('carries a null through rather than dropping it, since null is what deletes a key', () => {
    expect(splitPreferenceWrite({ preferred_model: null })).toEqual({
      model_preference: { preferred_model: null },
    });
  });

  it('keeps the three model-adjacent keys that stayed in the legacy column', () => {
    for (const key of ['starred_models', 'search_provider', 'search_depth']) {
      expect(MODEL_PREF_KEYS.has(key)).toBe(false);
    }
  });
});

describe('slotDefaultModel', () => {
  const deployment = { default_model: 'deploy-default', background_model: 'deploy-background' };

  it("resolves PTC to the user's primary, then the deployment's", () => {
    expect(slotDefaultModel({ preferred_model: 'mine' }, deployment, 'default')).toBe('mine');
    expect(slotDefaultModel({}, deployment, 'default')).toBe('deploy-default');
  });

  it("resolves fast through the user's Background model and primary before the deployment's", () => {
    expect(slotDefaultModel({ preferred_background_model: 'bg-pick', preferred_model: 'mine' }, deployment, 'background')).toBe('bg-pick');
    expect(slotDefaultModel({ preferred_model: 'mine' }, deployment, 'background')).toBe('mine');
    expect(slotDefaultModel({}, deployment, 'background')).toBe('deploy-background');
    // A deployment naming no Background model reports it as "".
    expect(slotDefaultModel({}, { default_model: 'deploy-default', background_model: '' }, 'background')).toBe('deploy-default');
  });

  it('resolves fast past the primary when Background is on Auto', () => {
    expect(slotDefaultModel({ preferred_model: 'mine', background_follows: 'deployment' }, deployment, 'background')).toBe('deploy-background');
    expect(slotDefaultModel(
      { preferred_background_model: 'bg-pick', preferred_model: 'mine', background_follows: 'deployment' }, deployment, 'background',
    )).toBe('bg-pick');
  });

  it('is null with nothing saved and no deployment defaults loaded', () => {
    expect(slotDefaultModel({}, null, 'default')).toBeNull();
  });
});

describe('backgroundDefaultChoice', () => {
  it('shows a saved Background model, else Auto beside a saved primary, else the primary', () => {
    expect(backgroundDefaultChoice({ preferred_background_model: 'bg-pick', background_follows: 'deployment' })).toBe('bg-pick');
    expect(backgroundDefaultChoice({ preferred_model: 'mine', background_follows: 'deployment' })).toBe(BACKGROUND_AUTO);
    expect(backgroundDefaultChoice({ preferred_model: 'mine' })).toBe('');
    // With no primary saved, Auto and following the primary run one model.
    expect(backgroundDefaultChoice({ background_follows: 'deployment' })).toBe('');
  });
});

describe('readDefaultModelScope', () => {
  it('reads an absent or unknown scope as ask', () => {
    expect(readDefaultModelScope(null)).toBe('ask');
    expect(readDefaultModelScope({ model_preference: {} })).toBe('ask');
    expect(readDefaultModelScope({ model_preference: { default_model_scope: 'bogus' } })).toBe('ask');
  });

  it('returns a saved answer', () => {
    expect(readDefaultModelScope({ model_preference: { default_model_scope: 'existing_threads' } })).toBe('existing_threads');
    expect(readDefaultModelScope({ model_preference: { default_model_scope: 'new_threads' } })).toBe('new_threads');
  });
});

describe('starredModels', () => {
  it('reads the list from other_preference, never the model bag', () => {
    expect(starredModels({ other_preference: { starred_models: ['a', 'b'] }, model_preference: { starred_models: ['c'] } })).toEqual(['a', 'b']);
  });

  it('reads anything but a list of names as no list', () => {
    expect(starredModels(null)).toEqual([]);
    expect(starredModels({ other_preference: { starred_models: 'a' } })).toEqual([]);
    expect(starredModels({ other_preference: { starred_models: ['a', 3, ''] } })).toEqual(['a']);
  });
});

describe('pruneModelReferences', () => {
  // The models a removed key or an unpicked provider served.
  const gone = new Set(['gpt-4o', 'gpt-3.5']);
  const isGone = (m: string) => gone.has(m);

  it('is empty when nothing names a gone model', () => {
    const prefs = { model_preference: { preferred_model: 'gpt-5.4', fallback_models: ['claude-5'] } };
    expect(pruneModelReferences(prefs, isGone)).toEqual({});
  });

  it('clears every slot that names a gone model, and only those', () => {
    const prefs = {
      model_preference: {
        preferred_model: 'claude-5',
        preferred_background_model: 'gpt-4o',
        compaction_model: 'gpt-3.5',
        summarization_model: 'gpt-4o',
        fetch_model: 'gpt-3.5',
      },
    };
    expect(pruneModelReferences(prefs, isGone)).toEqual({
      preferred_background_model: null,
      compaction_model: null,
      summarization_model: null,
      fetch_model: null,
    });
  });

  it('shrinks the fallbacks and the list, clearing one left empty', () => {
    const prefs = {
      model_preference: { fallback_models: ['gpt-5.4', 'gpt-3.5', 'claude-5'] },
      other_preference: { starred_models: ['gpt-4o'] },
    };
    expect(pruneModelReferences(prefs, isGone)).toEqual({
      fallback_models: ['gpt-5.4', 'claude-5'],
      starred_models: null,
    });
  });

  it("saves emptied fallbacks as empty, so the deployment's list does not take their place", () => {
    const prefs = { model_preference: { fallback_models: ['gpt-4o', 'gpt-3.5'] } };
    expect(pruneModelReferences(prefs, isGone)).toEqual({ fallback_models: [] });
  });

  it("drops a gone model's profile, so no model inheriting its name inherits its tuning", () => {
    const prefs = { model_preference: { profiles: { 'gpt-4o': { reasoning_effort: 'high' }, 'claude-5': { fast_mode: true } } } };
    expect(pruneModelReferences(prefs, isGone)).toEqual({ profiles: { 'gpt-4o': null } });
  });

  it('reads a slot still in the legacy column', () => {
    expect(pruneModelReferences({ other_preference: { preferred_model: 'gpt-4o' } }, isGone)).toEqual({ preferred_model: null });
  });
});
