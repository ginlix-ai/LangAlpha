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
  MODEL_PREF_KEYS,
  FLASH_AUTO,
  flashDefaultChoice,
  modeDefaultModel,
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

describe('modeDefaultModel', () => {
  const deployment = { default_model: 'deploy-default', flash_model: 'deploy-flash' };

  it("resolves PTC to the user's primary, then the deployment's", () => {
    expect(modeDefaultModel({ preferred_model: 'mine' }, deployment, 'ptc')).toBe('mine');
    expect(modeDefaultModel({}, deployment, 'ptc')).toBe('deploy-default');
  });

  it("resolves fast through the user's flash and primary before the deployment's", () => {
    expect(modeDefaultModel({ preferred_flash_model: 'flash', preferred_model: 'mine' }, deployment, 'fast')).toBe('flash');
    expect(modeDefaultModel({ preferred_model: 'mine' }, deployment, 'fast')).toBe('mine');
    expect(modeDefaultModel({}, deployment, 'fast')).toBe('deploy-flash');
    // A deployment naming no flash model reports it as "".
    expect(modeDefaultModel({}, { default_model: 'deploy-default', flash_model: '' }, 'fast')).toBe('deploy-default');
  });

  it('resolves fast past the primary when flash is on Auto', () => {
    expect(modeDefaultModel({ preferred_model: 'mine', flash_follows: 'deployment' }, deployment, 'fast')).toBe('deploy-flash');
    expect(modeDefaultModel(
      { preferred_flash_model: 'flash', preferred_model: 'mine', flash_follows: 'deployment' }, deployment, 'fast',
    )).toBe('flash');
  });

  it('is null with nothing saved and no deployment defaults loaded', () => {
    expect(modeDefaultModel({}, null, 'ptc')).toBeNull();
  });
});

describe('flashDefaultChoice', () => {
  it('shows a saved flash model, else Auto beside a saved primary, else the primary', () => {
    expect(flashDefaultChoice({ preferred_flash_model: 'flash', flash_follows: 'deployment' })).toBe('flash');
    expect(flashDefaultChoice({ preferred_model: 'mine', flash_follows: 'deployment' })).toBe(FLASH_AUTO);
    expect(flashDefaultChoice({ preferred_model: 'mine' })).toBe('');
    // With no primary saved, Auto and following the primary run one model.
    expect(flashDefaultChoice({ flash_follows: 'deployment' })).toBe('');
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
