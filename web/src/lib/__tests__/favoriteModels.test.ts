// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { favoriteModels, modelList, ownModelNames } from '../favoriteModels';

describe('favoriteModels', () => {
  describe('before the first edit', () => {
    it('seeds the default, the stars, then the own models, in that order and once each', () => {
      expect(favoriteModels({
        other: { starred_models: ['b', 'a'] },
        preferredModel: 'a',
        ownModels: ['c', 'b'],
      })).toEqual(['a', 'b', 'c']);
    });

    it('leaves out what the user removed, whichever rule lists it', () => {
      expect(favoriteModels({
        other: { starred_models: ['b'], hidden_quick_access_models: ['a', 'b', 'c'] },
        preferredModel: 'a',
        ownModels: ['c', 'd'],
      })).toEqual(['d']);
    });

    it('drops malformed entries rather than reaching a label lookup', () => {
      expect(favoriteModels({
        other: { starred_models: ['a', 7, null, ''] },
        preferredModel: undefined,
        ownModels: [],
      })).toEqual(['a']);
    });
  });

  describe('after the first edit', () => {
    // A newly connected account or a changed default adds nothing on its own.
    it('is the stars alone', () => {
      expect(favoriteModels({
        other: { starred_models: ['b'], favorites_starred_only: true, hidden_quick_access_models: ['b'] },
        preferredModel: 'a',
        ownModels: ['c'],
      })).toEqual(['b']);
    });

    it('is empty when every star was taken off', () => {
      expect(favoriteModels({
        other: { starred_models: [], favorites_starred_only: true },
        preferredModel: 'a',
        ownModels: ['c'],
      })).toEqual([]);
    });
  });
});

describe('ownModelNames', () => {
  const models = {
    platform: { models: ['served'] },
    oauth: { models: ['connected'] },
    key: { models: ['keyed', 'connected'] },
  };

  it('takes the models reached through a key or a connected account', () => {
    expect(ownModelNames(models, { served: 'credits', connected: 'subscription:claude-oauth', keyed: 'own_key' })).toEqual(['connected', 'keyed']);
  });

  it("takes a self-hosted deployment's own keys and local servers, which credits do not pay for", () => {
    expect(ownModelNames(models, { served: 'server_key', connected: 'subscription:claude-oauth' })).toEqual(['served', 'connected', 'keyed']);
  });

  // Listed, but a turn on it is refused until its key is back.
  it('leaves out a model the user added that no key of theirs serves now', () => {
    expect(ownModelNames(models, { served: 'server_key' }, new Set(['keyed']))).toEqual(['served', 'connected']);
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
