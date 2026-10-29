// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { pickerWrite, type PickerSnapshot } from '../pickerWrite';

const snapshot = (s: Partial<PickerSnapshot>): PickerSnapshot => ({
  provider: 'acme',
  selected: new Set(),
  owned: [],
  saved: [],
  unsaved: [],
  listed: [],
  builtIn: [],
  manifest: {},
  native: () => false,
  connected: new Set(['acme']),
  connectionOf: (p) => p,
  ...s,
});

describe('pickerWrite', () => {
  it("replaces this connection's entries and part of the list, keeping another's", () => {
    const theirs = { name: 'theirs', model_id: 'theirs', provider: 'other' };
    const stored = {
      model_preference: { custom_models: [theirs, { name: 'old', model_id: 'old', provider: 'acme' }] },
      other_preference: { starred_models: ['theirs', 'old'] },
    };
    const saved = [{ name: 'old', model_id: 'old', provider: 'acme' }];
    const fresh = { name: 'new', model_id: 'new', provider: 'acme' };
    const unpickedFresh = { name: 'skip', model_id: 'skip', provider: 'acme' };

    const patch = pickerWrite(stored, snapshot({
      selected: new Set(['new']),
      owned: ['old', 'new'],
      saved,
      unsaved: [fresh, unpickedFresh],
    }));

    expect(patch.custom_models).toEqual([theirs, fresh]);
    expect(patch.starred_models).toEqual(['theirs', 'new']);
  });

  it('refreshes a kept entry from the list and gives a variant pick an entry naming the variant', () => {
    const saved = [{ name: 'm', model_id: 'm', provider: 'acme', context: 1000 }];
    const patch = pickerWrite({}, snapshot({
      selected: new Set(['m', 'brand-model']),
      owned: ['m'],
      saved,
      listed: [{ id: 'm', context_window: 500 }],
      builtIn: ['brand-model'],
      manifest: { 'brand-model': { provider: 'brand', model_id: 'brand-id' } },
    }));

    expect(patch.custom_models).toEqual([
      { name: 'm', model_id: 'm', provider: 'acme', context: 500 },
      { name: 'brand-model', model_id: 'brand-id', provider: 'acme' },
    ]);
  });

  it('prunes a model just unpicked unless another connection still serves it', () => {
    const stored = {
      model_preference: { preferred_model: 'gone', fallback_models: ['kept'] },
      other_preference: { starred_models: ['gone', 'kept'] },
    };
    const patch = pickerWrite(stored, snapshot({
      owned: ['gone', 'kept'],
      manifest: { gone: { provider: 'acme' }, kept: { provider: 'brand' } },
      connected: new Set(['acme', 'brand']),
    }));

    expect(patch.preferred_model).toBeNull();
    expect(patch.starred_models).toEqual(['kept']);
    expect(patch).not.toHaveProperty('fallback_models');
  });

  it("drops an entry that only routed a model now native here, and keeps one that also tunes it", () => {
    const routing = { name: 'plan-m', model_id: 'plan-m', provider: 'acme' };
    const tuned = { name: 'plan-t', model_id: 'plan-t', provider: 'acme', context: 1000 };
    const patch = pickerWrite({}, snapshot({
      selected: new Set(['plan-m', 'plan-t']),
      owned: ['plan-m', 'plan-t'],
      saved: [routing, tuned],
      native: () => true,
    }));

    expect(patch.custom_models).toEqual([tuned]);
    expect(patch.starred_models).toEqual(['plan-m', 'plan-t']);
  });
});

