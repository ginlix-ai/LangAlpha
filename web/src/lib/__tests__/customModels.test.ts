// @vitest-environment node
import { describe, it, expect } from 'vitest';
import type { CustomModelEntry } from '@/components/model/types';
import { mergeProviderModels, replaceProviderModels, withoutProvider } from '../customModels';

/** Every provider its own connection, as for a provider of the user's own. */
const own = (provider: string) => provider;

const cm = (name: string, provider: string, extra: Partial<CustomModelEntry> = {}): CustomModelEntry => ({
  name,
  model_id: name,
  provider,
  ...extra,
});

describe('mergeProviderModels', () => {
  it('appends new entries when the slug has no prior entries', () => {
    const merged = mergeProviderModels([cm('claude-5', 'anthropic')], 'my-openrouter', [cm('gpt-5.4', 'my-openrouter'), cm('llama-4', 'my-openrouter')], own);

    expect(merged.map((m) => m.name).sort()).toEqual(['claude-5', 'gpt-5.4', 'llama-4']);
  });

  it('preserves other-slug entries untouched', () => {
    const other = cm('claude-5', 'anthropic', { input_modalities: ['text'] });
    const merged = mergeProviderModels([other, cm('old-model', 'my-openrouter')], 'my-openrouter', [cm('gpt-5.4', 'my-openrouter')], own);

    const claude = merged.find((m) => m.name === 'claude-5')!;
    expect(claude).toBe(other); // same object reference — unchanged
    expect(claude.input_modalities).toEqual(['text']);
  });

  it('preserves existing-slug entries when their name is NOT in the new batch', () => {
    const keeper = cm('user-added', 'my-openrouter', { model_id: 'custom-id-xyz' });
    const merged = mergeProviderModels([keeper], 'my-openrouter', [cm('gpt-5.4', 'my-openrouter')], own);

    expect(merged).toHaveLength(2);
    const kept = merged.find((m) => m.name === 'user-added')!;
    expect(kept.model_id).toBe('custom-id-xyz'); // not mutated
  });

  it('new batch wins when a name collides with an existing-slug entry', () => {
    const existingCollide = cm('gpt-5.4', 'my-openrouter', { model_id: 'stale-id' });
    const newEntry = cm('gpt-5.4', 'my-openrouter', { model_id: 'fresh-id', input_modalities: ['text', 'image'] });

    const merged = mergeProviderModels([existingCollide], 'my-openrouter', [newEntry], own);

    expect(merged).toHaveLength(1);
    expect(merged[0]).toBe(newEntry);
    expect(merged[0].model_id).toBe('fresh-id');
  });

  it('handles empty new batch by preserving all existing entries', () => {
    const existing = [cm('a', 'my-openrouter'), cm('b', 'my-openrouter')];
    const merged = mergeProviderModels(existing, 'my-openrouter', [], own);

    expect(merged).toEqual(existing);
  });

  it('handles empty existing by returning just the new batch', () => {
    const merged = mergeProviderModels([], 'my-openrouter', [cm('gpt-5.4', 'my-openrouter')], own);

    expect(merged).toHaveLength(1);
    expect(merged[0].name).toBe('gpt-5.4');
  });
});

describe('replaceProviderModels', () => {
  it("replaces one provider's entries and keeps every other provider's", () => {
    const other = cm('claude-5', 'anthropic');
    const next = [cm('b', 'mine')];
    expect(replaceProviderModels([other, cm('a', 'mine')], 'mine', next, own)).toEqual([other, ...next]);
  });
});

describe('withoutProvider', () => {
  it("drops only that provider's entries", () => {
    const other = cm('claude-5', 'anthropic');
    expect(withoutProvider([other, cm('a', 'mine')], 'mine', own)).toEqual([other]);
  });
});
