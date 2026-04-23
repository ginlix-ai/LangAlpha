import { describe, it, expect } from 'vitest';
import { mergeCustomModelsForSlug, type CustomModelEntry } from '../mergeCustomModelsForSlug';

const cm = (name: string, provider: string, extra: Partial<CustomModelEntry> = {}): CustomModelEntry => ({
  name,
  model_id: name,
  provider,
  ...extra,
});

describe('mergeCustomModelsForSlug', () => {
  it('appends new entries when the slug has no prior entries', () => {
    const merged = mergeCustomModelsForSlug({
      existing: [cm('claude-5', 'anthropic')],
      slug: 'my-openrouter',
      newForSlug: [cm('gpt-5.4', 'my-openrouter'), cm('llama-4', 'my-openrouter')],
    });

    expect(merged.map((m) => m.name).sort()).toEqual(['claude-5', 'gpt-5.4', 'llama-4']);
  });

  it('preserves other-slug entries untouched', () => {
    const other = cm('claude-5', 'anthropic', { input_modalities: ['text'] });
    const merged = mergeCustomModelsForSlug({
      existing: [other, cm('old-model', 'my-openrouter')],
      slug: 'my-openrouter',
      newForSlug: [cm('gpt-5.4', 'my-openrouter')],
    });

    const claude = merged.find((m) => m.name === 'claude-5')!;
    expect(claude).toBe(other); // same object reference — unchanged
    expect(claude.input_modalities).toEqual(['text']);
  });

  it('preserves existing-slug entries when their name is NOT in the new batch', () => {
    const keeper = cm('user-added', 'my-openrouter', { model_id: 'custom-id-xyz' });
    const merged = mergeCustomModelsForSlug({
      existing: [keeper],
      slug: 'my-openrouter',
      newForSlug: [cm('gpt-5.4', 'my-openrouter')],
    });

    expect(merged).toHaveLength(2);
    const kept = merged.find((m) => m.name === 'user-added')!;
    expect(kept.model_id).toBe('custom-id-xyz'); // not mutated
  });

  it('new batch wins when a name collides with an existing-slug entry', () => {
    const existingCollide = cm('gpt-5.4', 'my-openrouter', { model_id: 'stale-id' });
    const newEntry = cm('gpt-5.4', 'my-openrouter', { model_id: 'fresh-id', input_modalities: ['text', 'image'] });

    const merged = mergeCustomModelsForSlug({
      existing: [existingCollide],
      slug: 'my-openrouter',
      newForSlug: [newEntry],
    });

    expect(merged).toHaveLength(1);
    expect(merged[0]).toBe(newEntry);
    expect(merged[0].model_id).toBe('fresh-id');
  });

  it('handles empty new batch by preserving all existing entries', () => {
    const existing = [cm('a', 'my-openrouter'), cm('b', 'my-openrouter')];
    const merged = mergeCustomModelsForSlug({
      existing,
      slug: 'my-openrouter',
      newForSlug: [],
    });

    expect(merged).toEqual(existing);
  });

  it('handles empty existing by returning just the new batch', () => {
    const merged = mergeCustomModelsForSlug({
      existing: [],
      slug: 'my-openrouter',
      newForSlug: [cm('gpt-5.4', 'my-openrouter')],
    });

    expect(merged).toHaveLength(1);
    expect(merged[0].name).toBe('gpt-5.4');
  });
});
