// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { modelGroups } from '../modelGroups';
import type { Payer } from '../modelPayer';

function groups(payers: Record<string, Payer>, models: Record<string, { models?: string[]; display_name?: string }>) {
  return modelGroups({ models, payers });
}

describe('modelGroups', () => {
  it("splits a brand's subscription models from its keyed ones, each a tab of its own", () => {
    const [anthropic] = groups(
      { opus: 'credits', 'opus-oauth': 'subscription:claude-oauth' },
      { anthropic: { display_name: 'Anthropic', models: ['opus', 'opus-oauth'] } },
    );
    expect(anthropic.label).toBe('Anthropic');
    expect(anthropic.connections).toEqual([
      { id: 'subscription:claude-oauth', models: ['opus-oauth'], payer: 'subscription:claude-oauth' },
      { id: 'credits', models: ['opus'], payer: 'credits' },
    ]);
  });

  // Within one catalog group, a variant the user holds a key for and one the
  // plan serves are billed differently, so they are two tabs, not one list of
  // mixed tags.
  it('splits keyed models by who pays', () => {
    const [zai] = groups({ glm: 'credits', 'glm-cn': 'own_key' }, { 'z-ai': { models: ['glm', 'glm-cn'] } });
    expect(zai.connections.map((c) => [c.id, c.models])).toEqual([['own_key', ['glm-cn']], ['credits', ['glm']]]);
  });

  it('puts the brands the user pays for themselves first, keeping catalog order otherwise', () => {
    const order = groups(
      { opus: 'credits', glm: 'credits', grok: 'own_key', 'opus-oauth': 'subscription:claude-oauth' },
      { anthropic: { models: ['opus'] }, 'z-ai': { models: ['glm'] }, xai: { models: ['grok'] }, sub: { models: ['opus-oauth'] } },
    ).map((g) => g.id);
    expect(order).toEqual(['sub', 'xai', 'anthropic', 'z-ai']);
  });

  it('keeps two sign-ins apart, though both pay as a subscription', () => {
    const [openai] = groups(
      { 'gpt-codex': 'subscription:codex-oauth', 'gpt-other': 'subscription:other-oauth' },
      { openai: { models: ['gpt-codex', 'gpt-other'] } },
    );
    expect(openai.connections.map((c) => c.models)).toEqual([['gpt-codex'], ['gpt-other']]);
  });

  it('keeps a model nothing bills in a tab with no payer, after the billed ones', () => {
    const [ollama] = groups({}, { ollama: { models: ['local'] } });
    expect(ollama.connections).toEqual([{ id: 'none', models: ['local'], payer: undefined }]);
  });

  it('skips a group with no models', () => {
    expect(groups({}, { empty: { models: [] } })).toEqual([]);
  });
});
