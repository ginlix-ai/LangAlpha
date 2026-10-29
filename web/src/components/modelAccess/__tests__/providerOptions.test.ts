// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { ProviderCatalogEntry } from '@/components/model/types';
import { filterGroups, groupProviders, NO_FILTER, startingOption } from '../providerOptions';

const entry = (e: Partial<ProviderCatalogEntry> & { provider: string }): ProviderCatalogEntry =>
  ({ display_name: e.provider, access_type: 'api_key', ...e }) as ProviderCatalogEntry;

// The z-ai group as the catalog lists it: two coding-plan entries, and the
// API entry with China as a region of it. The plans and the keys are grouped
// apart, as the catalog hook groups them.
const catalog = [
  entry({ provider: 'z-ai-coding', group: 'z-ai', group_display_name: 'Zhipu AI', category: 'open_source', access_type: 'coding_plan', region: 'intl' }),
  entry({ provider: 'z-ai-cn-coding', group: 'z-ai', group_display_name: 'Zhipu AI', category: 'open_source', access_type: 'coding_plan', region: 'cn' }),
  entry({
    provider: 'z-ai',
    group: 'z-ai',
    group_display_name: 'Zhipu AI',
    category: 'open_source',
    region: 'intl',
    region_variants: [{ provider: 'z-ai-cn', region: 'cn', base_url: 'https://cn.example' }],
  } as Partial<ProviderCatalogEntry> & { provider: string }),
  entry({ provider: 'openai', group: 'openai', group_display_name: 'OpenAI', category: 'closed_source' }),
  entry({ provider: 'groq', group: 'groq', group_display_name: 'Groq', category: 'inference' }),
];
const keys = catalog.filter((e) => e.access_type === 'api_key');
const plans = catalog.filter((e) => e.access_type === 'coding_plan');

describe('groupProviders', () => {
  it("folds a provider's regions under it, international first", () => {
    const [zai, openai] = groupProviders(keys);
    expect(zai.name).toBe('Zhipu AI');
    expect(zai.category).toBe('open_source');
    expect(zai.markProvider).toBe('z-ai');
    expect(zai.options.map((o) => o.provider)).toEqual(['z-ai', 'z-ai-cn']);
    expect(zai.options[1]).toMatchObject({ plan: 'api_key', region: 'cn', base_url: 'https://cn.example' });
    expect(openai.options).toHaveLength(1);
    expect(groupProviders(plans)[0].options.map((o) => o.provider)).toEqual(['z-ai-coding', 'z-ai-cn-coding']);
  });

  it('keeps each entry on its own when the server sends no group', () => {
    const groups = groupProviders([entry({ provider: 'a' }), entry({ provider: 'b' })]);
    expect(groups.map((g) => g.group)).toEqual(['a', 'b']);
  });
});

describe('filterGroups', () => {
  const groups = groupProviders(keys);

  it('narrows by kind of provider', () => {
    expect(filterGroups(groups, { ...NO_FILTER, category: 'open_source' }).map((g) => g.group)).toEqual(['z-ai']);
    expect(filterGroups(groups, { ...NO_FILTER, category: 'inference' }).map((g) => g.group)).toEqual(['groq']);
    expect(filterGroups(groups, NO_FILTER)).toHaveLength(3);
  });

  it('matches the query on the name or the key', () => {
    expect(filterGroups(groups, { ...NO_FILTER, query: 'zhipu' }).map((g) => g.group)).toEqual(['z-ai']);
    expect(filterGroups(groups, { ...NO_FILTER, query: 'z-ai' }).map((g) => g.group)).toEqual(['z-ai']);
  });
});

describe('startingOption', () => {
  const [zai] = groupProviders(plans);

  it('starts on a saved key, else the first option', () => {
    expect(startingOption(zai, new Set(['z-ai-cn-coding'])).provider).toBe('z-ai-cn-coding');
    expect(startingOption(zai, new Set()).provider).toBe('z-ai-coding');
  });
});
