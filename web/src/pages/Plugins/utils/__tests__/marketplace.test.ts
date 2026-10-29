// @vitest-environment node
import { describe, it, expect } from 'vitest';
import type {
  MarketplaceEntry,
  MarketplaceTerm,
} from '@/pages/ChatAgent/utils/api/marketplace';
import { coverageLine, groupByCategory, matchesQuery, usedRegionTerms } from '../marketplace';

const term = (key: string, label: string, label_zh: string | null = null): MarketplaceTerm => ({
  key,
  label,
  label_zh,
});

const vocabulary = {
  assets: [
    term('equity', 'Equities', '股票'),
    term('fund', 'Funds', '基金'),
    term('bond', 'Bonds', '债券'),
    term('fx', 'FX', '外汇'),
    term('crypto', 'Crypto', '加密货币'),
  ],
  regions: [
    term('us', 'United States', '美国'),
    term('cn', 'Mainland China', '中国内地'),
    term('hk', 'Hong Kong', '中国香港'),
    term('global', 'Global', '全球'),
  ],
};

function entry(overrides: Partial<MarketplaceEntry> = {}): MarketplaceEntry {
  return {
    name: 'vendor',
    description: '',
    category: 'data',
    license: null,
    publisher: null,
    homepage: null,
    icon_url: null,
    keywords: [],
    auth: null,
    requires: null,
    coverage: null,
    source_url: 'https://github.com/acme/catalog/tree/main/plugins/vendor',
    installed: false,
    plugin: null,
    update_available: false,
    ...overrides,
  };
}

describe('matchesQuery', () => {
  const entries = [
    entry({ name: 'wind', publisher: { name: 'Wind Information', url: null, official: true } }),
    entry({ name: 'coingecko', keywords: ['crypto', 'defi'] }),
    entry({ name: 'dcf-model', description: 'Discounted cash flow builder' }),
  ];
  const names = (q: string) => entries.filter((e) => matchesQuery(e, q)).map((e) => e.name);

  it('matches name, description, publisher and keywords, ignoring case', () => {
    expect(names('WIND')).toEqual(['wind']);
    expect(names('information')).toEqual(['wind']);
    expect(names('defi')).toEqual(['coingecko']);
    expect(names('cash flow')).toEqual(['dcf-model']);
    expect(names('  ')).toEqual(['wind', 'coingecko', 'dcf-model']);
  });
});

describe('chip terms', () => {
  const entries = [
    entry({ coverage: { markets: { equity: ['cn', 'hk'], fund: ['cn'] }, data: [] } }),
    entry({ coverage: { markets: { equity: ['us', 'mx'] }, data: [] } }),
    entry({ coverage: null }),
  ];

  it('offers only used regions, in vocabulary order, with unknown keys last', () => {
    const regions = usedRegionTerms(entries, vocabulary.regions);
    expect(regions.map((t) => t.key)).toEqual(['us', 'cn', 'hk', 'mx']);
    expect(regions[3].label).toBe('MX');
  });
});

describe('coverageLine', () => {
  it('reads asset labels and region codes, in vocabulary order, dropping a lone global', () => {
    const coverage = {
      markets: { fx: ['global'], equity: ['us', 'hk', 'cn'], fund: ['cn'] },
      data: [],
    };
    expect(coverageLine(coverage, vocabulary, 'en-US')).toBe(
      'Equities US · CN · HK  ·  Funds CN  ·  FX',
    );
  });

  it('labels global beside named markets rather than spelling it as a code', () => {
    const coverage = { markets: { equity: ['global', 'us'] }, data: [] };
    expect(coverageLine(coverage, vocabulary, 'en-US')).toBe('Equities US · Global');
    expect(coverageLine(coverage, vocabulary, 'zh-CN')).toBe('股票 US · 全球');
  });

  it('reads zh asset labels in zh-CN and falls back to the label without one', () => {
    const zhVocab = { ...vocabulary, assets: [term('equity', 'Equities'), ...vocabulary.assets.slice(1)] };
    expect(coverageLine({ markets: { fund: ['cn'] }, data: [] }, zhVocab, 'zh-CN')).toBe('基金 CN');
    expect(coverageLine({ markets: { equity: ['cn'] }, data: [] }, zhVocab, 'zh-CN')).toBe(
      'Equities CN',
    );
  });

  it('uppercases a key the vocabulary lacks, and is empty without coverage', () => {
    expect(coverageLine({ markets: { warrant: ['kr'] }, data: [] }, vocabulary, 'en-US')).toBe(
      'WARRANT KR',
    );
    expect(coverageLine(null, vocabulary, 'en-US')).toBe('');
  });
});

describe('groupByCategory', () => {
  it('stacks known categories in order and folds the rest into Other', () => {
    const entries = [
      entry({ name: 'a', category: 'trading' }),
      entry({ name: 'b', category: null }),
      entry({ name: 'c', category: 'research' }),
      entry({ name: 'd', category: 'data' }),
      entry({ name: 'e', category: 'esoteric' }),
      entry({ name: 'f', category: 'data' }),
    ];
    expect(
      groupByCategory(entries).map(([c, rows]) => [c, rows.map((r) => r.name)]),
    ).toEqual([
      ['data', ['d', 'f']],
      ['research', ['c']],
      ['trading', ['a']],
      ['other', ['b', 'e']],
    ]);
  });
});
