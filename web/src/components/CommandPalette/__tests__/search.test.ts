// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { arrangeSearch, findItems, type Found, type Hit } from '../search';
import type { PaletteItem } from '../types';

const icon = () => null;
const item = (id: string, title: string, keywords?: string): PaletteItem => ({
  id,
  icon,
  title,
  keywords,
  actions: [{ label: 'Open', icon, run: () => {} }],
});
const t = (key: string) => key;
const ask = item('ask', 'Ask');
const none: Found = { threads: [], tickers: [], workspaces: [], skills: [], places: [], actions: [] };
const shape = (found: Partial<Found>) =>
  arrangeSearch({ ...none, ...found }, ask, t).map((g) => [g.id, g.items.map((i) => i.id)]);

describe('findItems', () => {
  it('ranks a leading match first and bolds the words in the title', () => {
    const hits = findItems([item('a', 'dcf-model'), item('b', 'Build a DCF'), item('c', 'xlsx', 'dcf sheets')], ['dcf'], 5);
    expect(hits.map((h) => [h.item.id, h.rank])).toEqual([['a', 0], ['b', 1], ['c', 3]]);
    expect(hits[0].item.spans).toEqual([[0, 3]]);
  });

  it('matches a query under three letters only at word starts', () => {
    expect(findItems([item('a', 'conventions'), item('b', 'NVDA notes')], ['nv'], 5).map((h) => h.item.id)).toEqual(['b']);
  });

  it('matches a two-character Chinese word anywhere, keywords included', () => {
    const items = [item('a', '英伟达财报分析'), item('b', '切换到深色主题', 'dark theme'), item('c', '新建对话', '财报')];
    expect(findItems(items, ['财报'], 5).map((h) => h.item.id)).toEqual(['a', 'c']);
    expect(findItems(items, ['深色'], 5).map((h) => h.item.id)).toEqual(['b']);
  });
});

describe('arrangeSearch', () => {
  const hit = (id: string, rank: number): Hit => ({ item: item(id, id), rank });

  it('leads with an app item named by the query over the exact ticker', () => {
    expect(shape({ skills: [hit('skill', 0)], tickers: [hit('DCF', -1)] })).toEqual([
      ['top', ['skill']],
      ['tickers', ['DCF']],
      ['ask', ['ask']],
    ]);
  });

  it('leads with the exact ticker over a thread named after it, its kin right after', () => {
    expect(shape({ threads: [hit('thread', 0)], tickers: [hit('NVDA', -1), hit('NVDA.TO', 1)], actions: [hit('act', 2)] })).toEqual([
      ['top', ['NVDA']],
      ['threads', ['thread']],
      ['tickers', ['NVDA.TO']],
      ['actions', ['act']],
      ['ask', ['ask']],
    ]);
  });

  it('puts tickers found by name after the app’s own matches', () => {
    expect(shape({ actions: [hit('dark-theme', 0)], tickers: [hit('DARK.L', 0)] })).toEqual([
      ['top', ['dark-theme']],
      ['tickers', ['DARK.L']],
      ['ask', ['ask']],
    ]);
  });
});
