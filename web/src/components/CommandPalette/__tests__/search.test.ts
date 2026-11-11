// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { SearchFileHit, SearchResponse, SearchThreadHit, SearchTurnHit } from '@/api/search';
import { arrangeSearch, extendsQuery, findItems, mergeThreads, narrowAnswer, type Found, type Hit } from '../search';
import type { PaletteItem } from '../types';

const icon = () => null;
const item = (id: string, title: string, keywords?: string): PaletteItem => ({
  id,
  icon,
  title,
  keywords,
  actions: [{ id: 'open', label: 'Open', icon, effect: 'run', run: () => {} }],
});
const t = (key: string) => key;
const none: Found = { threads: [], turns: [], files: [], tickers: [], workspaces: [], skills: [], places: [], actions: [] };
const shape = (found: Partial<Found>) =>
  arrangeSearch({ ...none, ...found }, { threads: false, turns: false, files: false }, t).map((g) => [g.id, g.items.map((i) => i.id)]);

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
    ]);
  });

  it('leads with the exact ticker over a thread named after it, its kin right after', () => {
    expect(shape({ threads: [hit('thread', 0)], tickers: [hit('NVDA', -1), hit('NVDA.TO', 1)], actions: [hit('act', 2)] })).toEqual([
      ['top', ['NVDA']],
      ['threads', ['thread']],
      ['tickers', ['NVDA.TO']],
      ['actions', ['act']],
    ]);
  });

  it('puts tickers found by name after the app’s own matches', () => {
    expect(shape({ actions: [hit('dark-theme', 0)], tickers: [hit('DARK.L', 0)] })).toEqual([
      ['top', ['dark-theme']],
      ['tickers', ['DARK.L']],
    ]);
  });
});

describe('arrangeSearch, server groups', () => {
  const hit = (id: string, rank: number): Hit => ({ item: item(id, id), rank });

  it('ends a server group holding more with Show all, which opens that kind alone, and its header too', () => {
    const groups = arrangeSearch({ ...none, turns: [hit('t1', 0)] }, { threads: false, turns: true, files: false }, t);
    expect(groups.map((g) => [g.id, g.items.map((i) => i.id), g.open])).toEqual([
      ['turns', ['t1', 'more:turns'], { kind: 'turns', via: 'drill' }],
    ]);
    expect(groups[0].items[1].actions[0]).toMatchObject({ effect: 'open', view: { kind: 'turns', via: 'drill' } });
  });
});

const thread = (id: string, title: string, meta = 'server meta'): PaletteItem => ({ ...item(`thread:${id}`, title), meta });
const local = (id: string, title: string, rank = 0): Hit => ({ item: { ...item(`thread:${id}`, title), meta: 'local meta' }, rank });
const ids = (hits: Hit[]) => hits.map((hit) => hit.item.id);

describe('mergeThreads', () => {
  it('keeps the local matches where they showed and adds the server’s after them', () => {
    const { hits, more } = mergeThreads([local('a', 'margin call'), local('b', 'margins', 1)], [thread('c', 'margin notes'), thread('a', 'margin call')], ['margin'], 5);
    expect(ids(hits)).toEqual(['thread:a', 'thread:b', 'thread:c']);
    expect(more).toBe(false);
  });

  it('gives a thread both found the server’s meta, which names its automation', () => {
    const { hits } = mergeThreads([local('a', 'Morning brief')], [thread('a', 'Morning brief', 'Daily brief · today')], ['morning'], 5);
    expect(hits[0].item.meta).toBe('Daily brief · today');
  });

  it('ranks a server find by its title, so one the query leads can be the top hit', () => {
    expect(mergeThreads([], [thread('x', 'semis outlook'), thread('y', 'the semis call')], ['semis'], 5).hits.map((hit) => hit.rank)).toEqual([0, 1]);
    expect(mergeThreads([], [thread('z', 'other')], ['semis'], 5).hits[0].rank).toBe(3);
  });

  it('caps the group and says it holds more', () => {
    const { hits, more } = mergeThreads([local('l', 'l')], ['a', 'b', 'c'].map((id) => thread(id, id)), ['x'], 3);
    expect(ids(hits)).toEqual(['thread:l', 'thread:a', 'thread:b']);
    expect(more).toBe(true);
  });
});

describe('narrowAnswer', () => {
  const threadHit = (id: string, title: string): SearchThreadHit => ({
    thread_id: id,
    title,
    workspace_id: 'ws',
    workspace_name: 'Semis',
    automation_name: null,
    updated_at: '2026-11-08',
    is_pinned: false,
    has_title: true,
    match: [],
  });
  const turnHit = (snippet: string, turn_index: number): SearchTurnHit => ({
    thread_id: 't1',
    thread_title: 'NVDA Q3 preview',
    workspace_id: 'ws',
    workspace_name: 'Semis',
    turn_index,
    side: 'agent',
    snippet,
    matches: [],
    turn_at: '2026-11-07',
    is_pinned: false,
    has_title: true,
  });
  const fileHit = (path: string): SearchFileHit => ({
    workspace_id: 'ws',
    workspace_name: 'Semis',
    path,
    name: path.split('/').pop() ?? path,
    size: 1200,
    modified_at: '2026-11-07',
    match: [],
  });
  const answer: SearchResponse = {
    threads: [threadHit('a', 'NVDA margins'), threadHit('b', 'NVDA guidance')],
    turns: [turnHit('margin held', 4), turnHit('capex rose', 5)],
    files: [fileHit('results/nvda_margins.md'), fileHit('data/capex.csv')],
    next_cursor: { threads: null, turns: 'c2', files: null },
    failed: [],
  };

  it('keeps only the rows the extended query still matches, spans redrawn', () => {
    const narrowed = narrowAnswer(answer, ['nvda', 'marg']);
    expect(narrowed.threads.map((hit) => [hit.thread_id, hit.match])).toEqual([['a', [[0, 4], [5, 9]]]]);
    expect(narrowAnswer(answer, ['marg']).turns.map((hit) => [hit.turn_index, hit.matches])).toEqual([[4, [[0, 4]]]]);
    expect(narrowAnswer(answer, ['marg']).files.map((hit) => [hit.path, hit.match])).toEqual([['results/nvda_margins.md', [[13, 17]]]]);
  });
});

describe('extendsQuery', () => {
  it('holds for a query that only adds to the last, in any case', () => {
    expect(extendsQuery('nvda m', 'NVDA')).toBe(true);
    expect(extendsQuery('nvd', 'nvda')).toBe(false);
    expect(extendsQuery('nvda', '')).toBe(false);
  });
});
