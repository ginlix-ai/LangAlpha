// @vitest-environment node
import { describe, expect, it, vi } from 'vitest';
import type { SearchFileHit, SearchThreadHit, SearchTurnHit } from '@/api/search';
import type { Thread } from '@/types/api';
import { askRow, fileRow, threadOfHit, threadOfList, threadRow, tickerRow, turnRow, type Verbs } from '../rows';
import type { PaletteAction } from '../types';

const verbs = (extra: Partial<Verbs> = {}): Verbs => ({
  t: (key, options) => (options ? `${key}:${JSON.stringify(options)}` : key),
  workspaceName: (id, given = '') => (id === 'home' ? 'Chief of Staff' : given || `ws ${id}`),
  isHome: (id) => id === 'home',
  when: (iso) => (iso ? `at ${iso}` : ''),
  newWork: { workspaceId: 'ws', pending: false, canStartThread: true, where: 'Analyst in Semis' },
  openThread: vi.fn(),
  newThread: vi.fn(),
  openFile: vi.fn(),
  openChart: vi.fn(),
  copy: vi.fn(),
  openInNewTab: vi.fn(),
  ...extra,
});
const ids = (actions: PaletteAction[]) => actions.map((action) => action.id);
const run = (action: PaletteAction) => (action.effect === 'run' ? action.run() : undefined);

const threadHit = (extra: Partial<SearchThreadHit> = {}): SearchThreadHit => ({
  thread_id: 'a',
  title: 'NVDA margins',
  workspace_id: 'ws',
  workspace_name: 'Semis',
  automation_name: null,
  updated_at: '2026-11-08',
  match: [[5, 12]],
  ...extra,
});
const turnHit = (extra: Partial<SearchTurnHit> = {}): SearchTurnHit => ({
  thread_id: 't1',
  thread_title: 'NVDA Q3 preview',
  workspace_id: 'ws',
  workspace_name: 'Semis',
  turn_index: 4,
  side: 'agent',
  snippet: 'gross margin held at 75%',
  matches: [[6, 12]],
  turn_at: '2026-11-07',
  ...extra,
});
const fileHit = (path: string, match: SearchFileHit['match'] = []): SearchFileHit => ({
  workspace_id: 'ws',
  workspace_name: 'Semis',
  path,
  name: path.split('/').pop() ?? path,
  size: 1200,
  modified_at: '2026-11-07',
  match,
});

describe('threadRow', () => {
  it('builds the same row from the server’s hit, bolding its spans, as from the app’s list', () => {
    const v = verbs();
    const fromHit = threadRow(threadOfHit(threadHit(), v), v);
    expect(fromHit).toMatchObject({
      id: 'thread:a',
      title: 'NVDA margins',
      spans: [[5, 12]],
      meta: 'Semis · at 2026-11-08',
      preview: { kind: 'thread', threadId: 'a' },
      place: { kind: 'thread', threadId: 'a', workspaceId: 'ws', title: 'NVDA margins' },
    });
    const thread = { thread_id: 'a', title: 'NVDA margins', workspace_id: 'ws', updated_at: '2026-11-08' } as Thread;
    const fromList = threadRow(threadOfList(thread, v), v);
    expect(fromList.id).toBe(fromHit.id);
    expect(fromList.meta).toBe('ws ws · at 2026-11-08');
    expect(ids(fromList.actions)).toEqual(['open', 'new-tab', 'copy-link']);
  });

  it('names an automation’s thread by its automation, and a thread with no title as untitled', () => {
    const v = verbs();
    expect(threadRow(threadOfHit(threadHit({ automation_name: 'Daily brief' }), v), v).meta).toBe('Daily brief · at 2026-11-08');
    expect(threadRow(threadOfHit(threadHit({ title: '' }), v), v)).toMatchObject({ title: 'palette.untitledThread', spans: undefined });
  });

  it('offers no new tab in the desktop shell', () => {
    const v = verbs({ openInNewTab: null });
    expect(ids(threadRow(threadOfHit(threadHit(), v), v).actions)).toEqual(['open', 'copy-link']);
  });
});

describe('turnRow', () => {
  it('builds a two-line row: the thread, then who said the matched words; Tab searches that thread', () => {
    const v = verbs();
    const row = turnRow(turnHit(), v);
    expect(row).toMatchObject({
      id: 'turn:t1:4:agent',
      title: 'NVDA Q3 preview',
      meta: 'Semis · at 2026-11-07',
      detail: { label: 'agents.analyst', text: 'gross margin held at 75%', spans: [[6, 12]] },
      preview: { kind: 'turn', threadId: 't1', turnIndex: 4, side: 'agent' },
      place: { kind: 'thread', threadId: 't1' },
    });
    expect(ids(row.actions)).toEqual(['open-at-turn', 'open-thread', 'new-tab', 'copy-link']);
    run(row.actions[0]);
    expect(v.openThread).toHaveBeenCalledWith('t1', 'ws', { focusTurn: 4 });
  });

  it('leads with the matched words in its own thread’s search, who said them after', () => {
    const row = turnRow(turnHit({ side: 'user' }), verbs(), { inThread: true });
    expect(row).toMatchObject({ title: 'gross margin held at 75%', spans: [[6, 12]], meta: 'palette.you · at 2026-11-07' });
    expect(row.place).toBeUndefined();
    expect(row.detail).toBeUndefined();
  });

  it('names the Chief of Staff in Home, and keeps the same turn matched on both sides as two rows', () => {
    const home = turnRow(turnHit({ workspace_id: 'home', workspace_name: 'Home' }), verbs());
    expect(home.detail?.label).toBe('agents.chiefOfStaff');
    expect(home.meta).toBe('Chief of Staff · at 2026-11-07');
    expect(turnRow(turnHit({ side: 'user' }), verbs()).id).not.toBe(turnRow(turnHit(), verbs()).id);
  });
});

describe('fileRow', () => {
  it('bolds the name where the path matched it, and names its folder and workspace', () => {
    // "res" in the folder, "marg" in the name, and a span that crosses the last slash.
    const row = fileRow(fileHit('results/nvda_margins.md', [[0, 3], [6, 9], [13, 17]]), verbs());
    expect(row).toMatchObject({ id: 'file:ws:results/nvda_margins.md', title: 'nvda_margins.md', meta: 'results · Semis' });
    expect(row.spans).toEqual([[0, 1], [5, 9]]);
    expect(ids(row.actions)).toEqual(['open', 'new-tab', 'copy-path']);
  });

  it('names only the workspace for a file at the folder root, unbolded when only the folder matched', () => {
    expect(fileRow(fileHit('notes.md'), verbs())).toMatchObject({ title: 'notes.md', meta: 'Semis', spans: undefined });
    expect(fileRow(fileHit('results/a.md', [[0, 7]]), verbs()).spans).toBeUndefined();
  });
});

describe('tickerRow', () => {
  it('opens the ticker’s own view as a view, not a run, and offers it nowhere in that view', () => {
    const row = tickerRow('NVDA', 'NVIDIA', 'NASDAQ', verbs());
    expect(ids(row.actions)).toEqual(['chart', 'new-thread', 'mentioning', 'copy-symbol']);
    expect(row.actions[2]).toMatchObject({ effect: 'open', view: { kind: 'ticker', symbol: 'NVDA', via: 'drill' } });
    expect(ids(tickerRow('NVDA', '', undefined, verbs(), { inView: true }).actions)).toEqual(['chart', 'new-thread', 'copy-symbol']);
  });

  it('starts no thread where none can be started', () => {
    const v = verbs({ newWork: { workspaceId: undefined, pending: false, canStartThread: false, where: undefined } });
    expect(ids(tickerRow('NVDA', '', undefined, v).actions)).toEqual(['chart', 'mentioning', 'copy-symbol']);
  });
});

describe('askRow', () => {
  it('asks nothing with no words', () => {
    expect(askRow(null, '  ', verbs())).toBeNull();
  });

  it('hands the words to where new work goes, waiting on that place', () => {
    const v = verbs();
    const row = askRow(null, ' margins ', v);
    expect(row).toMatchObject({ id: 'ask', accent: true, meta: 'Analyst in Semis' });
    expect(row?.actions[0]).toMatchObject({ effect: 'run', startsThread: true });
    run(row!.actions[0]);
    expect(v.newThread).toHaveBeenCalledWith(undefined, { message: 'margins' });
  });

  it('in a place, drafts to its agent unsent: a new thread in the workspace, or the thread’s own composer', () => {
    const v = verbs();
    const inWorkspace = askRow({ kind: 'workspace', workspaceId: 'ws', name: 'Semis' }, 'capex', v);
    expect(inWorkspace?.meta).toBe('palette.analystIn:{"workspace":"Semis"}');
    run(inWorkspace!.actions[0]);
    expect(v.newThread).toHaveBeenCalledWith('ws', { message: 'capex' });

    const inThread = askRow({ kind: 'thread', threadId: 't1', workspaceId: 'home', title: 'Brief' }, 'again', v);
    expect(inThread?.meta).toBe('palette.inThisThread:{"agent":"agents.chiefOfStaff"}');
    expect(inThread?.actions[0]).not.toHaveProperty('startsThread');
    run(inThread!.actions[0]);
    expect(v.openThread).toHaveBeenCalledWith('t1', 'home', { draftMessage: 'again' });
  });
});
