import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createTestQueryClient } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { getWorkspaceThreads, updateThread } from '@/pages/ChatAgent/utils/api';
import { runAuthResets } from '@/lib/authResets';
import { isThreadRemoved } from '@/lib/deletedThreads';
import { archiveThreadRow, pinThreadRow, setThreadArchived } from '@/pages/ChatAgent/hooks/threadRowMutations';
import { getNavPrefs } from '@/pages/ChatAgent/utils/navPrefs';

// The palette and the sidebar pin and archive through these: every cached
// list holding the thread changes at once, and all of it comes back when the
// server refuses.
vi.mock('@/pages/ChatAgent/utils/api', () => ({ updateThread: vi.fn(), updateThreadTitle: vi.fn(), getThread: vi.fn(), getWorkspaceThreads: vi.fn() }));
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

const row = (id: string, extra: Record<string, unknown> = {}) => ({ thread_id: id, title: `Thread ${id}`, workspace_id: 'ws1', ...extra });
const keys = {
  page: queryKeys.threads.page('ws1', 20),
  gallery: queryKeys.threads.gallery('ws1', false),
  archived: queryKeys.threads.gallery('ws1', true),
  recent: queryKeys.threads.recent(50),
  search: queryKeys.threads.search({ q: 'nvda', limit: 6 }),
  // A paged answer, listing the thread's turns only.
  turns: queryKeys.threads.search({ q: 'margin', kinds: ['turns'], limit: 20, paged: true }),
};
const turn = (id: string, index: number) => ({ thread_id: id, turn_index: index, side: 'agent', is_pinned: false });
const turnsOf = (data: unknown) => {
  const entry = data as { turns?: { thread_id: string; is_pinned: boolean }[]; pages?: { turns: { thread_id: string; is_pinned: boolean }[] }[] };
  return entry.pages ? entry.pages.flatMap((page) => page.turns) : (entry.turns ?? []);
};

function seeded() {
  const client = createTestQueryClient();
  client.setQueryData(keys.page, { threads: [row('t1'), row('t2')], total: 2 });
  client.setQueryData(keys.gallery, { pages: [{ threads: [row('t1'), row('t2')], total: 2 }], pageParams: [0] });
  client.setQueryData(keys.archived, { pages: [{ threads: [row('t0')], total: 1 }], pageParams: [0] });
  client.setQueryData(keys.recent, { threads: [row('t2'), row('t1')], total: 2 });
  client.setQueryData(keys.search, { threads: [row('t1', { match: [] })], turns: [turn('t1', 2), turn('t2', 0)], files: [], next_cursor: {}, failed: [] });
  client.setQueryData(keys.turns, { pages: [{ threads: [], turns: [turn('t1', 4)], files: [], next_cursor: {}, failed: [] }], pageParams: [null] });
  return client;
}
const snapshot = (client: ReturnType<typeof seeded>) => Object.fromEntries(Object.entries(keys).map(([name, key]) => [name, client.getQueryData(key)]));
const ids = (data: unknown) => {
  const entry = data as { threads?: { thread_id: string }[]; pages?: { threads: { thread_id: string }[] }[] };
  return (entry.pages ? entry.pages.flatMap((page) => page.threads) : (entry.threads ?? [])).map((r) => r.thread_id);
};

beforeEach(() => {
  vi.clearAllMocks();
  runAuthResets(); // the archived marks, as a sign out clears them
});

describe('pinThreadRow', () => {
  it('pins the row in every list at once, and the server hears it', async () => {
    let answer: (value: unknown) => void = () => {};
    vi.mocked(updateThread).mockReturnValue(new Promise((resolve) => (answer = resolve)) as never);
    const client = seeded();
    const saving = pinThreadRow(client, 'ws1', 't1', true);

    for (const name of ['page', 'gallery', 'recent', 'search'] as const) {
      const data = client.getQueryData(keys[name]) as { threads?: { thread_id: string; is_pinned?: boolean }[]; pages?: { threads: { thread_id: string; is_pinned?: boolean }[] }[] };
      const rows = data.pages ? data.pages[0].threads : data.threads!;
      expect(rows.find((r) => r.thread_id === 't1')?.is_pinned).toBe(true);
    }
    // Its conversation matches carry the pin too, apart from the row.
    for (const key of [keys.search, keys.turns]) {
      expect(turnsOf(client.getQueryData(key)).filter((r) => r.thread_id === 't1').every((r) => r.is_pinned)).toBe(true);
    }
    expect(turnsOf(client.getQueryData(keys.search)).find((r) => r.thread_id === 't2')?.is_pinned).toBe(false);
    answer({});
    expect(await saving).toBe(true);
    expect(updateThread).toHaveBeenCalledWith('t1', { is_pinned: true });
  });

  it('puts every list back as it was when the server refuses, and reads nothing again', async () => {
    vi.mocked(updateThread).mockRejectedValue(new Error('Request failed with status code 500'));
    const client = seeded();
    const before = snapshot(client);

    expect(await pinThreadRow(client, 'ws1', 't1', true)).toBe(false);
    expect(snapshot(client)).toEqual(before);
    expect(getWorkspaceThreads).not.toHaveBeenCalled();
  });

  it("reads the workspace's threads again before it resolves, so the nav tree re-freezes from the new order", async () => {
    vi.mocked(updateThread).mockResolvedValue({} as never);
    vi.mocked(getWorkspaceThreads).mockResolvedValue({ threads: [row('t2'), row('t1')], total: 2 } as never);
    const client = seeded();
    await pinThreadRow(client, 'ws1', 't1', false);
    const pageSize = getNavPrefs().threadPageSize;
    expect(getWorkspaceThreads).toHaveBeenCalledWith('ws1', pageSize, 0);
    expect(ids(client.getQueryData(queryKeys.threads.page('ws1', pageSize)))).toEqual(['t2', 't1']);
  });
});

describe('archiveThreadRow', () => {
  it('drops the row from every list but the archived view, the recent threads and the search answers included', async () => {
    vi.mocked(updateThread).mockResolvedValue({} as never);
    const client = seeded();
    expect(await archiveThreadRow(client, 'ws1', 't1')).toBe(true);

    expect(updateThread).toHaveBeenCalledWith('t1', { archived: true });
    expect(ids(client.getQueryData(keys.page))).toEqual(['t2']);
    expect(ids(client.getQueryData(keys.gallery))).toEqual(['t2']);
    expect(ids(client.getQueryData(keys.recent))).toEqual(['t2']);
    expect(ids(client.getQueryData(keys.search))).toEqual([]);
    expect(ids(client.getQueryData(keys.archived))).toEqual(['t0']);
    expect(turnsOf(client.getQueryData(keys.search)).map((r) => r.thread_id)).toEqual(['t2']);
    expect(turnsOf(client.getQueryData(keys.turns))).toEqual([]);
    expect(isThreadRemoved('t1')).toBe(true);
    expect((client.getQueryData(keys.recent) as { total: number }).total).toBe(1);
    // Read again, so a list that comes back from the server agrees.
    expect(client.getQueryState(keys.recent)?.isInvalidated).toBe(true);
    expect(client.getQueryState(keys.search)?.isInvalidated).toBe(true);
  });

  it('puts the row back everywhere when the server refuses', async () => {
    vi.mocked(updateThread).mockRejectedValue(new Error('Request failed with status code 500'));
    const client = seeded();
    const before = snapshot(client);

    expect(await archiveThreadRow(client, 'ws1', 't1')).toBe(false);
    expect(snapshot(client)).toEqual(before);
    expect(isThreadRemoved('t1')).toBe(false);
  });
});

describe('setThreadArchived', () => {
  it('brings a thread back: it leaves the archived view, and every list is read again', async () => {
    vi.mocked(updateThread).mockResolvedValue({} as never);
    const client = seeded();
    expect(await setThreadArchived(client, 'ws1', 't0', true)).toBe(true);
    expect(isThreadRemoved('t0')).toBe(true);
    expect(await setThreadArchived(client, 'ws1', 't0', false)).toBe(true);
    expect(isThreadRemoved('t0')).toBe(false);

    expect(updateThread).toHaveBeenCalledWith('t0', { archived: false });
    expect(ids(client.getQueryData(keys.archived))).toEqual([]);
    expect(ids(client.getQueryData(keys.page))).toEqual(['t1', 't2']);
    for (const key of [keys.page, keys.recent, keys.search]) expect(client.getQueryState(key)?.isInvalidated).toBe(true);
  });
});
