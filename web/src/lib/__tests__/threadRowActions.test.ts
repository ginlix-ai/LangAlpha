/**
 * The shared thread-row patcher.
 *
 * A workspace's threads sit in the cache under two shapes at once — the
 * sidebar's finite `{threads, total}` pages and the gallery's
 * `InfiniteData.pages` — under one `byWorkspace` prefix. Every mutation
 * composes this module, so the settled contract is: both shapes get patched,
 * totals track added/removed rows, untouched entries keep their identity, and
 * a failed mutation rolls back exactly what it wrote.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import {
  isArchivedThreadsKey,
  mapThreadCacheEntry,
  patchThreadRows,
  rollbackThreadRows,
} from '@/lib/threadRowActions';
import {
  getNavThreadsSnapshot,
  resetSharedWorkspaceThreads,
  setSharedWorkspaceThreads,
} from '@/lib/navThreadsStore';

interface Row { thread_id: string; title?: string; [key: string]: unknown }

const row = (id: string, title = `Thread ${id}`): Row => ({ thread_id: id, title });
const rename = (id: string, title: string) => (rows: Row[]) =>
  rows.some((r) => r.thread_id === id)
    ? rows.map((r) => (r.thread_id === id ? { ...r, title } : r))
    : rows;
const drop = (id: string) => (rows: Row[]) =>
  rows.some((r) => r.thread_id === id) ? rows.filter((r) => r.thread_id !== id) : rows;

const WS = 'ws-1';
const finiteKey = [...queryKeys.threads.byWorkspace(WS), 10, 0];
const galleryKey = queryKeys.threads.gallery(WS, false);
const archivedKey = queryKeys.threads.gallery(WS, true);

describe('mapThreadCacheEntry', () => {
  it('patches a finite page and leaves the total alone when the row count is unchanged', () => {
    const next = mapThreadCacheEntry<Row>(
      { threads: [row('t-1'), row('t-2')], total: 7 },
      rename('t-2', 'Renamed'),
    ) as { threads: Row[]; total: number };

    expect(next.threads.map((r) => r.title)).toEqual(['Thread t-1', 'Renamed']);
    expect(next.total).toBe(7);
  });

  it('patches every page of an infinite entry and moves each page copy of the total', () => {
    const next = mapThreadCacheEntry<Row>(
      {
        pages: [
          { threads: [row('t-1'), row('t-2')], total: 7 },
          { threads: [row('t-3')], total: 7 },
        ],
        pageParams: [0, 2],
      },
      drop('t-3'),
    ) as { pages: Array<{ threads: Row[]; total: number }>; pageParams: number[] };

    expect(next.pages[0].threads.map((r) => r.thread_id)).toEqual(['t-1', 't-2']);
    expect(next.pages[1].threads).toEqual([]);
    // `total` is duplicated across pages, so a dropped row has to move BOTH
    // copies or the "has more" arithmetic drifts by one forever.
    expect(next.pages.map((p) => p.total)).toEqual([6, 6]);
    expect(next.pageParams).toEqual([0, 2]);
  });

  it('restricts an infinite entry to page 0 under headOnly (prepend semantics)', () => {
    const next = mapThreadCacheEntry<Row>(
      {
        pages: [
          { threads: [row('t-1')], total: 2 },
          { threads: [row('t-2')], total: 2 },
        ],
        pageParams: [0, 1],
      },
      (rows) => [row('t-new'), ...rows],
      { headOnly: true },
    ) as { pages: Array<{ threads: Row[]; total: number }> };

    expect(next.pages[0].threads.map((r) => r.thread_id)).toEqual(['t-new', 't-1']);
    expect(next.pages[1].threads.map((r) => r.thread_id)).toEqual(['t-2']);
    expect(next.pages.map((p) => p.total)).toEqual([3, 3]);
  });

  it('returns undefined for entries that carry no rows, and for a no-op mapper', () => {
    // Non-list payloads share the byWorkspace prefix, so a prefix-wide patch
    // walks over them — they must come back untouched, not half-rewritten.
    expect(mapThreadCacheEntry<Row>({ thread_id: 't-1', title: 'x' }, rename('t-1', 'y'))).toBeUndefined();
    expect(mapThreadCacheEntry<Row>(undefined, rename('t-1', 'y'))).toBeUndefined();
    expect(mapThreadCacheEntry<Row>({ threads: [row('t-1')] }, rename('t-absent', 'y'))).toBeUndefined();
  });
});

describe('isArchivedThreadsKey', () => {
  it('recognizes the gallery archived view and a plain archived suffix', () => {
    expect(isArchivedThreadsKey(archivedKey)).toBe(true);
    expect(isArchivedThreadsKey([...queryKeys.threads.byWorkspace(WS), { archived: true }])).toBe(true);
    expect(isArchivedThreadsKey(galleryKey)).toBe(false);
    expect(isArchivedThreadsKey(finiteKey)).toBe(false);
  });
});

describe('patchThreadRows', () => {
  function seed() {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(finiteKey, { threads: [row('t-1'), row('t-2')], total: 5 });
    qc.setQueryData(galleryKey, {
      pages: [{ threads: [row('t-1'), row('t-2')], total: 5 }, { threads: [row('t-9')], total: 5 }],
      pageParams: [0, 2],
    });
    qc.setQueryData(archivedKey, { pages: [{ threads: [row('t-arch')], total: 1 }], pageParams: [0] });
    // A non-list payload sharing the prefix — the walk must step over it.
    qc.setQueryData([...queryKeys.threads.byWorkspace(WS), 'summary'], { count: 2 });
    return qc;
  }

  it('reaches both shapes under the prefix in one pass', () => {
    const qc = seed();
    patchThreadRows<Row>(qc, queryKeys.threads.byWorkspace(WS), rename('t-2', 'Renamed'));

    const finite = qc.getQueryData(finiteKey) as { threads: Row[] };
    const gallery = qc.getQueryData(galleryKey) as { pages: Array<{ threads: Row[] }> };
    expect(finite.threads[1].title).toBe('Renamed');
    expect(gallery.pages[0].threads[1].title).toBe('Renamed');
  });

  it('leaves unrelated entries untouched — same object identity, no churn', () => {
    const qc = seed();
    const summaryKey = [...queryKeys.threads.byWorkspace(WS), 'summary'];
    const archivedBefore = qc.getQueryData(archivedKey);
    const summaryBefore = qc.getQueryData(summaryKey);

    patchThreadRows<Row>(qc, queryKeys.threads.byWorkspace(WS), rename('t-2', 'Renamed'));

    // t-2 is absent from the archived view, so its mapper is a no-op there.
    expect(qc.getQueryData(archivedKey)).toBe(archivedBefore);
    expect(qc.getQueryData(summaryKey)).toBe(summaryBefore);
  });

  it('honors skipKey so an archive never lands in the archived view', () => {
    const qc = seed();
    qc.setQueryData(archivedKey, { pages: [{ threads: [row('t-2')], total: 1 }], pageParams: [0] });
    const archivedBefore = qc.getQueryData(archivedKey);

    patchThreadRows<Row>(qc, queryKeys.threads.byWorkspace(WS), drop('t-2'), {
      skipKey: isArchivedThreadsKey,
    });

    expect((qc.getQueryData(finiteKey) as { threads: Row[] }).threads.map((r) => r.thread_id)).toEqual(['t-1']);
    expect(qc.getQueryData(archivedKey)).toBe(archivedBefore);
  });

  it('rolls back exactly the entries it wrote', () => {
    const qc = seed();
    const finiteBefore = qc.getQueryData(finiteKey);
    const galleryBefore = qc.getQueryData(galleryKey);

    const snapshot = patchThreadRows<Row>(qc, queryKeys.threads.byWorkspace(WS), drop('t-2'));
    expect(snapshot.cache.length).toBe(2);
    expect(qc.getQueryData(finiteKey)).not.toBe(finiteBefore);

    rollbackThreadRows(qc, snapshot);

    // Value, not identity: setQueryData structurally shares against the patched
    // data, so the restored entry is an equal object, not the original one.
    expect(qc.getQueryData(finiteKey)).toEqual(finiteBefore);
    expect(qc.getQueryData(galleryKey)).toEqual(galleryBefore);
  });
});

// The store-side half of the contract: rows paged into the nav tree's shared
// "Show more" store render as extras beneath page-0, so every patch must reach
// them too — a removal that only cleaned the caches would leave a ghost row in
// the sidebar (archive-from-gallery, cross-tab feed prunes).
describe('patchThreadRows — nav Show-more store composition', () => {
  beforeEach(() => resetSharedWorkspaceThreads());

  const seedStore = () => setSharedWorkspaceThreads(() => ({
    [WS]: { threads: [row('t-1'), row('t-2'), row('t-3')], loading: false, total: 5 },
    'ws-2': { threads: [row('u-1')], loading: false, total: 1 },
  }));

  it('drops the row from the store alongside the caches and tracks the total', () => {
    const qc = new QueryClient();
    qc.setQueryData(finiteKey, { threads: [row('t-1'), row('t-2')], total: 5 });
    seedStore();

    patchThreadRows<Row>(qc, queryKeys.threads.byWorkspace(WS), drop('t-2'));

    expect(getNavThreadsSnapshot()[WS].threads.map((r) => r.thread_id)).toEqual(['t-1', 't-3']);
    expect(getNavThreadsSnapshot()[WS].total).toBe(4);
    // Workspaces the mapper doesn't match keep their entry identity.
    expect(getNavThreadsSnapshot()['ws-2'].threads.map((r) => r.thread_id)).toEqual(['u-1']);
  });

  it('rollbackThreadRows restores the store entries it wrote', () => {
    const qc = new QueryClient();
    seedStore();
    const before = getNavThreadsSnapshot()[WS];

    const snapshot = patchThreadRows<Row>(qc, queryKeys.threads.byWorkspace(WS), drop('t-2'));
    expect(getNavThreadsSnapshot()[WS].threads.length).toBe(2);

    rollbackThreadRows(qc, snapshot);
    // Identity, not just equality: the snapshot holds the original entry.
    expect(getNavThreadsSnapshot()[WS]).toBe(before);
  });

  it('headOnly (prepend semantics) never touches the store', () => {
    const qc = new QueryClient();
    seedStore();
    const before = getNavThreadsSnapshot();

    patchThreadRows<Row>(
      qc,
      queryKeys.threads.byWorkspace(WS),
      (rows) => [row('t-new'), ...rows],
      { headOnly: true },
    );

    expect(getNavThreadsSnapshot()).toBe(before);
  });
});
