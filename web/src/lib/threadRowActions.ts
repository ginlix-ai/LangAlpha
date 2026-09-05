/**
 * Shape-agnostic row patching for cached thread lists.
 *
 * A workspace's threads sit in the cache under more than one shape at once —
 * the sidebar's finite `{threads, total}` page entries and ThreadGallery's
 * `InfiniteData.pages` — plus the nav tree's module-level "Show more" store.
 * The mapper that rewrites rows therefore lives here once and every mutation
 * composes it: a helper that only understood `{threads}` silently skipped the
 * infinite entries, so gallery rows never saw titles, seen cursors, or
 * optimistic inserts.
 */
import type { QueryClient } from '@tanstack/react-query';
import type { Thread } from '@/types/api';
import {
  patchNavThreadsRows,
  restoreNavThreadsRows,
  type NavThreadsStoreSnapshot,
  type ThreadRecord as NavThreadRecord,
} from '@/lib/navThreadsStore';

/**
 * Rewrite a list of rows. Return the SAME array to signal "nothing to do" —
 * callers skip the cache write, so unrelated entries never churn. Generic over
 * the row type because the nav tree carries a looser row shape than `Thread`;
 * nothing here inspects a field, so the mapper picks the view.
 */
export type ThreadRowMapper<TRow = Thread> = (rows: TRow[]) => TRow[];

interface ThreadListPage<TRow> {
  threads?: TRow[];
  total?: number;
  [key: string]: unknown;
}

interface InfiniteThreadList<TRow> {
  pages?: ThreadListPage<TRow>[];
  [key: string]: unknown;
}

/** Snapshot of the entries a patch touched (query caches + the nav store), for rollbackThreadRows. */
export interface ThreadCacheSnapshot {
  cache: Array<[readonly unknown[], unknown]>;
  store: NavThreadsStoreSnapshot;
}

export interface MapThreadRowsOptions {
  /** Restrict an infinite entry to its first page — prepend semantics (a new row belongs on page 0, not on every page). */
  headOnly?: boolean;
}

function withTotal<TRow>(page: ThreadListPage<TRow>, delta: number): ThreadListPage<TRow> {
  if (delta === 0 || typeof page.total !== 'number') return page;
  return { ...page, total: Math.max(0, page.total + delta) };
}

/**
 * Map one cache entry's rows, whatever shape it holds. Returns `undefined`
 * when the entry carries no thread rows (detail/status/liveness entries) or
 * the mapper made no change, so callers can skip the write.
 */
export function mapThreadCacheEntry<TRow = Thread>(
  data: unknown,
  mapRows: ThreadRowMapper<TRow>,
  { headOnly = false }: MapThreadRowsOptions = {},
): unknown | undefined {
  if (!data || typeof data !== 'object') return undefined;

  const infinite = data as InfiniteThreadList<TRow>;
  if (Array.isArray(infinite.pages)) {
    const source = infinite.pages;
    let delta = 0;
    let changed = false;
    const mapped = source.map((page, index) => {
      if (headOnly && index > 0) return page;
      const rows = page?.threads;
      if (!Array.isArray(rows)) return page;
      const next = mapRows(rows);
      if (next === rows) return page;
      changed = true;
      delta += next.length - rows.length;
      return { ...page, threads: next };
    });
    if (!changed) return undefined;
    // `total` is duplicated across pages (each page echoes the server count),
    // so a row added or dropped on one page has to move every copy or the
    // "has more" arithmetic drifts.
    return { ...infinite, pages: mapped.map((page) => withTotal(page, delta)) };
  }

  const list = data as ThreadListPage<TRow>;
  const rows = list.threads;
  if (!Array.isArray(rows)) return undefined;
  const next = mapRows(rows);
  if (next === rows) return undefined;
  return withTotal({ ...list, threads: next }, next.length - rows.length);
}

/**
 * True for a cache key holding the ARCHIVED view of a workspace's threads.
 * Both the gallery's `{view: 'gallery', archived}` key and any plain
 * `{archived: true}` suffix live under the `byWorkspace` prefix, so a
 * prefix-wide patch that must not touch archived rows filters on this.
 */
export function isArchivedThreadsKey(key: readonly unknown[]): boolean {
  return key.some(
    (part) => typeof part === 'object' && part !== null && (part as Record<string, unknown>).archived === true,
  );
}

/**
 * Apply `mapRows` to every cached thread list under `queryKey`, AND to the nav
 * tree's module-level "Show more" store — its rows render as extras beneath
 * page-0, so a patch that skipped it would let a removed row survive in the
 * sidebar. Returns the pre-patch snapshot of everything written, for rollback.
 *
 * The store isn't scoped by query key, so non-`headOnly` mappers must be
 * id-targeted (return the same array when no row matches). `headOnly` marks
 * prepend semantics — page-0 by definition — and skips the store.
 */
export function patchThreadRows<TRow = Thread>(
  queryClient: QueryClient,
  queryKey: readonly unknown[],
  mapRows: ThreadRowMapper<TRow>,
  options: MapThreadRowsOptions & { skipKey?: (key: readonly unknown[]) => boolean } = {},
): ThreadCacheSnapshot {
  const { skipKey, ...mapOptions } = options;
  const cache: ThreadCacheSnapshot['cache'] = [];
  for (const [key, data] of queryClient.getQueriesData({ queryKey })) {
    if (skipKey?.(key)) continue;
    const next = mapThreadCacheEntry(data, mapRows, mapOptions);
    if (next === undefined) continue;
    cache.push([key, data]);
    queryClient.setQueryData(key, next);
  }
  // Same mapper, looser row shape — nothing here inspects a field beyond what
  // the mapper itself matches on, so the view-level cast is safe.
  const store = mapOptions.headOnly
    ? []
    : patchNavThreadsRows(mapRows as unknown as (rows: NavThreadRecord[]) => NavThreadRecord[]);
  return { cache, store };
}

/** Restore a snapshot captured by patchThreadRows. */
export function rollbackThreadRows(queryClient: QueryClient, snapshot: ThreadCacheSnapshot): void {
  for (const [key, data] of snapshot.cache) queryClient.setQueryData(key, data);
  restoreNavThreadsRows(snapshot.store);
}
