/**
 * User thread-events feed client: one SSE connection per tab (mounted at the
 * authenticated shell) driving the lifecycle store for EVERY thread — the
 * push channel that makes dispatched/backgrounded runs visible without any
 * chat view mounted.
 *
 * Connection policy (thread-lifecycle v6 §2.3): forever-resubscribe. A
 * connection that lived ≥30s counts as healthy → instant re-attach (covers
 * the server's 600s duration cap); anything shorter backs off exponentially
 * to 30s. pagehide/freeze abort the stream (never hold a socket through
 * bfcache); pageshow/visibilitychange resume it.
 *
 * Restart safety: `start()`/`stop()` bump a GENERATION counter and the loop
 * re-checks it after every suspension point. Auth churn (sign-out → sign-in)
 * therefore cannot leave a zombie loop reconnecting beside the live one, and a
 * dying loop cannot abort or null the live loop's connection.
 *
 * NOTE (layering wart, deliberate): imports streamFetch/getAuthHeaders from
 * pages/ChatAgent/utils/api/transport — the one implementation of the SSE
 * parser + pre-expiry token refresh. Duplicating that subtle logic in lib/
 * would be worse than a lib→pages import.
 */
import type { QueryClient } from '@tanstack/react-query';
import { isCacheOnlyMeta, queryKeys } from '@/lib/queryKeys';
import { patchThreadTitle } from '@/lib/threadListCache';
import { isArchivedThreadsKey, patchThreadRows } from '@/lib/threadRowActions';
import {
  getAuthHeaders,
  streamFetch,
} from '@/pages/ChatAgent/utils/api/transport';
import type { Thread } from '@/types/api';
import {
  getLifecycleQueryClient,
  markThreadSeen,
  setLifecycleQueryClient,
} from './seen';
import {
  applyFeedEvent,
  applySnapshot,
  getActiveThreadId,
  pruneThread,
  seedFromListRows,
  type SnapshotFrame,
} from './store';

const FEED_URL = '/api/v1/users/me/thread-events';
const HEALTHY_CONNECTION_MS = 30_000;
const BACKOFF_START_MS = 1_000;
const BACKOFF_MAX_MS = 30_000;
const INVALIDATE_DEBOUNCE_MS = 300;

interface ClientState {
  started: boolean;
  /** Bumped by every start/stop; the running loop's claim on this client. */
  generation: number;
  suspended: boolean;
  abort: AbortController | null;
  resume: (() => void) | null;
  backoffMs: number;
  pendingWorkspaces: Set<string>;
  /** Armed at every connection attach; the connection's first snapshot
   * consumes it and upgrades its invalidate to the whole 'threads' prefix —
   * events may have been missed while detached (frozen tab, backend restart,
   * 600s cap) and the feed has no replay. */
  resyncOnSnapshot: boolean;
  pendingFullInvalidate: boolean;
  invalidateTimer: ReturnType<typeof setTimeout> | null;
  unsubscribeCache: (() => void) | null;
  detachWindow: (() => void) | null;
}

// globalThis-anchored like the store: HMR must not spawn a second connection.
const ANCHOR = '__langalpha_thread_lifecycle_feed__';
const globalAnchor = globalThis as typeof globalThis & {
  [ANCHOR]?: ClientState;
};
const client: ClientState =
  globalAnchor[ANCHOR] ??
  (globalAnchor[ANCHOR] = {
    started: false,
    generation: 0,
    suspended: false,
    abort: null,
    resume: null,
    backoffMs: BACKOFF_START_MS,
    pendingWorkspaces: new Set(),
    resyncOnSnapshot: false,
    pendingFullInvalidate: false,
    invalidateTimer: null,
    unsubscribeCache: null,
    detachWindow: null,
  });

/** Backoff nap that `stop()`/suspend can cut short through the loop's signal. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(finish, ms);
    function finish(): void {
      clearTimeout(timer);
      signal.removeEventListener('abort', finish);
      resolve();
    }
    signal.addEventListener('abort', finish);
  });
}

/** invalidateQueries only refetches enabled observers, and the nav tree's
 * page-0 lists observe cache-only (`enabled: false`) — a stale marking alone
 * leaves them frozen until the user navigates into the workspace. Fetch the
 * invalidated ones a {@link CACHE_ONLY_META} observer vouches for.
 *
 * One voucher is enough: a key's observers all share its arguments, and
 * unrelated disabled observers do attach to these keys — a parked ChatView
 * holds one on its own workspace's list — so demanding unanimity would
 * refreeze exactly the lists this exists to thaw. */
function refetchCacheOnlyLists(qc: QueryClient, queryKey: readonly unknown[]): void {
  for (const query of qc.getQueryCache().findAll({ queryKey })) {
    // state.isInvalidated, not the `stale` filter: with observers attached
    // isStale() reads their results, and a disabled observer never reports
    // stale, so the filter matches none of these.
    if (!query.state.isInvalidated) continue;
    // An enabled observer means invalidateQueries already refetched it.
    if (query.isActive()) continue;
    if (!query.observers.some((o) => isCacheOnlyMeta(o.options.meta))) continue;
    void query.fetch().catch(() => {});
  }
}

/** Debounced list invalidation: a settle burst (parallel dispatched runs)
 * coalesces into one refetch per touched workspace + the recent lists. */
function scheduleInvalidate(workspaceId?: string | null): void {
  if (workspaceId) client.pendingWorkspaces.add(workspaceId);
  if (client.invalidateTimer) return;
  client.invalidateTimer = setTimeout(() => {
    client.invalidateTimer = null;
    const qc = getLifecycleQueryClient();
    if (!qc) return;
    const full = client.pendingFullInvalidate;
    client.pendingFullInvalidate = false;
    const workspaces = [...client.pendingWorkspaces];
    client.pendingWorkspaces.clear();
    if (full) {
      // Reconnect resync: one prefix-wide invalidate subsumes the scoped ones.
      void qc.invalidateQueries({ queryKey: queryKeys.threads.all });
      refetchCacheOnlyLists(qc, queryKeys.threads.all);
      return;
    }
    for (const ws of workspaces) {
      void qc.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(ws) });
      refetchCacheOnlyLists(qc, queryKeys.threads.byWorkspace(ws));
    }
    void qc.invalidateQueries({ queryKey: queryKeys.threads.recentAll() });
  }, INVALIDATE_DEBOUNCE_MS);
}

function onFeedEvent(raw: Record<string, unknown>): void {
  const eventName = raw.event as string | undefined;
  if (eventName === 'snapshot') {
    const snap = raw as unknown as SnapshotFrame;
    applySnapshot(snap);
    if (client.resyncOnSnapshot) {
      client.resyncOnSnapshot = false;
      client.pendingFullInvalidate = true;
      scheduleInvalidate();
      return;
    }
    // Scope the refetch to the workspaces the frame actually touches — a
    // bare scheduleInvalidate() only reaches the 'recent' list, which makes
    // the periodic reconcile a no-op on the chat page.
    const touched = new Set<string>();
    for (const entry of [...(snap.live ?? []), ...(snap.unseen ?? [])]) {
      if (entry.workspace_id) touched.add(entry.workspace_id);
    }
    if (touched.size) {
      for (const ws of touched) scheduleInvalidate(ws);
    } else {
      scheduleInvalidate();
    }
    return;
  }
  if (eventName !== 'thread_lifecycle') return; // 'timeout' → loop re-attaches

  const evt = raw as {
    type?: string;
    thread_id?: string;
    workspace_id?: string | null;
    run_id?: string | null;
    title?: string | null;
    pinned?: boolean | null;
    updated_at?: string | null;
  };
  switch (evt.type) {
    case 'run_started':
    case 'run_settled': {
      applyFeedEvent(raw as Parameters<typeof applyFeedEvent>[0]);
      scheduleInvalidate(evt.workspace_id);
      // Settled while the user is looking at the thread: stamp seen so the
      // dot never appears for a watched finish.
      if (
        evt.type === 'run_settled' &&
        evt.thread_id &&
        evt.run_id &&
        evt.thread_id === getActiveThreadId()
      ) {
        markThreadSeen(evt.thread_id, evt.run_id);
      }
      return;
    }
    case 'thread_title': {
      const qc = getLifecycleQueryClient();
      // An empty title is a real payload — clearing a title publishes "" —
      // so gate on presence, not truthiness.
      if (qc && evt.thread_id && evt.title != null) {
        // updated_at versions the event: a delayed generated-title event must
        // not overwrite a manual rename that already landed in the cache.
        patchThreadTitle(qc, evt.thread_id, evt.title, evt.updated_at ?? undefined);
      }
      return;
    }
    // Pin hint: pin state lives only in list rows (the snapshot frame has no
    // is_pinned), so without this a pin in another tab never reaches the
    // sidebar. Patch the flag in place for the cache-only lists, then refetch
    // for the server's pinned-first ordering.
    case 'thread_pinned': {
      const qc = getLifecycleQueryClient();
      if (qc && evt.thread_id && typeof evt.pinned === 'boolean') {
        const tid = evt.thread_id;
        const pinned = evt.pinned;
        patchThreadRows<Thread>(
          qc,
          evt.workspace_id ? queryKeys.threads.byWorkspace(evt.workspace_id) : queryKeys.threads.all,
          (rows) => (rows.some((t) => t.thread_id === tid)
            ? rows.map((t) => (t.thread_id === tid ? { ...t, is_pinned: pinned } : t))
            : rows),
        );
      }
      scheduleInvalidate(evt.workspace_id);
      return;
    }
    // Archive is a prune too: an archived row leaves the snapshot exactly like
    // a deleted one, so its retained observations (and unseen state) must go.
    case 'thread_deleted':
    case 'thread_archived': {
      const qc = getLifecycleQueryClient();
      if (qc && evt.thread_id) {
        const tid = evt.thread_id;
        // Drop the row from the cached lists + the nav tree's Show-more store
        // directly: the invalidate below only reaches enabled observers, and
        // the sidebar's page-0 observers are cache-only — without this, a
        // thread archived in another tab lingers in the tree until reload.
        // Archived rows keep their archived-view entries (that list gains the
        // row via the invalidate instead).
        patchThreadRows<Thread>(
          qc,
          evt.workspace_id ? queryKeys.threads.byWorkspace(evt.workspace_id) : queryKeys.threads.all,
          (rows) => (rows.some((t) => t.thread_id === tid) ? rows.filter((t) => t.thread_id !== tid) : rows),
          evt.type === 'thread_archived' ? { skipKey: isArchivedThreadsKey } : {},
        );
      }
      if (evt.thread_id) pruneThread(evt.thread_id);
      scheduleInvalidate(evt.workspace_id);
      return;
    }
    // The reverse move: drop the row from cached archived views; the live
    // lists regain it via the invalidate refetch (which also re-seeds its
    // lifecycle fields — they were pruned when the archive event landed).
    case 'thread_unarchived': {
      const qc = getLifecycleQueryClient();
      if (qc && evt.thread_id) {
        const tid = evt.thread_id;
        patchThreadRows<Thread>(
          qc,
          evt.workspace_id ? queryKeys.threads.byWorkspace(evt.workspace_id) : queryKeys.threads.all,
          (rows) => (rows.some((t) => t.thread_id === tid) ? rows.filter((t) => t.thread_id !== tid) : rows),
          { skipKey: (key) => !isArchivedThreadsKey(key) },
        );
      }
      scheduleInvalidate(evt.workspace_id);
      return;
    }
    default:
      return; // forward-compatible: unknown types are ignored
  }
}

/** Seed the store from every thread-list response (rows carry lifecycle
 * fields) — cold caches first, then each future success via subscription. */
function attachCacheSeeding(qc: QueryClient): void {
  const seedFromData = (data: unknown): void => {
    // Two cache shapes live under the 'threads' prefix: plain lists
    // ({threads}) and the gallery's infinite query ({pages: [{threads}]}).
    const shaped = data as
      | { threads?: Thread[]; pages?: Array<{ threads?: Thread[] }> }
      | undefined;
    const rows = Array.isArray(shaped?.pages)
      ? shaped.pages.flatMap((p) => p?.threads ?? [])
      : shaped?.threads;
    if (Array.isArray(rows) && rows.length) seedFromListRows(rows);
  };
  for (const [, data] of qc.getQueriesData({ queryKey: queryKeys.threads.all })) {
    seedFromData(data);
  }
  client.unsubscribeCache = qc.getQueryCache().subscribe((event) => {
    if (event.type !== 'updated' || event.action?.type !== 'success') return;
    const key = event.query.queryKey;
    if (!Array.isArray(key) || key[0] !== 'threads') return;
    seedFromData(event.query.state.data);
  });
}

function attachWindowHandlers(): void {
  const suspend = (): void => {
    if (client.suspended) return;
    client.suspended = true;
    client.abort?.abort();
  };
  const resume = (): void => {
    if (!client.suspended) return;
    client.suspended = false;
    client.resume?.();
  };
  const onVisibility = (): void => {
    if (document.visibilityState === 'visible') resume();
  };
  window.addEventListener('pagehide', suspend);
  window.addEventListener('freeze', suspend);
  window.addEventListener('pageshow', resume);
  document.addEventListener('visibilitychange', onVisibility);
  client.detachWindow = () => {
    window.removeEventListener('pagehide', suspend);
    window.removeEventListener('freeze', suspend);
    window.removeEventListener('pageshow', resume);
    document.removeEventListener('visibilitychange', onVisibility);
  };
}

/**
 * The reconnect loop for one generation. Every suspension point re-checks
 * `client.generation` before touching shared state: a stale loop returns
 * instead of reconnecting, and its abort controller — loop-local, published
 * to `client.abort` only for the window it owns — is released by identity so
 * it can never null the live loop's connection.
 */
async function runLoop(gen: number): Promise<void> {
  while (client.generation === gen) {
    if (client.suspended) {
      let release!: () => void;
      const resumed = new Promise<void>((r) => {
        release = r;
      });
      client.resume = release;
      await resumed;
      if (client.resume === release) client.resume = null;
      continue; // generation + suspension re-checked at the loop top
    }

    const controller = new AbortController();
    client.abort = controller; // generation is current — no await since the check
    client.resyncOnSnapshot = true; // this connection's first snapshot resyncs
    const attachedAt = Date.now();
    try {
      try {
        const headers = await getAuthHeaders();
        if (client.generation !== gen) return;
        await streamFetch(
          FEED_URL,
          {
            method: 'GET',
            headers: { Accept: 'text/event-stream', ...headers },
            signal: controller.signal,
          },
          onFeedEvent,
        );
      } catch {
        // Non-2xx (auth churn, backend restart) → backoff below.
      }
      if (client.generation !== gen) return;
      if (client.suspended) continue; // reconnect instantly on resume
      const lived = Date.now() - attachedAt;
      if (lived >= HEALTHY_CONNECTION_MS) {
        client.backoffMs = BACKOFF_START_MS; // healthy (incl. 600s cap) → instant
      } else {
        const jitter = Math.random() * 0.3 * client.backoffMs;
        await sleep(client.backoffMs + jitter, controller.signal);
        if (client.generation !== gen) return;
        client.backoffMs = Math.min(client.backoffMs * 2, BACKOFF_MAX_MS);
      }
    } finally {
      if (client.abort === controller) client.abort = null;
    }
  }
}

/** Idempotent; called from the authenticated shell mount. */
export function startThreadLifecycleFeed(queryClient: QueryClient): void {
  setLifecycleQueryClient(queryClient);
  if (client.started) return;
  client.started = true;
  attachCacheSeeding(queryClient);
  attachWindowHandlers();
  void runLoop((client.generation += 1));
}

/** Shell unmount (logout route change): tear the connection down. */
export function stopThreadLifecycleFeed(): void {
  if (!client.started) return;
  client.started = false;
  // Invalidation, not a flag: the running loop bails at its next check and a
  // later start() owns a generation it can never confuse with this one.
  client.generation += 1;
  client.suspended = false;
  client.abort?.abort();
  client.resume?.();
  client.unsubscribeCache?.();
  client.unsubscribeCache = null;
  client.detachWindow?.();
  client.detachWindow = null;
  if (client.invalidateTimer) {
    clearTimeout(client.invalidateTimer);
    client.invalidateTimer = null;
  }
  client.pendingWorkspaces.clear();
  client.resyncOnSnapshot = false;
  client.pendingFullInvalidate = false;
  client.backoffMs = BACKOFF_START_MS;
}
