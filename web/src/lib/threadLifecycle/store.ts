/**
 * threadLifecycleStore — the single render source for per-thread run state:
 * spinner (live run), needs-input (interrupted), and the unseen dot.
 *
 * Design (thread-lifecycle v6): RETAINED OBSERVATIONS, not live-only state.
 * Each thread keeps two layered observations — `local` (this tab's own SSE
 * stream via useChatMessages) and `feed` (the user-events feed + snapshots +
 * list-row seeding) — and the EFFECTIVE observation is the seq-newest across
 * layers. Everything the UI shows is DERIVED, never stored as a flag:
 *
 *   spinner     = effective.status ∈ live set   (gated by liveSuppressedBelowSeq)
 *   needsInput  = effective.status = interrupted (same gate)
 *   unseen      = effective.status ∈ {completed,failed,cancelled}
 *                 && runSeq > lastSeenSeq && runSeq > seenSuppressedBelowSeq
 *                 && thread ≠ activeThreadId
 *
 * Both reconciliation watermarks are REQUIRED — a single status-agnostic
 * scalar is the v5 bug: live absence from a snapshot (the live set is
 * uncapped, absence is proof) must only ever switch live indicators off and
 * never erase terminal unseen state that was merely clipped below the unseen
 * cap; terminal absence proves seen only at/above the truncation cutoff.
 *
 * Anchored on globalThis (survives HMR module identity churn) and registered
 * with authResets (module singletons outlive React — web/AGENTS.md).
 */
import { useMemo, useSyncExternalStore } from 'react';
import { registerAuthReset } from '@/lib/authResets';
import { createEmitter, type Emitter } from '@/lib/emitter';
import type { Thread } from '@/types/api';

export type PublicRunStatus =
  | 'idle'
  | 'queued'
  | 'running'
  | 'stopping'
  | 'recovering'
  | 'completed'
  | 'interrupted'
  | 'failed'
  | 'cancelled';

export interface LifecycleObservation {
  runId?: string;
  runSeq?: number;
  status: PublicRunStatus;
  interruptReason?: string | null;
}

interface ThreadEntry {
  local?: LifecycleObservation;
  feed?: LifecycleObservation;
  lastSeenSeq: number;
  liveSuppressedBelowSeq: number;
  seenSuppressedBelowSeq: number;
}

export interface SnapshotEntry {
  thread_id: string;
  workspace_id: string;
  run_id: string;
  run_seq: number;
  status: PublicRunStatus;
  interrupt_reason?: string | null;
  last_seen_run_seq: number;
}

export interface SnapshotFrame {
  as_of_seq: number;
  /**
   * 0 = the unseen set is complete; absence below this seq proves nothing.
   * The wire also carries a `unseen_truncated` boolean, deliberately untyped
   * here: this cutoff subsumes it (0 iff not truncated) and is the only form
   * the absence rule can consume.
   */
  oldest_included_unseen_seq: number;
  live: SnapshotEntry[];
  unseen: SnapshotEntry[];
}

// Hand mirror of the wire authority — LIVE_PUBLIC_STATUSES /
// TERMINAL_PUBLIC_STATUSES in src/server/contracts/status.py, where the
// partition is derived and contract-tested. Change there first, then here.
const LIVE_STATUSES: ReadonlySet<string> = new Set([
  'queued',
  'running',
  'stopping',
  'recovering',
]);
export const TERMINAL_FAMILY: ReadonlySet<string> = new Set([
  'completed',
  'failed',
  'cancelled',
]);

interface StoreState {
  threads: Map<string, ThreadEntry>;
  activeThreadId: string | null;
  emitter: Emitter;
  running: ReadonlySet<string>;
  needsInput: ReadonlySet<string>;
  unseen: ReadonlySet<string>;
  effStatus: ReadonlyMap<string, PublicRunStatus>;
  resetRegistered: boolean;
}

const ANCHOR = '__langalpha_thread_lifecycle_store__';

function freshState(): StoreState {
  return {
    threads: new Map(),
    activeThreadId: null,
    emitter: createEmitter(),
    running: new Set(),
    needsInput: new Set(),
    unseen: new Set(),
    effStatus: new Map(),
    resetRegistered: false,
  };
}

const globalAnchor = globalThis as typeof globalThis & {
  [ANCHOR]?: StoreState;
};
const state: StoreState = globalAnchor[ANCHOR] ?? (globalAnchor[ANCHOR] = freshState());

if (!state.resetRegistered) {
  state.resetRegistered = true;
  registerAuthReset(() => resetThreadLifecycle());
}

function ensure(tid: string): ThreadEntry {
  let entry = state.threads.get(tid);
  if (!entry) {
    entry = { lastSeenSeq: 0, liveSuppressedBelowSeq: 0, seenSuppressedBelowSeq: 0 };
    state.threads.set(tid, entry);
  }
  return entry;
}

function isTerminalish(status: PublicRunStatus): boolean {
  return TERMINAL_FAMILY.has(status) || status === 'interrupted';
}

/**
 * The seq-newest observation across layers. With both seqs known it's a pure
 * compare (equal seq / same run → terminal beats live). A local observation
 * has no seq (the chat stream doesn't carry run_seq): it's trusted for its
 * own runId until a seq-bearing layer speaks for that same run, and a local
 * LIVE claim for a run the feed hasn't seen yet (feed lag on run_started)
 * keeps the spinner rather than letting an older feed settle extinguish it.
 */
function effective(entry: ThreadEntry): LifecycleObservation | undefined {
  const { local, feed } = entry;
  if (!local) return feed;
  if (!feed) return local;
  if (typeof local.runSeq === 'number' && typeof feed.runSeq === 'number') {
    if (local.runSeq !== feed.runSeq) {
      return local.runSeq > feed.runSeq ? local : feed;
    }
    return isTerminalish(feed.status) ? feed : local;
  }
  if (typeof local.runSeq !== 'number') {
    if (local.runId && feed.runId && local.runId === feed.runId) {
      // Same run: the seq-bearing feed observation is authoritative
      // (terminal beats live at equal identity — a wedged local stream
      // can't hold a spinner over a crash-recovered settle).
      return isTerminalish(feed.status) ? feed : local;
    }
    // Unmatched runs. A local LIVE claim wins even with no runId yet (the id
    // latches only at response headers — a stale terminal feed obs must not
    // kill the just-sent spinner); a local TERMINAL claim defers to the
    // seq-bearing feed (a newer run may have started from another tab).
    return isTerminalish(local.status) ? feed : local;
  }
  // Local has a seq but feed doesn't (feed observations always carry seq in
  // practice) — trust the seq-bearing layer.
  return local;
}

function setsEqual(a: ReadonlySet<string>, b: ReadonlySet<string>): boolean {
  if (a.size !== b.size) return false;
  for (const v of a) if (!b.has(v)) return false;
  return true;
}

function notify(): void {
  state.emitter.emit();
}

/** Recompute every derived snapshot; notify only when something changed. */
function recompute(): void {
  const running = new Set<string>();
  const needsInput = new Set<string>();
  const unseen = new Set<string>();
  const effStatus = new Map<string, PublicRunStatus>();

  for (const [tid, entry] of state.threads) {
    const eff = effective(entry);
    if (!eff) continue;
    effStatus.set(tid, eff.status);
    const seqKnown = typeof eff.runSeq === 'number';
    if (LIVE_STATUSES.has(eff.status)) {
      if (!seqKnown || (eff.runSeq as number) > entry.liveSuppressedBelowSeq) {
        running.add(tid);
      }
    } else if (eff.status === 'interrupted') {
      if (!seqKnown || (eff.runSeq as number) > entry.liveSuppressedBelowSeq) {
        needsInput.add(tid);
      }
    } else if (TERMINAL_FAMILY.has(eff.status) && tid !== state.activeThreadId) {
      if (seqKnown) {
        if (
          (eff.runSeq as number) > entry.lastSeenSeq &&
          (eff.runSeq as number) > entry.seenSuppressedBelowSeq
        ) {
          unseen.add(tid);
        }
      } else if (entry.local === eff) {
        // A seq-less settle can only come from this tab's own stream
        // transition — fresh by construction, so mint the dot immediately;
        // the feed/list supply the seq (and the durable cursor) right after.
        unseen.add(tid);
      }
    }
  }

  let changed = false;
  if (!setsEqual(state.running, running)) {
    state.running = running;
    changed = true;
  }
  if (!setsEqual(state.needsInput, needsInput)) {
    state.needsInput = needsInput;
    changed = true;
  }
  if (!setsEqual(state.unseen, unseen)) {
    state.unseen = unseen;
    changed = true;
  }
  // effStatus consumers read individual (primitive) values; refresh the map
  // whenever entries changed shape. Compared entry-wise without materializing
  // an array — this runs on every feed event/snapshot.
  let effChanged = changed || effStatus.size !== state.effStatus.size;
  if (!effChanged) {
    for (const [tid, status] of effStatus) {
      if (state.effStatus.get(tid) !== status) {
        effChanged = true;
        break;
      }
    }
  }
  if (effChanged) {
    state.effStatus = effStatus;
    changed = true;
  }
  if (changed) notify();
}

// ---------------------------------------------------------------------------
// Local layer (this tab's own chat stream)
// ---------------------------------------------------------------------------

export function publishLocalRunning(threadId: string, runId?: string): void {
  const entry = ensure(threadId);
  if (
    entry.local &&
    entry.local.status === 'running' &&
    entry.local.runId === runId
  ) {
    return;
  }
  entry.local = { runId, status: 'running' };
  recompute();
}

/**
 * A run reached its natural end on this tab's stream (NOT unmount/eviction —
 * cleanup never mints completion; use clearLocalObservation for that).
 */
export function publishLocalSettled(threadId: string, runId?: string): void {
  const entry = ensure(threadId);
  entry.local = { runId, status: 'completed' };
  recompute();
}

/** Cleanup path (unmount, LRU eviction, id flip): drop only the local layer —
 * a feed-known observation persists underneath. */
export function clearLocalObservation(threadId: string): void {
  const entry = state.threads.get(threadId);
  if (!entry || !entry.local) return;
  entry.local = undefined;
  recompute();
}

/** The thread the user is viewing; unseen derivation excludes it. */
export function setActiveThread(threadId: string | null): void {
  if (state.activeThreadId === threadId) return;
  state.activeThreadId = threadId;
  recompute();
}

export function getActiveThreadId(): string | null {
  return state.activeThreadId;
}

// ---------------------------------------------------------------------------
// Feed layer (user-events feed, snapshots, list-row seeding)
// ---------------------------------------------------------------------------

/** Monotonic merge: an older observation never overrides a newer one; equal
 * seq → terminal beats live (an incoming live never downgrades a terminal). */
function mergeFeedObservation(tid: string, obs: LifecycleObservation): void {
  const entry = ensure(tid);
  const cur = entry.feed;
  if (cur && typeof cur.runSeq === 'number') {
    if (typeof obs.runSeq !== 'number') return;
    if (obs.runSeq < cur.runSeq) return;
    if (
      obs.runSeq === cur.runSeq &&
      isTerminalish(cur.status) &&
      !isTerminalish(obs.status)
    ) {
      return;
    }
  }
  entry.feed = obs;
}

export function applyFeedEvent(evt: {
  type: string;
  thread_id: string;
  run_id?: string | null;
  run_seq?: number | null;
  status?: string | null;
  interrupt_reason?: string | null;
}): void {
  if (evt.type !== 'run_started' && evt.type !== 'run_settled') return;
  if (!evt.thread_id || !evt.status) return;
  mergeFeedObservation(evt.thread_id, {
    runId: evt.run_id ?? undefined,
    runSeq: typeof evt.run_seq === 'number' ? evt.run_seq : undefined,
    status: evt.status as PublicRunStatus,
    interruptReason: evt.interrupt_reason ?? null,
  });
  recompute();
}

/**
 * Snapshot application is monotonic, never wholesale: present entries merge
 * by seq; ABSENCE is handled by the per-thread watermarks (both layers, never
 * by deleting one layer's entry — clearing feed alone would re-expose a stale
 * local `running` underneath), status-aware because the two snapshot sets
 * have different completeness guarantees (see module docstring).
 */
export function applySnapshot(frame: SnapshotFrame): void {
  const present = new Set<string>();
  for (const entry of [...(frame.live ?? []), ...(frame.unseen ?? [])]) {
    present.add(entry.thread_id);
    mergeFeedObservation(entry.thread_id, {
      runId: entry.run_id,
      runSeq: entry.run_seq,
      status: entry.status,
      interruptReason: entry.interrupt_reason ?? null,
    });
    const e = ensure(entry.thread_id);
    if (entry.last_seen_run_seq > e.lastSeenSeq) {
      e.lastSeenSeq = entry.last_seen_run_seq;
    }
    // Positive DB presence beats a prior ABSENCE inference: run_seq is
    // allocated by nextval() and committed later, so a run can commit BELOW
    // an earlier snapshot's as_of watermark. Without this release the
    // watermark suppresses that run forever (suppression only ever raises).
    //
    // Releasing on presence is safe because a present entry is always the
    // thread's LATEST run — three invariants carry that: the snapshot SQL's
    // per-thread LATERAL LIMIT 1, frames delivered sequentially per
    // connection, and forks truncating the run chain atomically. The
    // newest-known guard makes it robust anyway: an entry older than what
    // this store has already observed must not lower watermarks it didn't
    // earn (post-merge, feed.runSeq is the max — equality means this entry
    // IS the newest).
    if (
      typeof entry.run_seq === 'number' &&
      entry.run_seq >= (e.feed?.runSeq ?? 0)
    ) {
      if (e.seenSuppressedBelowSeq >= entry.run_seq) {
        e.seenSuppressedBelowSeq = entry.run_seq - 1;
      }
      if (e.liveSuppressedBelowSeq >= entry.run_seq) {
        e.liveSuppressedBelowSeq = entry.run_seq - 1;
      }
    }
  }

  const asOf = frame.as_of_seq ?? 0;
  const cutoff = frame.oldest_included_unseen_seq ?? 0;
  for (const [tid, entry] of state.threads) {
    if (present.has(tid)) continue;
    for (const obs of [entry.local, entry.feed]) {
      if (!obs || typeof obs.runSeq !== 'number' || obs.runSeq > asOf) continue;
      if (LIVE_STATUSES.has(obs.status) || obs.status === 'interrupted') {
        // Live set is UNCAPPED — absence is proof the run isn't live.
        if (asOf > entry.liveSuppressedBelowSeq) {
          entry.liveSuppressedBelowSeq = asOf;
        }
      } else if (TERMINAL_FAMILY.has(obs.status) && obs.runSeq >= cutoff) {
        // Unseen set is CAPPED — absence proves seen only above the cutoff.
        if (asOf > entry.seenSuppressedBelowSeq) {
          entry.seenSuppressedBelowSeq = asOf;
        }
      }
    }
  }
  recompute();
}

/** Sequence-aware list reconciliation: a stale list response can neither
 * erase a newer settlement nor resurrect a cleared dot (monotonic merge +
 * monotonic cursors). */
export function seedFromListRows(rows: Thread[]): void {
  let touched = false;
  for (const row of rows) {
    if (typeof row?.latest_run_seq !== 'number' || row.latest_run_seq <= 0) {
      continue;
    }
    const tid = row.thread_id;
    if (!tid) continue;
    touched = true;
    mergeFeedObservation(tid, {
      runId: row.latest_run_id ?? undefined,
      runSeq: row.latest_run_seq,
      status: (row.run_status ?? 'idle') as PublicRunStatus,
      interruptReason: row.interrupt_reason ?? null,
    });
    const entry = ensure(tid);
    const seen = typeof row.last_seen_run_seq === 'number' ? row.last_seen_run_seq : 0;
    if (seen > entry.lastSeenSeq) entry.lastSeenSeq = seen;
  }
  if (touched) recompute();
}

/** Authoritative cursors from a /seen POST (or an optimistic local raise). */
export function applySeenCursors(
  threadId: string,
  cursors: { last_seen_run_seq?: number },
): void {
  const entry = ensure(threadId);
  const seen = cursors.last_seen_run_seq ?? 0;
  if (seen <= entry.lastSeenSeq) return;
  entry.lastSeenSeq = seen;
  recompute();
}

/** Optimistic seen: opening a thread clears its dot for this session even if
 * the /seen POST is lost (the durable stamp self-heals on the next open). */
export function raiseSeenToEffective(threadId: string): void {
  const entry = state.threads.get(threadId);
  if (!entry) return;
  const eff = effective(entry);
  if (!eff || typeof eff.runSeq !== 'number') return;
  if (eff.runSeq <= entry.lastSeenSeq) return;
  if (!TERMINAL_FAMILY.has(eff.status)) return;
  entry.lastSeenSeq = eff.runSeq;
  recompute();
}

export function pruneThread(threadId: string): void {
  if (!state.threads.delete(threadId)) return;
  recompute();
}

export function getEffectiveObservation(
  threadId: string,
): LifecycleObservation | undefined {
  const entry = state.threads.get(threadId);
  return entry ? effective(entry) : undefined;
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

function subscribe(fn: () => void): () => void {
  return state.emitter.subscribe(fn);
}

/** Per-thread lifecycle flags as primitives. Rows must subscribe here — never
 * to a whole set — so a run event re-renders only the row it belongs to, not
 * every row in every mounted list. */
export function useThreadRunning(threadId: string): boolean {
  return useSyncExternalStore(subscribe, () => state.running.has(threadId));
}

/** The same `running` set useThreadRunning subscribes to, read imperatively —
 * for event handlers that must branch on liveness at click time, where a
 * subscription can't be taken conditionally. Never call this during render. */
export function isThreadRunning(threadId: string): boolean {
  return state.running.has(threadId);
}

/** True while the thread's latest run is interrupted (waiting for input). */
export function useThreadNeedsInput(threadId: string): boolean {
  return useSyncExternalStore(subscribe, () => state.needsInput.has(threadId));
}

/** True when the thread finished off-screen and hasn't been viewed yet. */
export function useThreadUnseen(threadId: string): boolean {
  return useSyncExternalStore(subscribe, () => state.unseen.has(threadId));
}

/** The thread's effective public status ('idle' when nothing is known). */
export function useThreadRunStatus(threadId: string): PublicRunStatus {
  return useSyncExternalStore(
    subscribe,
    () => state.effStatus.get(threadId) ?? 'idle',
  );
}

/** Status ordinals for the packed snapshot below; append-only (the packed
 *  value never leaves a render pass, so the order carries no wire meaning). */
const PACKED_STATUSES: readonly PublicRunStatus[] = [
  'idle', 'queued', 'running', 'stopping', 'recovering',
  'completed', 'interrupted', 'failed', 'cancelled',
];

export interface ThreadFlags {
  isRunning: boolean;
  needsInput: boolean;
  isUnseen: boolean;
  status: PublicRunStatus;
}

function packThreadFlags(threadId: string): number {
  const status = state.effStatus.get(threadId) ?? 'idle';
  return (
    (state.running.has(threadId) ? 1 : 0) |
    (state.needsInput.has(threadId) ? 2 : 0) |
    (state.unseen.has(threadId) ? 4 : 0) |
    (Math.max(0, PACKED_STATUSES.indexOf(status)) << 3)
  );
}

/**
 * All four per-thread indicators from ONE subscription. A row that reads them
 * separately pays four `useSyncExternalStore` subscriptions and four snapshot
 * comparisons per store emit; packing them into a single integer snapshot
 * makes that one. Still per-thread — never subscribe a row to a whole set.
 */
export function useThreadFlags(threadId: string): ThreadFlags {
  const packed = useSyncExternalStore(subscribe, () => packThreadFlags(threadId));
  return useMemo(
    () => ({
      isRunning: (packed & 1) !== 0,
      needsInput: (packed & 2) !== 0,
      isUnseen: (packed & 4) !== 0,
      status: PACKED_STATUSES[packed >> 3] ?? 'idle',
    }),
    [packed],
  );
}

// Logout / account switch / test isolation.
export function resetThreadLifecycle(): void {
  state.threads = new Map();
  state.activeThreadId = null;
  state.running = new Set();
  state.needsInput = new Set();
  state.unseen = new Set();
  state.effStatus = new Map();
  notify();
}
