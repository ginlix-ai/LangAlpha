import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query';
import { useStableArray } from '@/hooks/useStableArray';
import { epochMs } from '@/lib/elapsed';
import { queryKeys } from '@/lib/queryKeys';
import { useThreadRunStatuses, type PublicRunStatus } from '@/lib/threadLifecycle/store';
import type { ReportOutcome } from '@/types/api';
import {
  getHandoffLiveness,
  type DispatchSteerLiveness,
  type HandoffLiveness,
  type LivenessAsk,
} from '../utils/api';
import { isLastingRefusal } from '@/api/errors';

/** UI-facing lifecycle of a dispatched PTC research run. */
export type PTCDispatchStatus =
  | 'starting'
  | 'running'
  | 'needs_input'
  | 'completed'
  | 'failed'
  | 'stopped';

/** A hand-off's status as the reads resolve it: its run's, or `dropped` for a
 *  steer no run took in, which started nothing. Kept out of
 *  `PTCDispatchStatus`, every member of which the status surfaces draw. */
export type DispatchReadStatus = PTCDispatchStatus | 'dropped';

const TERMINAL: ReadonlySet<DispatchReadStatus> = new Set(['completed', 'failed', 'stopped', 'dropped']);

// A dispatched run that never registers (e.g. a continuation rejected by the
// per-user cap) stays 'starting' on every poll. Stop after this many polls so
// the turn doesn't watch a genuinely stuck dispatch forever. A real run flips to
// 'running' within seconds, so this only trips on a stuck dispatch, or on a read
// that keeps failing before it ever saw the run.
const STARTING_POLL_CAP = 30;
// The batched liveness read is cheap, but a Chief of Staff turn can register
// several dispatched runs at once, so stay responsive for the first few polls
// (quick completion / HITL prompt) then back off: a dispatched analysis runs
// for minutes.
const POLL_FAST_MS = 4_000;
const POLL_STEADY_MS = 10_000;
const POLL_FAST_COUNT = 5;

/** Map the backend's public status vocabulary (`WorkflowRunStatus`) onto the
 *  card's status. The cases enumerate that union's members so the mapping stays
 *  exhaustive: `idle`/`queued`/`recovering` (registered but no output yet) show
 *  "starting" and keep polling; `stopping` is still a live run. `raw` stays
 *  `unknown` — the wire boundary is untrusted, so the default absorbs anything
 *  off-vocabulary, including legacy pre-1.6 spellings (`active`). */
export function mapLivenessStatus(raw: unknown): PTCDispatchStatus {
  switch (raw) {
    case 'running':
    case 'stopping':
    case 'active': return 'running';
    case 'interrupted': return 'needs_input';
    case 'completed': return 'completed';
    case 'failed': return 'failed';
    case 'cancelled': return 'stopped';
    case 'idle':
    case 'queued':
    case 'recovering': return 'starting';
    default: return 'starting';
  }
}

/**
 * Pure poll-cadence decision for the dispatch-status query. Both counters are
 * scoped to the current polling window (reset when polling re-arms on a wake),
 * not the query's lifetime: `pollCount` drives the fast→steady cadence,
 * `startingRounds` counts CONSECUTIVE still-'starting' results toward the cap.
 * Returns the next refetch interval in ms, or `false` to stop polling.
 * Exported for tests.
 */
export function nextDispatchPollInterval(
  status: PTCDispatchStatus,
  pollCount: number,
  startingRounds: number = pollCount,
): number | false {
  if (TERMINAL.has(status)) return false;
  if (status === 'starting' && startingRounds >= STARTING_POLL_CAP) return false;
  return pollCount < POLL_FAST_COUNT ? POLL_FAST_MS : POLL_STEADY_MS;
}

/**
 * Collapse a turn's per-run statuses into the single status that drives the
 * shared poll cadence: any in-flight run (running/needs_input) keeps the
 * fast→steady cadence alive; a still-`starting`-only set polls until the
 * starting cap then gives up; an all-terminal set stops.
 */
function aggregateStatus(statuses: DispatchReadStatus[]): PTCDispatchStatus {
  if (statuses.some((s) => s === 'running' || s === 'needs_input')) return 'running';
  if (statuses.some((s) => s === 'starting')) return 'starting';
  return 'completed';
}

/** How a report says the run it reports on ended. */
export function mapReportOutcome(outcome: ReportOutcome): PTCDispatchStatus {
  switch (outcome) {
    case 'completed': return 'completed';
    case 'error': return 'failed';
    case 'cancelled': return 'stopped';
  }
}

/** Where a run stands, as a read or the feed has it. */
export interface RunRead<S extends DispatchReadStatus = PTCDispatchStatus> {
  status: S;
  /** When the run started (epoch ms), when the ledger has it. */
  startedAt?: number;
  /** When it settled (epoch ms); absent while it is live. */
  finishedAt?: number;
}

/** Where a hand-off's run stands, as the reads resolve it. */
type DispatchRead = RunRead<DispatchReadStatus>;

/** What a read says of a thread beside its latest run. */
interface ThreadNames {
  /** The thread's title, once it is named. */
  title?: string;
  /** The current name of the thread's workspace. */
  workspaceName?: string;
}

/** A thread's latest run, and what the same read says of the thread. */
type ThreadRead = RunRead & ThreadNames;

/**
 * What pins a hand-off's run on its thread. The run its report is on, else the
 * one its dispatch named. For a hand-off steered into a run already going,
 * until a report names its run, its steer: the run that takes the steer in is
 * the hand-off's own, since the one it was steered into can end without
 * reading it, and stands in only for a steer the read does not know. With
 * neither, the thread's latest run; for a dispatch whose outcome is unknown,
 * `since` is when it was made (epoch ms), and a run the thread started before
 * then is not the hand-off's.
 */
export type DispatchPin =
  | { kind: 'run'; runId: string }
  | { kind: 'steer'; steerId: string; steeredInto: string }
  | { kind: 'thread'; since?: number };

/** What a hand-off watches: its thread, and what pins its run there. */
export interface DispatchWatch {
  threadId: string;
  pin: DispatchPin;
  /** How the report that answers the hand-off says its run ended. Not read
   *  by the poll, which goes on until it has that run's times. */
  outcome?: ReportOutcome;
}

/** A hand-off's own run as a read has it: `queued` while its steer waits for
 *  a run to take it in, `dropped` once none will, and undefined while there is
 *  no run of its own to read. */
type OwnRead = RunRead | 'queued' | 'dropped' | undefined;

/** Whether the hand-off's own run is over, so the read alone says how it ended. */
const ownOver = (own: OwnRead) => own === 'dropped' || (typeof own === 'object' && TERMINAL.has(own.status));

/**
 * Whether the thread's latest run can be a hand-off made at `since`: one it
 * started, or one still live then, which a hand-off that steered in joined.
 * A run already over by then cannot be.
 */
function liveSince(thread: ThreadRead | undefined, since: number): boolean {
  if (thread?.startedAt === undefined) return false;
  if (thread.startedAt >= since) return true;
  return thread.finishedAt === undefined ? !TERMINAL.has(thread.status) : thread.finishedAt >= since;
}

/**
 * Which run describes a hand-off, and so both its status and its times, first
 * match wins. A report that answers it says how its run ended, whatever the
 * reads have caught up to; the times are its own run's, or the thread's latest
 * once that ended as the report says. Its own run once that is over: the
 * hand-off ended there, whatever the thread does later, and a steer dropped
 * ended it with no run at all. A dispatch of unknown outcome waits for a run
 * the thread had live once it was made. A steer still queued has no run of its
 * own, and the thread's live run is not ours: it is the run the steer went
 * into, which reads it next, so the hand-off is Working while that run is live
 * and Starting after, until a later run takes the steer in. Else the live run:
 * the thread's latest, which the feed reaches first, timed by its own run when
 * the read has it. An interrupted run is answered by a new run on the thread,
 * so a hand-off whose run stopped to ask follows the thread.
 */
function resolveDispatchRead(
  watch: DispatchWatch,
  own: OwnRead,
  thread: ThreadRead | undefined,
  pushed: PTCDispatchStatus | null,
): DispatchRead {
  const run = typeof own === 'object' ? own : undefined;
  if (watch.outcome) {
    const status = mapReportOutcome(watch.outcome);
    const timed = run ?? (thread?.status === status ? thread : undefined);
    return { status, startedAt: timed?.startedAt, finishedAt: timed?.finishedAt };
  }
  if (own === 'dropped') return { status: 'dropped' };
  if (run && TERMINAL.has(run.status)) {
    return { status: run.status, startedAt: run.startedAt, finishedAt: run.finishedAt };
  }
  const { pin } = watch;
  if (pin.kind === 'thread' && pin.since !== undefined && !liveSince(thread, pin.since)) {
    return { status: 'starting' };
  }
  const status = pushed ?? thread?.status ?? 'starting';
  if (own === 'queued' && TERMINAL.has(status)) return { status: 'starting' };
  if (run && run.status !== 'needs_input') {
    return { status, startedAt: run.startedAt, finishedAt: run.finishedAt };
  }
  // The read's times are the run it read, the thread's latest then. A live
  // read is the same run a push has since settled; a settled one the push has
  // moved on from is an earlier run, whose start would time the new one.
  const timed = !!thread && (thread.status === 'running' || thread.status === status);
  return {
    status,
    startedAt: timed ? thread.startedAt : undefined,
    finishedAt: timed ? thread.finishedAt : undefined,
  };
}

/** One read as the provider caches it, with what it asked: an id asked
 *  about and absent has no run yet, which is an answer too. */
interface LivenessRead extends HandoffLiveness {
  asked: LivenessAsk;
}

interface ReadSlices {
  threads: Map<string, ThreadRead>;
  runs: Map<string, RunRead>;
  steers: Map<string, DispatchSteerLiveness>;
  asked: LivenessAsk;
}

const NOTHING_ASKED: LivenessAsk = { threads: [], runs: [], steers: [] };

const runReadOf = (row: { status: string; run_started_at?: string | null; run_finished_at?: string | null }): RunRead => ({
  status: mapLivenessStatus(row.status),
  startedAt: epochMs(row.run_started_at) ?? undefined,
  finishedAt: epochMs(row.run_finished_at) ?? undefined,
});

function slicesOf(read: LivenessRead | undefined): ReadSlices {
  const threads = new Map<string, ThreadRead>();
  for (const row of read?.liveness ?? []) {
    threads.set(row.thread_id, {
      ...runReadOf(row),
      title: row.title?.trim() || undefined,
      workspaceName: row.workspace_name?.trim() || undefined,
    });
  }
  const runs = new Map<string, RunRead>();
  for (const row of read?.runs ?? []) runs.set(row.run_id, runReadOf(row));
  const steers = new Map<string, DispatchSteerLiveness>();
  for (const row of read?.steers ?? []) steers.set(row.steer_id, row);
  return { threads, runs, steers, asked: read?.asked ?? NOTHING_ASKED };
}

/**
 * A hand-off's own run as a read has it. A taken steer's is the run that took
 * it in, which the read carries whether it was asked for or not. A steer the
 * read does not know (purged, or never the user's) leaves the run it was
 * steered into.
 */
function ownRead({ pin }: DispatchWatch, read: ReadSlices): OwnRead {
  if (pin.kind === 'thread') return undefined;
  if (pin.kind === 'run') return read.runs.get(pin.runId);
  const steer = read.steers.get(pin.steerId);
  if (!steer) return read.runs.get(pin.steeredInto);
  if (steer.state === 'taken' && steer.run_id) return read.runs.get(steer.run_id);
  return steer.state === 'dropped' ? 'dropped' : 'queued';
}

/** The runs and steers a pin is read by. A steer pin asks for the run it was
 *  steered into as well, so the fallback for a steer the read does not know
 *  needs no second read. */
function pinnedIds(pin: DispatchPin): { runs: string[]; steers: string[] } {
  switch (pin.kind) {
    case 'run': return { runs: [pin.runId], steers: [] };
    case 'steer': return { runs: [pin.steeredInto], steers: [pin.steerId] };
    case 'thread': return { runs: [], steers: [] };
  }
}

const sortedSet = (values: Iterable<string>) => [...new Set(values)].sort();

/** What the watches ask one read, each list sorted and deduped. */
function askOf(watches: readonly DispatchWatch[]): LivenessAsk {
  const pinned = watches.map((w) => pinnedIds(w.pin));
  return {
    threads: sortedSet(watches.map((w) => w.threadId)),
    runs: sortedSet(pinned.flatMap((p) => p.runs)),
    steers: sortedSet(pinned.flatMap((p) => p.steers)),
  };
}

/** Whether a read asked about everything a pin is read by. */
function askedFor(asked: LivenessAsk, pin: DispatchPin): boolean {
  const { runs, steers } = pinnedIds(pin);
  return runs.every((id) => asked.runs.includes(id)) && steers.every((id) => asked.steers.includes(id));
}

interface DispatchStatusContextValue {
  register: (watch: DispatchWatch) => void;
  unregister: (watch: DispatchWatch) => void;
  /** Read again now: a push said the thread's run settled, and the last read
   *  does not say when. */
  settle: (threadId: string) => void;
  /** The resolved reads, distributed via context. */
  read: ReadSlices;
}

const DispatchStatusContext = createContext<DispatchStatusContextValue | null>(null);

/** What the poll reads for a watch; the outcome is not part of it. */
const watchKey = ({ threadId, pin }: DispatchWatch) => JSON.stringify([threadId, pin]);

/**
 * Runs ONE batched dispatch-liveness query for every hand-off in a Chief of
 * Staff turn. Each hand-off registers what it watches (its thread, and what
 * pins its run there); the provider asks one read about all of them and
 * distributes a slice per thread, run and steer via context, collapsing N
 * per-card `/status` polls into a single request on a single timer.
 */
export function DispatchStatusProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [watchMap, setWatchMap] = useState<ReadonlyMap<string, DispatchWatch>>(() => new Map());
  // Ref-count registrations so two cards on the same hand-off share one entry
  // and one card unmounting doesn't drop a watch another card still holds.
  const countsRef = useRef<Map<string, number>>(new Map());
  // How many distinct watches each thread has, for `register`.
  const threadWatchesRef = useRef<Map<string, number>>(new Map());
  // Latest distributed slices, readable from the stable `register` callback.
  const slicesRef = useRef<Map<string, ThreadRead>>(new Map());
  // Poll-cadence counters scoped to the CURRENT polling window. The query's
  // update counts are cumulative for its life, so deriving cadence from them
  // directly makes the starting cap trip instantly (and skips the fast window)
  // on any later dispatch; these reset whenever polling re-arms instead.
  const cadenceRef = useRef({ polls: 0, startingRounds: 0, lastSettledCount: 0 });

  const register = useCallback((watch: DispatchWatch) => {
    const key = watchKey(watch);
    const counts = countsRef.current;
    const perThread = threadWatchesRef.current;
    const threadWatched = (perThread.get(watch.threadId) ?? 0) > (counts.has(key) ? 1 : 0);
    const next = (counts.get(key) ?? 0) + 1;
    counts.set(key, next);
    if (next === 1) {
      perThread.set(watch.threadId, (perThread.get(watch.threadId) ?? 0) + 1);
      const { threadId, pin } = watch;
      setWatchMap((prev) => (prev.has(key) ? prev : new Map(prev).set(key, { threadId, pin })));
    }
    // A hand-off pinned only to its thread registering on a thread already
    // read, whose run already finished, is a RESUMED dispatch: the ask (and
    // thus the query key) is unchanged, so the dormant query would keep
    // serving the stale terminal slice forever. Re-arm the cadence window and
    // wake every ask variant. Terminal-gated so re-renders during live polling
    // never add fetches. A run or steer of its own is a new ask, read on its own.
    if (watch.pin.kind !== 'thread' || (next === 1 && !threadWatched)) return;
    const current = slicesRef.current.get(watch.threadId)?.status;
    if (current && TERMINAL.has(current)) {
      cadenceRef.current.polls = 0;
      cadenceRef.current.startingRounds = 0;
      void queryClient.invalidateQueries({ queryKey: queryKeys.threads.dispatchLivenessAll() });
    }
  }, [queryClient]);

  const unregister = useCallback((watch: DispatchWatch) => {
    const key = watchKey(watch);
    const counts = countsRef.current;
    const next = (counts.get(key) ?? 0) - 1;
    if (next > 0) {
      counts.set(key, next);
      return;
    }
    counts.delete(key);
    const perThread = threadWatchesRef.current;
    const left = (perThread.get(watch.threadId) ?? 1) - 1;
    if (left > 0) perThread.set(watch.threadId, left);
    else perThread.delete(watch.threadId);
    setWatchMap((prev) => {
      if (!prev.has(key)) return prev;
      const rest = new Map(prev);
      rest.delete(key);
      return rest;
    });
  }, []);

  const watches = useMemo(() => [...watchMap.values()], [watchMap]);
  // Serialized first, so a new watch on a thread and run already read keeps
  // the ask, and the query key, as it was.
  const askJoined = JSON.stringify(askOf(watches));
  const ask = useMemo(() => JSON.parse(askJoined) as LivenessAsk, [askJoined]);
  // What the query reads, for `settle`: a refetch runs even while the query
  // is disabled, so asking before a thread's id is read would send a read
  // without it, and the read its registration starts answers it anyway.
  const askRef = useRef<LivenessAsk>(ask);
  useEffect(() => {
    askRef.current = ask;
  }, [ask]);

  // A changed ask is a NEW cache entry (its settle count restarts at 0), so
  // the cadence window restarts with it.
  useEffect(() => {
    cadenceRef.current = { polls: 0, startingRounds: 0, lastSettledCount: 0 };
  }, [ask]);

  const { data, refetch } = useQuery({
    queryKey: queryKeys.threads.dispatchLiveness(ask),
    queryFn: async (): Promise<LivenessRead> => ({ asked: ask, ...(await getHandoffLiveness(ask)) }),
    enabled: ask.threads.length > 0,
    staleTime: 2_000,
    // A new hand-off is a new key; the others keep what they read meanwhile
    // rather than drop back to 'starting' for a round trip.
    placeholderData: keepPreviousData,
    refetchInterval: (query) => {
      const slices = slicesOf(query.state.data as LivenessRead | undefined);
      // Each hand-off as the poll alone resolves it: one whose own run is
      // over keeps nothing alive, whatever its thread runs since, and nor does
      // a steer dropped. Omitted ids map to 'starting' (keep watching),
      // matching mapLivenessStatus.
      const statuses = watches.map((w) =>
        resolveDispatchRead(w, ownRead(w, slices), slices.threads.get(w.threadId), null).status);
      const aggregate = aggregateStatus(statuses);
      // Advance window-scoped counters once per finished fetch, deduped on the
      // cumulative settle count since this callback re-evaluates more often.
      // A failure counts too: the query keeps the last answer, so the round
      // reads as that answer did. Counting only successes held a failing
      // endpoint on the fast cadence forever.
      const cadence = cadenceRef.current;
      const settled = query.state.dataUpdateCount + query.state.errorUpdateCount;
      if (settled !== cadence.lastSettledCount) {
        cadence.lastSettledCount = settled;
        cadence.polls += 1;
        cadence.startingRounds = aggregate === 'starting' ? cadence.startingRounds + 1 : 0;
      }
      // A 403 answers the same next time. A 401 is an expired session that
      // can come back, so it only backs off.
      if (isLastingRefusal(query.state.error)) return false;
      return nextDispatchPollInterval(aggregate, cadence.polls, cadence.startingRounds);
    },
    // The app's one retry, except for the refusal that stops the poll above.
    retry: (failureCount, err) => failureCount < 1 && !isLastingRefusal(err),
  });

  const read = useMemo(() => slicesOf(data), [data]);

  useEffect(() => {
    slicesRef.current = read.threads;
  }, [read]);

  // A push can settle a run before the poll reads it, and the poll stops once
  // its own read is terminal, so a push that settles a run gets one read of
  // its own. Joining a read already in flight keeps N cards on one thread to
  // one request.
  const settle = useCallback((threadId: string) => {
    if (askRef.current.threads.includes(threadId)) void refetch({ cancelRefetch: false });
  }, [refetch]);

  const value = useMemo<DispatchStatusContextValue>(
    () => ({ register, unregister, settle, read }),
    [register, unregister, settle, read],
  );

  // JSX, not createElement: the compiler reads a plain call that is handed
  // the ref-backed callbacks as a ref read in render, and skips the provider.
  return <DispatchStatusContext value={value}>{children}</DispatchStatusContext>;
}

/** Lifecycle-store status → card status; null while the store has nothing
 *  ('idle') so the pre-ledger dispatch window falls through to the poll. */
export function mapLifecycleStatus(s: PublicRunStatus): PTCDispatchStatus | null {
  switch (s) {
    case 'running':
    case 'stopping': return 'running';
    case 'interrupted': return 'needs_input';
    case 'completed': return 'completed';
    case 'failed': return 'failed';
    case 'cancelled': return 'stopped';
    case 'queued':
    case 'recovering': return 'starting';
    default: return null;
  }
}

/** A hand-off's status as `useDispatchStatuses` reads it. */
export interface DispatchStatusView extends DispatchRead, ThreadNames {
  /** Whether a read, the feed or a report has spoken yet, or nothing is
   *  watched. Until then the status is the default, not news. */
  read: boolean;
}

const UNWATCHED: DispatchStatusView = { status: 'starting', read: true };

const sameWatches = (a: (DispatchWatch | null)[], b: (DispatchWatch | null)[]) =>
  a.length === b.length && a.every((w, i) => (w && watchKey(w)) === (b[i] && watchKey(b[i])));

/** What one watch's settle check turns on; `due` once a push settled a run
 *  the last read does not say is over. */
interface SettleAsk {
  key: string;
  threadId: string;
  pushed: PTCDispatchStatus | null;
  due: boolean;
  finishKnown: boolean;
}

const sameAsks = (a: (SettleAsk | null)[], b: (SettleAsk | null)[]) =>
  a.length === b.length && a.every((x, i) => {
    const y = b[i];
    return x === y || (!!x && !!y && x.key === y.key && x.pushed === y.pushed && x.due === y.due && x.finishKnown === y.finishKnown);
  });

/**
 * Read hand-offs' liveness, one view per watch; a null watch reads nothing.
 * The lifecycle store (user feed) is the primary source for a thread once its
 * run is ledger-known: settles arrive as pushes, not on the next poll tick.
 * The provider's batched poll is the pre-ledger fallback: a just-dispatched
 * run has no lifecycle row yet, and only the poll can distinguish "still
 * registering" from "never registered". A pinned run is read by its id too,
 * and a steer by its id, which names the run that took it in; once that run
 * is over, or the steer was dropped, the read alone says how the hand-off
 * ended, unless a report already has (see `resolveDispatchRead`). The
 * thread's title and its workspace's current name come from the poll.
 */
export function useDispatchStatuses(watches: (DispatchWatch | null)[]): DispatchStatusView[] {
  const ctx = useContext(DispatchStatusContext);
  // The stable register fns, not the context value (whose identity changes
  // every poll), so the effects don't re-register on each distribution.
  const register = ctx?.register;
  const unregister = ctx?.unregister;
  const settle = ctx?.settle;
  const stable = useStableArray(watches, sameWatches);

  // Registers what was added and drops what went, so a watch that stays is
  // not dropped and taken again, which on a thread already read would read
  // as a resumed dispatch.
  const heldRef = useRef<Map<string, DispatchWatch>>(new Map());
  useEffect(() => {
    if (!register || !unregister) return;
    const held = heldRef.current;
    const next = new Map<string, DispatchWatch>();
    for (const w of stable) if (w) next.set(watchKey(w), w);
    for (const [key, w] of next) if (!held.has(key)) register(w);
    for (const [key, w] of held) if (!next.has(key)) unregister(w);
    heldRef.current = next;
  }, [register, unregister, stable]);
  useEffect(() => () => {
    heldRef.current.forEach((w) => unregister?.(w));
    heldRef.current = new Map();
  }, [unregister]);

  const lifecycle = useThreadRunStatuses(watches.map((w) => w?.threadId ?? ''));
  const asks: (SettleAsk | null)[] = [];
  const views = watches.map((watch, i) => {
    if (!watch) {
      asks.push(null);
      return UNWATCHED;
    }
    const { threadId, pin } = watch;
    const pushed = mapLifecycleStatus(lifecycle[i] ?? 'idle');
    // Until a read asks about the pinned run and the steer, the one on hand is
    // kept from before the pin, and its thread may be on an earlier run, or
    // on the run the steer missed.
    const current = !!ctx && askedFor(ctx.read.asked, pin);
    const named = ctx?.read.threads.get(threadId);
    const thread = current ? named : undefined;
    const own = ctx && current ? ownRead(watch, ctx.read) : undefined;
    const resolved = resolveDispatchRead(watch, own, thread, pushed);
    // A push that settled the run, when the last read cannot say when: the
    // read was live, or of another run (the push moved here while this view
    // watched). A pinned run that is over, or a steer dropped, needs nothing
    // more.
    asks.push(ctx ? {
      key: watchKey(watch),
      threadId,
      pushed,
      due: !ownOver(own) && pushed !== null && TERMINAL.has(pushed),
      finishKnown: resolved.finishedAt !== undefined && resolved.status === pushed,
    } : null);
    return {
      ...resolved,
      title: named?.title,
      workspaceName: named?.workspaceName,
      read: !ctx || !!watch.outcome || pushed !== null || (current && ctx.read.asked.threads.includes(threadId)),
    };
  });

  // Each watch's check runs when what it turns on changes, as its own effect
  // would: the feed's status moving is a settle to read, and so is one the
  // read still cannot time.
  const settleAsks = useStableArray(asks, sameAsks);
  const seenRef = useRef<Map<string, SettleAsk>>(new Map());
  useEffect(() => {
    const seen = seenRef.current;
    const next = new Map<string, SettleAsk>();
    for (const ask of settleAsks) {
      if (!ask) continue;
      next.set(ask.key, ask);
      const before = seen.get(ask.key);
      if (before && sameAsks([before], [ask])) continue;
      const moved = !!before && before.pushed !== ask.pushed;
      if (settle && ask.due && (moved || !ask.finishKnown)) settle(ask.threadId);
    }
    seenRef.current = next;
  }, [settle, settleAsks]);

  return views;
}
