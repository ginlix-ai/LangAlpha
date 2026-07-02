import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { getDispatchLiveness, type DispatchLiveness } from '../utils/api';

/** UI-facing lifecycle of a dispatched PTC research run. */
export type PTCDispatchStatus =
  | 'starting'
  | 'running'
  | 'needs_input'
  | 'completed'
  | 'failed'
  | 'stopped';

const TERMINAL: ReadonlySet<PTCDispatchStatus> = new Set(['completed', 'failed', 'stopped']);

// A dispatched run that never registers (e.g. a continuation rejected by the
// per-user cap) stays 'starting' on every poll. Stop after this many successful
// polls so the turn doesn't watch a genuinely stuck dispatch forever. A real run
// flips to 'running' within seconds, so this only trips on a stuck dispatch.
const STARTING_POLL_CAP = 30;
// The batched liveness read is cheap, but a flash turn can register several
// dispatched runs at once, so stay responsive for the first few polls (quick
// completion / HITL prompt) then back off — a dispatched analysis runs for
// minutes.
const POLL_FAST_MS = 4_000;
const POLL_STEADY_MS = 10_000;
const POLL_FAST_COUNT = 5;

/** Map the backend WorkflowStatus enum onto the card's status. `unknown`/absent
 *  means the run hasn't registered yet, so we show "starting" and keep polling. */
function mapStatus(raw: unknown): PTCDispatchStatus {
  switch (raw) {
    case 'active': return 'running';
    case 'interrupted': return 'needs_input';
    case 'completed': return 'completed';
    case 'failed': return 'failed';
    case 'cancelled': return 'stopped';
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
function aggregateStatus(statuses: PTCDispatchStatus[]): PTCDispatchStatus {
  if (statuses.some((s) => s === 'running' || s === 'needs_input')) return 'running';
  if (statuses.some((s) => s === 'starting')) return 'starting';
  return 'completed';
}

interface DispatchStatusContextValue {
  register: (threadId: string) => void;
  unregister: (threadId: string) => void;
  /** Per-thread resolved dispatch status, distributed via context. */
  slices: Map<string, PTCDispatchStatus>;
}

const DispatchStatusContext = createContext<DispatchStatusContextValue | null>(null);

/**
 * Runs ONE batched dispatch-liveness query for every PTC card in a flash turn.
 * Cards register/unregister their thread id; the provider polls the sorted id
 * set once and distributes a per-id status slice via context — collapsing N
 * per-card `/status` polls into a single request on a single timer.
 */
export function DispatchStatusProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [ids, setIds] = useState<string[]>([]);
  // Ref-count registrations so two cards on the same thread share one entry and
  // one card unmounting doesn't drop an id another card still watches.
  const countsRef = useRef<Map<string, number>>(new Map());
  // Latest distributed slices, readable from the stable `register` callback.
  const slicesRef = useRef<Map<string, PTCDispatchStatus>>(new Map());
  // Poll-cadence counters scoped to the CURRENT polling window. dataUpdateCount
  // is cumulative for the query's life, so deriving cadence from it directly
  // makes the starting cap trip instantly (and skips the fast window) on any
  // later dispatch; these reset whenever polling re-arms instead.
  const cadenceRef = useRef({ polls: 0, startingRounds: 0, lastUpdateCount: 0 });

  const register = useCallback((threadId: string) => {
    const counts = countsRef.current;
    const next = (counts.get(threadId) ?? 0) + 1;
    counts.set(threadId, next);
    if (next === 1) {
      setIds((prev) => (prev.includes(threadId) ? prev : [...prev, threadId]));
      return;
    }
    // A re-registration on an id whose run already finished is a RESUMED
    // dispatch: the id set (and thus the query key) is unchanged, so the
    // dormant query would keep serving the stale terminal slice forever.
    // Re-arm the cadence window and wake every id-set variant. Terminal-gated
    // so re-renders during live polling never add fetches.
    const current = slicesRef.current.get(threadId);
    if (current && TERMINAL.has(current)) {
      cadenceRef.current.polls = 0;
      cadenceRef.current.startingRounds = 0;
      void queryClient.invalidateQueries({ queryKey: queryKeys.threads.dispatchLivenessAll() });
    }
  }, [queryClient]);

  const unregister = useCallback((threadId: string) => {
    const counts = countsRef.current;
    const next = (counts.get(threadId) ?? 0) - 1;
    if (next > 0) {
      counts.set(threadId, next);
      return;
    }
    counts.delete(threadId);
    setIds((prev) => prev.filter((id) => id !== threadId));
  }, []);

  const sortedIds = useMemo(() => [...ids].sort(), [ids]);

  // A changed id set is a NEW cache entry (dataUpdateCount restarts at 0), so
  // the cadence window restarts with it.
  useEffect(() => {
    cadenceRef.current = { polls: 0, startingRounds: 0, lastUpdateCount: 0 };
  }, [sortedIds]);

  const { data } = useQuery({
    queryKey: queryKeys.threads.dispatchLiveness(sortedIds),
    queryFn: () => getDispatchLiveness(sortedIds),
    enabled: sortedIds.length > 0,
    staleTime: 2_000,
    refetchInterval: (query) => {
      const rows = (query.state.data ?? []) as DispatchLiveness[];
      const byId = new Map(rows.map((r) => [r.thread_id, r]));
      // Omitted ids map to 'starting' (keep watching), matching mapStatus.
      const statuses = sortedIds.map((id) => mapStatus(byId.get(id)?.status));
      const aggregate = aggregateStatus(statuses);
      // Advance window-scoped counters once per fetch RESULT (deduped on the
      // cumulative dataUpdateCount — this callback re-evaluates more often).
      const cadence = cadenceRef.current;
      if (query.state.dataUpdateCount !== cadence.lastUpdateCount) {
        cadence.lastUpdateCount = query.state.dataUpdateCount;
        cadence.polls += 1;
        cadence.startingRounds = aggregate === 'starting' ? cadence.startingRounds + 1 : 0;
      }
      return nextDispatchPollInterval(aggregate, cadence.polls, cadence.startingRounds);
    },
  });

  const slices = useMemo(() => {
    const map = new Map<string, PTCDispatchStatus>();
    for (const row of (data ?? []) as DispatchLiveness[]) {
      map.set(row.thread_id, mapStatus(row.status));
    }
    return map;
  }, [data]);

  useEffect(() => {
    slicesRef.current = slices;
  }, [slices]);

  const value = useMemo<DispatchStatusContextValue>(
    () => ({ register, unregister, slices }),
    [register, unregister, slices],
  );

  return createElement(DispatchStatusContext.Provider, { value }, children);
}

/**
 * Read one dispatched thread's liveness from the shared DispatchStatusProvider.
 * Registers the id while `enabled` so the provider's batched query covers it;
 * returns 'starting' when no provider is mounted or the id hasn't resolved yet.
 */
export function useDispatchStatus(
  threadId: string | undefined,
  enabled: boolean,
): { status: PTCDispatchStatus } {
  const ctx = useContext(DispatchStatusContext);
  // Pull the stable register fns out of the context value (whose identity
  // changes every poll) so this effect doesn't re-run — and re-register — on
  // each distribution.
  const register = ctx?.register;
  const unregister = ctx?.unregister;
  const active = enabled && !!threadId;

  useEffect(() => {
    if (!register || !unregister || !active || !threadId) return;
    register(threadId);
    return () => unregister(threadId);
  }, [register, unregister, active, threadId]);

  return { status: (ctx && threadId ? ctx.slices.get(threadId) : undefined) ?? 'starting' };
}
