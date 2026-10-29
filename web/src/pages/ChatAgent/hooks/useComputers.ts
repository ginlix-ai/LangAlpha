/**
 * Computer list query and the status fan-out.
 *
 * A machine is what actually starts and stops; its workspaces are folders on
 * it. So a surface showing many workspaces subscribes once per machine and
 * fans the transition out, instead of opening one connection per project to
 * hear the same news several times.
 */
import { useCallback, useEffect, useMemo, useSyncExternalStore } from 'react';

import { QueryClient, queryOptions, useQuery, useQueryClient } from '@tanstack/react-query';

import { registerAuthReset } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ComputerStorage, ComputersResponse, SshPairingStatus } from '@/types/api';

import {
  createSshPairing,
  getComputerProviders,
  getComputerStorage,
  getComputers,
  getSshKey,
  getSshPairing,
  streamComputerEvents,
  streamWorkspaceEvents,
} from '../utils/api';
import { apiErrorStatus } from '@/api/errors';
import {
  cachedWorkspaceLists,
  patchComputerStatusInCaches,
  patchWorkspaceStatusInCaches,
} from '../utils/warmWorkspace';
import type { ComputerRootRow } from '../utils/agentPaths';
import { isComputerStatusTerminal, isComputerStatusTransitional } from '../utils/computerStatus';
import { activeSpecChange } from '../components/specChangeUi';
import { recordPrepareFailure, recordPrepareStage } from './computerPreparation';

/** How often the list is re-read while a spec change runs. */
export const SPEC_CHANGE_POLL_MS = 3000;

/**
 * Reconnect pacing for a status stream that closed mid-transition. The first
 * retry is quick because the common cause is the server's own stream cap; the
 * cap keeps a flapping link from hammering the origin, and the count bounds a
 * machine that never leaves 'starting' to a few minutes of listening.
 */
export const STATUS_RECONNECT = { baseMs: 500, capMs: 15_000, maxAttempts: 20 };

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      signal.removeEventListener('abort', done);
      clearTimeout(timer);
      resolve();
    }
    signal.addEventListener('abort', done, { once: true });
  });
}

/** The machine's status as the list cache holds it right now. */
function cachedComputerStatus(queryClient: QueryClient, computerId: string): string | undefined {
  for (const [, data] of queryClient.getQueriesData<ComputersResponse | undefined>({
    queryKey: queryKeys.computers.lists(),
  })) {
    const row = data?.computers?.find((c) => c.computer_id === computerId);
    if (row) return row.status;
  }
  return undefined;
}

/**
 * Shared computer list. Every consumer with this key reads one cached entry.
 *
 * A spec change runs on the server after its request has answered and no
 * status frame names its outcome, so the list re-reads itself while any row
 * has one in progress. Observers share the timer: every fetch resets them all.
 */
function computersList() {
  return queryOptions({
    queryKey: queryKeys.computers.lists(),
    queryFn: getComputers,
    staleTime: 30_000,
    refetchInterval: (query) =>
      query.state.data?.computers.some((c) => activeSpecChange(c)) ? SPEC_CHANGE_POLL_MS : false,
  });
}

export function useComputers(options: { enabled?: boolean } = {}) {
  return useQuery({ ...computersList(), enabled: options.enabled ?? true });
}

/**
 * One machine off the shared list, for a row that only names it (the sidebar's
 * folders): the row re-renders when that machine changes, not when any other
 * does or the list polls a spec change. It reads what the list already holds
 * rather than refetch on every row that mounts.
 */
export function useComputer(computerId: string | null) {
  return useQuery({
    ...computersList(),
    enabled: !!computerId,
    refetchOnMount: false,
    select: (data) => data.computers.find((c) => c.computer_id === computerId) ?? null,
  });
}

/**
 * The server's list, for a decision made outside render, such as whether a
 * machine is still in use. Never the cached one: that can predate the removal
 * the decision is about, made on another device. Rejects rather than answering
 * with nothing, so a failed read never passes for an empty list.
 */
export function fetchComputerList(queryClient: QueryClient): Promise<ComputersResponse> {
  return queryClient.fetchQuery({ ...computersList(), staleTime: 0 });
}

/** Names the create, so a decision that needs a new row on the list can wait for it to land. */
export const CREATE_COMPUTER_MUTATION_KEY = ['computers', 'create'] as const;

/**
 * Names a reconnect, which moves a computer onto a new host key: turning
 * remote control off once no row is this Mac must wait for it, or it reads
 * the list before the row names the helper's new key.
 */
export const RECONNECT_COMPUTER_MUTATION_KEY = ['computers', 'reconnect'] as const;

/**
 * Every computer's root, by computer, off the shared list: the root its
 * workspace folders sit under. A workspace moves between computers with its
 * folder, so an older turn's absolute path can start at a root it has since
 * left, and the Chief of Staff links a file on another computer at its whole
 * path there. Selected, so a refetch that only moved a status leaves its
 * readers alone.
 */
export function useComputerRoots(options: { enabled: boolean }): readonly ComputerRootRow[] | undefined {
  const { data } = useQuery({
    ...computersList(),
    enabled: options.enabled,
    select: (list) => list.computers.map((c) => ({ computer_id: c.computer_id, root_dir: c.root_dir })),
  });
  return data;
}

// The fetch is wrapped so the api module is read at call time, not at import.
const providersQuery = queryOptions({
  queryKey: queryKeys.computers.providers(),
  queryFn: () => getComputerProviders(),
  staleTime: 60_000,
});

/**
 * The providers a new computer can go on. Read by the create form and by the
 * rows, which name a machine's provider once there is more than one to tell
 * apart. `enabled` is the caller's gate: fetch while a computer surface is open.
 */
export function useComputerProviders(options: { enabled: boolean }) {
  return useQuery({ ...providersQuery, enabled: options.enabled });
}

/** Read the providers ahead of the New computer form, which waits on them. */
export function prefetchComputerProviders(queryClient: QueryClient) {
  return queryClient.prefetchQuery(providersQuery);
}

/**
 * Put a machine the server just created on every cached list, ahead of the
 * refetch, where the refetch will put it: the server lists the primary first,
 * then the most recently active, which a machine just made is.
 */
export function addComputerRow(queryClient: QueryClient, computer: Computer) {
  queryClient.setQueriesData<ComputersResponse | undefined>(
    { queryKey: queryKeys.computers.lists() },
    (prev) => {
      if (!prev?.computers || prev.computers.some((c) => c.computer_id === computer.computer_id)) return prev;
      const computers = [...prev.computers];
      const at = computer.is_primary ? 0 : computers.filter((c) => c.is_primary).length;
      computers.splice(at, 0, computer);
      return { ...prev, computers, ...(typeof prev.total === 'number' ? { total: prev.total + 1 } : {}) };
    },
  );
}

/** Take a machine the server just removed off every cached list, ahead of the refetch. */
export function removeComputerRow(queryClient: QueryClient, computerId: string) {
  queryClient.setQueriesData<ComputersResponse | undefined>(
    { queryKey: queryKeys.computers.lists() },
    (prev) => !prev?.computers?.some((c) => c.computer_id === computerId) ? prev : {
      ...prev,
      computers: prev.computers.filter((c) => c.computer_id !== computerId),
      ...(typeof prev.total === 'number' ? { total: Math.max(0, prev.total - 1) } : {}),
    },
  );
}

/**
 * Merge `patch` into one machine's row in every cached computer list, where
 * `when` (if given) accepts the row as it stands.
 */
export function patchComputerRow(
  queryClient: QueryClient,
  computerId: string,
  patch: Partial<Computer>,
  when?: (row: Computer) => boolean,
) {
  queryClient.setQueriesData<ComputersResponse | undefined>(
    { queryKey: queryKeys.computers.lists() },
    (prev) => !prev?.computers ? prev : {
      ...prev,
      computers: prev.computers.map((c) =>
        c.computer_id === computerId && (!when || when(c)) ? { ...c, ...patch } : c),
    },
  );
}

/**
 * Refresh everything that repeats a machine's values: its row, its workspaces,
 * the tier quota. Deliberately the list key and not the `computers` prefix:
 * the storage breakdown sits under that prefix too, and refetching it runs
 * `du` over the whole machine, which a rename or an always-on toggle has no
 * reason to pay for. A change that does move bytes calls
 * {@link invalidateMachineStorage} alongside.
 */
export function invalidateMachine(queryClient: QueryClient) {
  void queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() });
  void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
  void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() });
}

/** Re-read the per-folder breakdown, if anyone is looking at it. */
export function invalidateMachineStorage(queryClient: QueryClient, computerId: string) {
  void queryClient.invalidateQueries({ queryKey: queryKeys.computers.storage(computerId) });
}

/**
 * When, counted from a turn's end, the machine rows are re-read. The server
 * measures only after its post-turn housekeeping (the skill reconcile, then
 * the backup of every changed project), which takes a second on a quiet turn
 * and most of a minute after a large write, so the reads back off.
 */
export const TURN_END_REFRESH_MS: readonly number[] = [3_000, 15_000, 45_000];

let turnEndRefreshTimer: ReturnType<typeof setTimeout> | null = null;
// Bumped by every new series and by sign-out, so a read already in flight
// cannot schedule the next step of a series that was replaced.
let turnEndRefreshGeneration = 0;

function cancelTurnEndRefresh() {
  if (turnEndRefreshTimer) clearTimeout(turnEndRefreshTimer);
  turnEndRefreshTimer = null;
  turnEndRefreshGeneration += 1;
}

registerAuthReset(cancelTurnEndRefresh);

/** Each cached machine's reading time, by id. */
function diskReadings(queryClient: QueryClient): Map<string, string | undefined> {
  const readings = new Map<string, string | undefined>();
  for (const [, data] of queryClient.getQueriesData<ComputersResponse | undefined>({
    queryKey: queryKeys.computers.lists(),
  })) {
    for (const c of data?.computers ?? []) readings.set(c.computer_id, c.disk?.measured_at);
  }
  return readings;
}

/**
 * Re-read the machine rows once a turn has ended, so the disk reading the
 * server takes at turn end reaches the warning and the gallery.
 *
 * Delayed, because the server measures after it has sent the stream's end,
 * and repeated on {@link TURN_END_REFRESH_MS} until a row shows a reading it
 * did not have when the turn ended. The server skips a machine measured in
 * the last minute and never measures a local one without a quota, so the
 * series can run out without a new reading; that is what bounds it. A newer
 * turn end restarts the series, since one list read serves every machine.
 * Only the rows: the storage breakdown is not touched here.
 */
export function refreshComputersAfterTurn(
  queryClient: QueryClient,
  delaysMs: readonly number[] = TURN_END_REFRESH_MS,
) {
  cancelTurnEndRefresh();
  const generation = turnEndRefreshGeneration;
  const before = diskReadings(queryClient);
  const moved = () => {
    for (const [id, measuredAt] of diskReadings(queryClient)) {
      if (measuredAt && measuredAt !== before.get(id)) return true;
    }
    return false;
  };
  const step = (i: number) => {
    if (i >= delaysMs.length) return;
    turnEndRefreshTimer = setTimeout(() => {
      turnEndRefreshTimer = null;
      void queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() }).then(() => {
        if (generation === turnEndRefreshGeneration && !moved()) step(i + 1);
      });
    }, Math.max(0, delaysMs[i] - (delaysMs[i - 1] ?? 0)));
  };
  step(0);
}

/**
 * Put a live storage reading on the machine's row, which the server has just
 * stored too, unless the row already holds a newer one.
 */
function applyStorageReading(queryClient: QueryClient, computerId: string, storage: ComputerStorage) {
  const disk = storage.disk;
  if (!storage.live || !disk) return;
  patchComputerRow(queryClient, computerId, { disk }, (c) =>
    !c.disk || Date.parse(c.disk.measured_at) < Date.parse(disk.measured_at));
}

/**
 * A machine's disk with a per-workspace breakdown. It runs `du` over every
 * folder on the machine, so it is fetched only while someone is looking.
 */
export function useComputerStorage(computerId: string, options: { enabled: boolean }) {
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: queryKeys.computers.storage(computerId),
    queryFn: () => getComputerStorage(computerId),
    enabled: options.enabled,
    // Each read is a `du` on the machine, up to tens of seconds on a small
    // one, and the server reuses a reading younger than about a minute anyway.
    // So: no refetch on focus, no retry of a read that already cost that much,
    // and a spec change or an explicit re-open is what asks again.
    staleTime: 60_000,
    refetchOnWindowFocus: false,
    retry: false,
  });
  const storage = query.data;
  useEffect(() => {
    if (storage) applyStorageReading(queryClient, computerId, storage);
  }, [storage, computerId, queryClient]);
  return query;
}

/**
 * Write `status` into the computer and its cached workspace projections.
 * Exported because the action handlers reflect their own 202 through it, which
 * is also what arms the status stream for the transition that follows.
 */
export { patchComputerStatusInCaches } from '../utils/warmWorkspace';

/**
 * Subscribe to every machine in flight and mirror each transition onto the
 * machine row and onto every workspace that lives on it.
 *
 * Mount this once above every surface that can start or stop a machine, not on
 * the surface that happens to hold a workspace list: arming the stream is a
 * cache write any of them can make, so a page where the write has no watcher
 * leaves a row saying "Starting" forever. It takes no arguments for the same
 * reason - the watch set is read from the caches, so it is the same set
 * wherever it mounts.
 *
 * Only transitional machines get a stream. A machine at rest changes state only
 * because someone acts, and the action writes 'starting' into the cache itself,
 * which is what brings it into this set, so waiting on a resting machine would
 * hold a connection open for news that cannot arrive. That matters more than it
 * sounds: the dev proxy speaks HTTP/1.1, whose six connections per origin a
 * gallery of resting machines would spend entirely.
 *
 * A machine with no workspaces yet is watched too. It has nothing to fan out
 * to, but it is exactly the row a user just pressed Start on in the computers
 * list, and that row is the one that has to stop saying "Starting".
 *
 * Workspaces with no `computer_id` (Home before its first start, and rows
 * that predate the split) keep the per-workspace channel as their fallback,
 * since no machine will speak for them.
 */
export function useComputerStatusFanout(): void {
  const queryClient = useQueryClient();
  const { data: computerData } = useComputers();

  // Only the ids, joined, so the effect re-runs when the *set* of watched
  // machines changes rather than on every list refetch that returns equal rows.
  const watchedComputerKey = useMemo(
    () =>
      (computerData?.computers ?? [])
        .filter((c) => isComputerStatusTransitional(c.status))
        .map((c) => c.computer_id)
        .sort()
        .join(','),
    [computerData],
  );

  const watchedWorkspaceKey = useUnboundTransitionalWorkspaceKey();

  useEffect(() => {
    if (!watchedComputerKey) return;
    const controller = new AbortController();
    const reconcile = () => Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() }),
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() }),
    ]);

    for (const computerId of watchedComputerKey.split(',')) {
      void (async () => {
        // A stream that closes mid-transition is reopened, because the watch
        // key only changes when the *set* of moving machines does: a refetch
        // that still says 'starting' leaves this effect exactly where it was,
        // so nothing else would ever listen again.
        for (let attempt = 0; !controller.signal.aborted; attempt++) {
          // The last status this stream delivered. The close below has to tell
          // a transition that ended on the wire from one that ended without us,
          // and the stream is the only witness: nothing caches a machine on its own.
          let lastStatus: string | undefined;
          await streamComputerEvents(
            computerId,
            (frame) => {
              const { status } = frame;
              lastStatus = status;
              if (frame.stage) recordPrepareStage(computerId, frame.stage);
              // Before the status lands, so whoever reads the failure off the
              // list finds its reason waiting.
              if (status === 'error' && frame.error) recordPrepareFailure(computerId, frame.error);
              // The frame brings the failure the row keeps, so the card has it
              // even if the re-read below is slow or fails.
              if (frame.start_failure) patchComputerRow(queryClient, computerId, { start_failure: frame.start_failure });
              patchComputerStatusInCaches(queryClient, computerId, status);
              // A breakdown read while the machine was down only says to start it.
              if (status === 'running') invalidateMachineStorage(queryClient, computerId);
              // A failed start still needs the row: the server keeps why on
              // it (`start_failure`) before it sends 'error', or 'stopped' for
              // a start that reverted. Asked here, not after the close: either
              // leaves the watch set, so this effect is torn down, and its
              // signal aborted, on the next render.
              if (status === 'error' || status === 'stopped') void reconcile();
            },
            controller.signal,
          );
          if (controller.signal.aborted) return;
          // On a terminal status the stream said so. Otherwise it hit the
          // server's 600 s cap or a dropped link, and the cache may now be
          // holding a transition that finished without us: reconcile, and if
          // the machine is still moving, listen again.
          if (isComputerStatusTerminal(lastStatus)) return;
          await reconcile();
          if (controller.signal.aborted) return;
          if (!isComputerStatusTransitional(cachedComputerStatus(queryClient, computerId))) return;
          if (attempt >= STATUS_RECONNECT.maxAttempts) return;
          await sleep(
            Math.min(STATUS_RECONNECT.baseMs * 2 ** attempt, STATUS_RECONNECT.capMs),
            controller.signal,
          );
        }
      })();
    }

    return () => controller.abort();
  }, [watchedComputerKey, queryClient]);

  useEffect(() => {
    if (!watchedWorkspaceKey) return;
    const controller = new AbortController();
    for (const workspaceId of watchedWorkspaceKey.split(',')) {
      void streamWorkspaceEvents(
        workspaceId,
        (status, _sandboxState, computerId) =>
          patchWorkspaceStatusInCaches(queryClient, workspaceId, status, computerId),
        controller.signal,
      );
    }
    return () => controller.abort();
  }, [watchedWorkspaceKey, queryClient]);
}

/** Root of the workspace key family: the cache events this watch cares about. */
const WORKSPACE_KEY_ROOT = queryKeys.workspaces.all[0];

/**
 * The workspace statuses that move on without anyone acting. Not the
 * computer's transitional set: its 'creating' is a new machine's preparation,
 * which a row with no machine never goes through.
 */
const WORKSPACE_MOVING = new Set(['starting', 'stopping']);

/**
 * The in-flight workspaces no machine speaks for, as a sorted joined key.
 *
 * Read from the cache rather than taken as a prop so the fan-out mounts once,
 * and subscribed rather than snapshotted because the arming write is a cache
 * write: `warmWorkspace` putting the 202's 'starting' on a row is what brings
 * it into this set.
 */
function useUnboundTransitionalWorkspaceKey(): string {
  const queryClient = useQueryClient();

  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      queryClient.getQueryCache().subscribe((event) => {
        const [root] = event.query.queryKey;
        if (typeof root === 'string' && root === WORKSPACE_KEY_ROOT) onStoreChange();
      }),
    [queryClient],
  );

  const getKey = useCallback(() => {
    const ids = new Set(
      cachedWorkspaceLists(queryClient)
        .filter((ws) => !ws.computer_id && !!ws.status && WORKSPACE_MOVING.has(ws.status))
        .map((ws) => ws.workspace_id),
    );
    return [...ids].sort().join(',');
  }, [queryClient]);

  return useSyncExternalStore(subscribe, getKey);
}

/** The key to authorize on the user's machine; fetched once the user connects by hand. */
export function useSshKey(options: { enabled: boolean }) {
  return useQuery({
    queryKey: queryKeys.computers.sshKey(),
    queryFn: () => getSshKey(),
    enabled: options.enabled,
    staleTime: Infinity,
    retry: false,
  });
}

/** How often a pairing is re-read while the machine has not been reached. */
export const SSH_PAIRING_POLL_MS = 2000;

/**
 * The pairing the open form shows, minted once and kept while it is mounted.
 *
 * A query rather than a mutation so a re-render, or a remount in development,
 * reuses the one in flight instead of minting another. Never refetched on its
 * own: a new command replaces the one the user may already have pasted, so
 * only an expiry or "use a different machine" asks for one. Dropped on unmount,
 * so the next opening starts a fresh pairing. With `computerId` it is a
 * pairing that reconnects that computer, cached apart from a new machine's.
 */
export function useSshPairingMint(options: { enabled: boolean; computerId?: string | null }) {
  const computerId = options.computerId ?? null;
  return useQuery({
    queryKey: queryKeys.sshPairings.mint(computerId),
    queryFn: () => createSshPairing({ computerId }),
    enabled: options.enabled,
    staleTime: Infinity,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });
}

/**
 * Still worth asking about: the machine may check in, or check in again after
 * a fix. A machine reached but unable to host a computer is ready too, and the
 * same command run again once its owner fixes it moves the pairing on.
 */
export function sshPairingPending(status: SshPairingStatus | undefined): boolean {
  return !status || status.status !== 'ready' || !status.probe?.supported;
}

/**
 * A pairing's status, re-read every couple of seconds until it is ready.
 * `paused` holds the polling while a connect of our own is in flight, whose
 * answer is written straight into this entry; a poll that crossed it would
 * put the older state back. A 404 stops it: the pairing is gone, and the
 * caller mints another.
 */
export function useSshPairingStatus(token: string | null, options: { enabled: boolean; paused: boolean }) {
  return useQuery({
    queryKey: queryKeys.sshPairings.status(token ?? ''),
    queryFn: () => getSshPairing(token as string),
    enabled: options.enabled && !!token,
    // A ready pairing is settled: read again on a refocus after its 15
    // minutes, it would answer 404 and take the summary away from the user.
    staleTime: (query) => (sshPairingPending(query.state.data) ? 0 : Infinity),
    gcTime: 0,
    retry: false,
    refetchInterval: (query) => {
      if (options.paused) return false;
      if (query.state.status === 'error' && apiErrorStatus(query.state.error) === 404) return false;
      return sshPairingPending(query.state.data) ? SSH_PAIRING_POLL_MS : false;
    },
  });
}
