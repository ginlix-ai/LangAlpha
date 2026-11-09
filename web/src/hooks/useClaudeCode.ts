import { useEffect } from 'react';
import { useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import {
  getClaudeCodeComputers, getClaudeCodeSetup, getClaudeCodeStatus, setClaudeCodeComputerClis, type ClaudeCodeComputer,
  type ClaudeCodeComputers, type ClaudeCodeState, type ClaudeCodeStatus,
} from '@/api/model';
import { TOKEN_WORKS } from '@/lib/claudeCode';
import { queryKeys } from '@/lib/queryKeys';
import { isFeatureEnabled, useFeatures } from './useFeatures';

/** A token Anthropic has not answered for yet is read again this often. */
const CHECKING_POLL_MS = 5_000;
/** The longest delay a timer takes; a token lasting longer is read again at this pace. */
const MAX_DELAY_MS = 2 ** 31 - 1;
/** How often a token past its expiry by this clock is read again while the
 *  server, its clock behind this one, still says it works. */
const PAST_EXPIRY_POLL_MS = 30_000;

/** When a token that works is read again: as its expiry passes, when the
 *  server reads it as refused, so every reader changes then without a clock
 *  of its own. False for one with no expiry, or no token that works. */
function untilExpiry(state: string | undefined, expiresAt: string | null | undefined): number | false {
  const at = expiresAt && state && TOKEN_WORKS.has(state) ? Date.parse(expiresAt) : NaN;
  if (Number.isNaN(at)) return false;
  const left = at - Date.now();
  return left >= 0 ? Math.min(left + 1_000, MAX_DELAY_MS) : PAST_EXPIRY_POLL_MS;
}

/** When a credential Check now is hidden for is read again: as the first
 *  limit its plan holds ends, when the server says again whether a check
 *  would still be billed (`check_bills`). Its rule needs the model a check
 *  asks for, which only the server knows, so it is read rather than worked
 *  out here. False while Check now is not hidden for it. */
function untilBillingEnds(entry: Pick<ClaudeCodeStatus, 'check_bills' | 'plan'> | undefined): number | false {
  if (!entry?.check_bills) return false;
  const ends = (entry.plan?.limits ?? []).map((limit) => Date.parse(limit.until) - Date.now()).filter((left) => left > 0);
  return ends.length > 0 ? Math.min(Math.min(...ends) + 1_000, MAX_DELAY_MS) : false;
}

/** When an own computer's token that works is read again: as its expiry
 *  passes, so a reader judging it at the read's time (`readAt`) finds it past
 *  then without a clock of its own, and as often as a token past its expiry
 *  is (`untilExpiry`) until a read lands after it, since a read that fails
 *  leaves the last one standing. False once one has, since the server keeps
 *  its last check's answer until the next. */
function untilComputerExpiry({ state, expires_at: expiresAt }: ClaudeCodeComputer, readAt: number): number | false {
  const at = expiresAt && TOKEN_WORKS.has(state) ? Date.parse(expiresAt) : NaN;
  if (Number.isNaN(at) || readAt >= at) return false;
  const left = at - Date.now();
  return left >= 0 ? Math.min(left + 1_000, MAX_DELAY_MS) : PAST_EXPIRY_POLL_MS;
}

/** The earlier of two reads again, either of which may be none. */
const sooner = (a: number | false, b: number | false) => (a === false ? b : b === false ? a : Math.min(a, b));

/** When Check now may run again on a credential, by this browser's clock:
 *  the server's `check_in` counted from when its answer arrived, so a
 *  browser clock set apart from the server's leaves the hold as long as the
 *  server meant. Null while a check may run. */
const holdUntil = (checkIn: number | null | undefined, arrivedAt: number) => (checkIn ? arrivedAt + checkIn * 1_000 : null);

/** Writes `update` over each entry of the cached computers list that `which`
 *  picks, kept as of the list's own read, which every entry's `check_in`
 *  counts from: the `check_in` an update gives counts from now, so it is
 *  moved to that read. */
export function updateComputers(
  queryClient: QueryClient, which: (c: ClaudeCodeComputer) => boolean, update: (c: ClaudeCodeComputer) => ClaudeCodeComputer,
) {
  const key = queryKeys.features.claudeCodeComputers();
  const listAt = queryClient.getQueryState(key)?.dataUpdatedAt;
  const since = listAt ? (Date.now() - listAt) / 1_000 : 0;
  queryClient.setQueryData<ClaudeCodeComputers>(key, (list) => list && ({
    ...list,
    computers: list.computers.map((c) => {
      if (!which(c)) return c;
      const next = update(c);
      return next.check_in ? { ...next, check_in: next.check_in + since } : next;
    }),
  }), { updatedAt: listAt });
}

/**
 * Saves how many Claude Code agents the user's own computer `computerId`
 * runs at once. A list read already in flight is cancelled first, so it
 * cannot land after the save and put the old number back; the entry the
 * server answers replaces the computer's in the list, and the list and setup
 * are read again, so every reader shows the new number.
 */
export function useSetComputerClis(computerId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (clis: number) => setClaudeCodeComputerClis(computerId, clis),
    onMutate: () => queryClient.cancelQueries({ queryKey: queryKeys.features.claudeCodeComputers() }),
    onSuccess: (entry) => {
      updateComputers(queryClient, (c) => c.computer_id === entry.computer_id, () => entry);
    },
    onSettled: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeComputers() });
      void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeSetup() });
    },
  });
}

/** `whenOff`: read even while the user's flag is off, for a token left in
 *  place when they opted out, which Settings still offers to remove. */
interface ReadOptions {
  whenOff?: boolean;
}

function useClaudeCodeFlag() {
  const { data: features, isLoading } = useFeatures();
  return { flagged: isFeatureEnabled(features, 'claude_code_runtime'), featuresLoading: isLoading };
}

/**
 * Where the user's Claude Code stands: whether its models are on for them
 * (`enabled`, wherever they run), whether this server runs turns on the
 * setup token saved in Settings (`runsHere`), and what that token is doing.
 *
 * The flag gates the read, so a status cached while opted in reads as `off`
 * once the user opts out. The server answers at once and checks a token it
 * has no answer for meanwhile (`checking`), so the status is read again until
 * the answer lands. One it could not check (`unverified`) waits for the next
 * ordinary read, since each recheck spends a request on the user's plan. A
 * token that works is read again as it expires.
 */
export function useClaudeCodeStatus({ whenOff = false }: ReadOptions = {}) {
  const { flagged, featuresLoading } = useClaudeCodeFlag();
  const { data, dataUpdatedAt, isLoading } = useQuery({
    queryKey: queryKeys.features.claudeCodeStatus(),
    queryFn: () => getClaudeCodeStatus(),
    enabled: flagged || whenOff,
    staleTime: 60_000,
    retry: false,
    refetchInterval: (query) => (query.state.data?.state === 'checking'
      ? CHECKING_POLL_MS : sooner(untilExpiry(query.state.data?.state, query.state.data?.expires_at), untilBillingEnds(query.state.data))),
  });
  const state: ClaudeCodeState = flagged ? data?.state ?? 'off' : 'off';
  // A server from before `enabled` ran Claude Code only on the saved token.
  const enabled = flagged && (data?.enabled ?? state !== 'off');
  return {
    enabled,
    runsHere: enabled && state !== 'off',
    state,
    saved: data?.saved === true,
    email: data?.email ?? null,
    expiresAt: data?.expires_at ?? null,
    plan: data?.plan,
    checkAfter: holdUntil(data?.check_in, dataUpdatedAt),
    checkBills: data?.check_bills === true,
    isLoading: featuresLoading || isLoading,
  };
}

/**
 * The user's own computers (SSH, and a Mac through Remote control) and the
 * token each one's Claude Code turns use, read again every 5 s while any of
 * them is being checked, as the saved token expires while one signs in
 * with it, and as a token of one's own expires. The saved token read as refused while one still signs in with it
 * reads them again too, since the status may be read first (a refocus) and
 * the expiry timer then never fires. With the flag off the server says
 * `off` for each, still naming a token left on one.
 */
export function useClaudeCodeComputers({ whenOff = false }: ReadOptions = {}) {
  const { flagged, featuresLoading } = useClaudeCodeFlag();
  const saved = useClaudeCodeStatus({ whenOff });
  const { data, dataUpdatedAt, isLoading, isError } = useQuery({
    queryKey: queryKeys.features.claudeCodeComputers(),
    queryFn: () => getClaudeCodeComputers(),
    enabled: flagged || whenOff,
    staleTime: 60_000,
    retry: 1,
    refetchInterval: (query) => {
      const computers = query.state.data?.computers ?? [];
      if (computers.some((c) => c.state === 'checking')) return CHECKING_POLL_MS;
      const expiry = computers.some((c) => c.source === 'saved') ? untilExpiry(saved.state, saved.expiresAt) : false;
      const expiries = computers.map((c) => untilComputerExpiry(c, query.state.dataUpdatedAt));
      return [...computers.map(untilBillingEnds), ...expiries].reduce(sooner, expiry);
    },
  });
  const queryClient = useQueryClient();
  const behindSaved = saved.state === 'rejected'
    && (data?.computers ?? []).some((c) => c.source === 'saved' && TOKEN_WORKS.has(c.state));
  useEffect(() => {
    if (behindSaved) void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeComputers() });
  }, [behindSaved, queryClient]);
  // A failed read again keeps the list it had, which still says whose they are.
  return {
    computers: data?.computers ?? [], cloudRefusals: data?.cloud_refusals ?? {}, cliComputer: data?.cli_computer ?? null,
    clisMax: data?.clis_max ?? null,
    isLoading: featuresLoading || isLoading,
    unread: isError && !data,
    /** When the list arrived, by this clock: the time a token's expiry is judged at. */
    readAt: dataUpdatedAt,
    checkAfterOf: (entry: ClaudeCodeComputer) => holdUntil(entry.check_in, dataUpdatedAt),
  };
}

/**
 * Claude Code's entry under Connect a subscription: where it stands as a
 * whole, read whether the user turned it on or not, so the entry is there
 * whenever the server offers Claude Code and says what is missing. Null
 * where the server offers none. Read again every 5 s while a token is being
 * checked, and as the next working token expires; every token change and the opt-in reread it with the other
 * Claude Code reads, as a computer added, removed or started does. Read
 * again whenever Settings opens, since a computer can come online (a Mac's
 * Remote control turned on) with no change made here. Not read while
 * `enabled` is false.
 */
export function useClaudeCodeSetup({ enabled = true }: { enabled?: boolean } = {}) {
  const saved = useClaudeCodeStatus();
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.features.claudeCodeSetup(),
    queryFn: () => getClaudeCodeSetup(),
    enabled,
    staleTime: 60_000,
    refetchOnMount: 'always',
    retry: 1,
    refetchInterval: (query) => {
      const setup = query.state.data;
      if (setup?.state === 'checking' || setup?.pin_state === 'checking') return CHECKING_POLL_MS;
      if (setup?.state !== 'connected') return false;
      // Read again as the next working token expires, which may be the one it is connected by.
      const waits = [{ state: saved.state, expires_at: saved.expiresAt }, ...setup.computers]
        .map(({ state, expires_at }) => untilExpiry(state, expires_at))
        .filter((wait): wait is number => wait !== false);
      return waits.length > 0 ? Math.min(...waits) : false;
    },
  });
  // A failed read is not "not offered": the entry stays, its status unknown.
  return { setup: data ?? null, isLoading, unread: isError && data === undefined };
}
