import { useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getClaudeCodeComputers, getClaudeCodeSetup, getClaudeCodeStatus, type ClaudeCodeState } from '@/api/model';
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
  const at = expiresAt && (state === 'signed_in' || state === 'unverified') ? Date.parse(expiresAt) : NaN;
  if (Number.isNaN(at)) return false;
  const left = at - Date.now();
  return left >= 0 ? Math.min(left + 1_000, MAX_DELAY_MS) : PAST_EXPIRY_POLL_MS;
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
  const { data, isLoading } = useQuery({
    queryKey: queryKeys.features.claudeCodeStatus(),
    queryFn: () => getClaudeCodeStatus(),
    enabled: flagged || whenOff,
    staleTime: 60_000,
    retry: false,
    refetchInterval: (query) => (query.state.data?.state === 'checking'
      ? CHECKING_POLL_MS : untilExpiry(query.state.data?.state, query.state.data?.expires_at)),
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
    isLoading: featuresLoading || isLoading,
  };
}

/**
 * The user's own computers (SSH, and a Mac through Remote control) and the
 * token each one's Claude Code turns use, read again every 5 s while any of
 * them is being checked, and as the saved token expires while one signs in
 * with it. The saved token read as refused while one still signs in with it
 * reads them again too, since the status may be read first (a refocus) and
 * the expiry timer then never fires. With the flag off the server says
 * `off` for each, still naming a token left on one.
 */
export function useClaudeCodeComputers({ whenOff = false }: ReadOptions = {}) {
  const { flagged, featuresLoading } = useClaudeCodeFlag();
  const saved = useClaudeCodeStatus({ whenOff });
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.features.claudeCodeComputers(),
    queryFn: () => getClaudeCodeComputers(),
    enabled: flagged || whenOff,
    staleTime: 60_000,
    retry: 1,
    refetchInterval: (query) => {
      const computers = query.state.data?.computers ?? [];
      if (computers.some((c) => c.state === 'checking')) return CHECKING_POLL_MS;
      return computers.some((c) => c.source === 'saved') ? untilExpiry(saved.state, saved.expiresAt) : false;
    },
  });
  const queryClient = useQueryClient();
  const behindSaved = saved.state === 'rejected'
    && (data?.computers ?? []).some((c) => c.source === 'saved' && (c.state === 'signed_in' || c.state === 'unverified'));
  useEffect(() => {
    if (behindSaved) void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeComputers() });
  }, [behindSaved, queryClient]);
  // A failed read again keeps the list it had, which still says whose they are.
  return { computers: data?.computers ?? [], isLoading: featuresLoading || isLoading, unread: isError && !data };
}

/**
 * Claude Code's entry under Connect a subscription: where it stands as a
 * whole, read whether the user turned it on or not, so the entry is there
 * whenever the server offers Claude Code and says what is missing. Null
 * where the server offers none. Read again every 5 s while a token is being
 * checked, and as the next working token expires; every token change and the opt-in reread it with the other
 * Claude Code reads, as a computer added, removed or started does. Read
 * again whenever Settings opens, since a computer can come online (a Mac's
 * Remote control turned on) with no change made here.
 */
export function useClaudeCodeSetup() {
  const saved = useClaudeCodeStatus();
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.features.claudeCodeSetup(),
    queryFn: () => getClaudeCodeSetup(),
    staleTime: 60_000,
    refetchOnMount: 'always',
    retry: 1,
    refetchInterval: (query) => {
      const setup = query.state.data;
      if (setup?.state === 'checking') return CHECKING_POLL_MS;
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
