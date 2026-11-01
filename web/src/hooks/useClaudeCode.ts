import { useQuery } from '@tanstack/react-query';
import { getClaudeCodeComputers, getClaudeCodeStatus, type ClaudeCodeState } from '@/api/model';
import { queryKeys } from '@/lib/queryKeys';
import { isFeatureEnabled, useFeatures } from './useFeatures';

/** A token Anthropic has not answered for yet is read again this often. */
const CHECKING_POLL_MS = 5_000;

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
 * ordinary read, since each recheck spends a request on the user's plan.
 */
export function useClaudeCodeStatus({ whenOff = false }: ReadOptions = {}) {
  const { flagged, featuresLoading } = useClaudeCodeFlag();
  const { data, isLoading } = useQuery({
    queryKey: queryKeys.features.claudeCodeStatus(),
    queryFn: () => getClaudeCodeStatus(),
    enabled: flagged || whenOff,
    staleTime: 60_000,
    retry: false,
    refetchInterval: (query) => (query.state.data?.state === 'checking' ? CHECKING_POLL_MS : false),
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
 * them is being checked. With the flag off the server says `off` for each,
 * still naming a token left on one.
 */
export function useClaudeCodeComputers({ whenOff = false }: ReadOptions = {}) {
  const { flagged, featuresLoading } = useClaudeCodeFlag();
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.features.claudeCodeComputers(),
    queryFn: () => getClaudeCodeComputers(),
    enabled: flagged || whenOff,
    staleTime: 60_000,
    retry: 1,
    refetchInterval: (query) => (query.state.data?.computers.some((c) => c.state === 'checking') ? CHECKING_POLL_MS : false),
  });
  // A failed read again keeps the list it had, which still says whose they are.
  return { computers: data?.computers ?? [], isLoading: featuresLoading || isLoading, unread: isError && !data };
}
