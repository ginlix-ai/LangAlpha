import { useQuery } from '@tanstack/react-query';
import { getClaudeCodeStatus, type ClaudeCodeState } from '@/api/model';
import { queryKeys } from '@/lib/queryKeys';
import { isFeatureEnabled, useFeatures } from './useFeatures';

/**
 * Where the user's Claude Code stands: whether the server runs it for them
 * (their flag and the deployment both allow it), and what their setup token
 * is doing.
 *
 * The flag gates the read, so a status cached while opted in reads as `off`
 * once the user opts out. The server answers at once and checks a token it
 * has no answer for meanwhile (`checking`), so the status is read again until
 * the answer lands. One it could not check (`unverified`) waits for the next
 * ordinary read, since each recheck spends a request on the user's plan.
 */
export function useClaudeCodeStatus() {
  const { data: features, isLoading: featuresLoading } = useFeatures();
  const flagged = isFeatureEnabled(features, 'claude_code_runtime');
  const { data, isLoading } = useQuery({
    queryKey: queryKeys.features.claudeCodeStatus(),
    queryFn: getClaudeCodeStatus,
    enabled: flagged,
    staleTime: 60_000,
    retry: false,
    refetchInterval: (query) => (query.state.data?.state === 'checking' ? 5_000 : false),
  });
  const state: ClaudeCodeState = flagged ? data?.state ?? 'off' : 'off';
  return {
    state,
    email: data?.email ?? null,
    expiresAt: data?.expires_at ?? null,
    isLoading: featuresLoading || isLoading,
  };
}
