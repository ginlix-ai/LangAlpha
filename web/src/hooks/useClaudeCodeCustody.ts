import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getClaudeCodeCustody, setClaudeCodeCustody, type ClaudeCodeCustodyMode } from '@/api/model';
import { queryKeys } from '@/lib/queryKeys';
import { useClaudeCodeStatus } from './useClaudeCode';

/**
 * How the user's Claude Code runs on cloud computers, where the server lets
 * them choose: null where it does not (its 404). Read while Claude Code
 * models are on for them, with the server's verdict for a turn there beside
 * the choice, so the app never works the verdict out itself. A choice made sets the
 * answer, and the status, which the server may now give otherwise, is read
 * again; one the server refuses rejects `choose`.
 */
export function useClaudeCodeCustody() {
  const queryClient = useQueryClient();
  const status = useClaudeCodeStatus();
  const offered = status.enabled;
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.user.claudeCodeCustody(),
    queryFn: () => getClaudeCodeCustody(),
    enabled: offered,
    staleTime: 60_000,
    retry: 1,
  });
  const choice = useMutation({
    mutationFn: (mode: ClaudeCodeCustodyMode) => setClaudeCodeCustody(mode),
    onSuccess: async (custody) => {
      // A read already on its way may have left before the choice landed.
      await queryClient.cancelQueries({ queryKey: queryKeys.user.claudeCodeCustody() });
      queryClient.setQueryData(queryKeys.user.claudeCodeCustody(), custody);
      void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeStatus() });
    },
  });
  return {
    custody: offered ? data ?? null : null,
    isLoading: status.isLoading || (offered && isLoading),
    /** The first read failed, so whether the server offers a choice is unknown. */
    unread: offered && isError && !data,
    choose: choice.mutateAsync,
    /** The mode being chosen, until the server answers. */
    choosing: choice.isPending ? choice.variables : null,
  };
}
