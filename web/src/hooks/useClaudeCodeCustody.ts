import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getClaudeCodeCustody, setClaudeCodeCustody, type ClaudeCodeCustodyMode } from '@/api/model';
import { queryKeys } from '@/lib/queryKeys';
import { useClaudeCodeStatus } from './useClaudeCode';

/**
 * How the user's Claude Code runs on cloud computers, where the server lets
 * them choose: null where it does not (its 404). Read while Claude Code
 * models are on for them, wherever they run, with the server's verdict for a
 * turn there beside the choice, so the app never works the verdict out
 * itself: the status alone cannot tell a server that runs their saved token
 * because it is self-hosted from one that does because they chose to have
 * it stored.
 *
 * A choice made sets the answer, and the status and computers, which the
 * server may now give otherwise, are read again. One the server refuses
 * rejects `choose` and reads the choice again, since what it offers
 * may have changed, and the status and computers too, since one refused as
 * `turns_running` has already deleted the token.
 */
export function useClaudeCodeCustody() {
  const queryClient = useQueryClient();
  const status = useClaudeCodeStatus();
  const { data, isLoading, isError } = useQuery({
    queryKey: queryKeys.user.claudeCodeCustody(),
    queryFn: () => getClaudeCodeCustody(),
    enabled: status.enabled,
    staleTime: 60_000,
    retry: 1,
  });
  const choice = useMutation({
    mutationFn: ({ mode, disclosure }: { mode: ClaudeCodeCustodyMode; disclosure?: string }) => setClaudeCodeCustody(mode, disclosure),
    onSuccess: async (custody) => {
      // A read already on its way may have left before the choice landed.
      await queryClient.cancelQueries({ queryKey: queryKeys.user.claudeCodeCustody() });
      queryClient.setQueryData(queryKeys.user.claudeCodeCustody(), custody);
      void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCode() });
    },
    onError: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.user.claudeCodeCustody() });
      // A refusal can follow a token already deleted (`turns_running`).
      void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCode() });
    },
  });
  return {
    custody: status.enabled ? data ?? null : null,
    isLoading: status.isLoading || (status.enabled && isLoading),
    /** The first read failed, so whether the server offers a choice is unknown. */
    unread: status.enabled && isError && !data,
    choose: choice.mutateAsync,
    /** The mode being chosen, until the server answers. */
    choosing: choice.isPending ? choice.variables.mode : null,
  };
}
