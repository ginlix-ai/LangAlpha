import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getClaudeCodeCustody, setClaudeCodeCustody, type ClaudeCodeCustodyMode } from '@/api/model';
import { authSessionCheck } from '@/lib/authResets';
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
 * server may now give otherwise, are read again; one answered after a
 * sign-out or an account switch was the last account's, so it rejects
 * instead and sets nothing. A status that could not be read leaves the
 * choice unread too, since it is not known whether one is read at all. One the server refuses
 * rejects `choose` and reads the choice again, since what it offers
 * may have changed, and the status and computers too, since one refused as
 * `turns_running` has already deleted the token. With `enabled` false
 * nothing is read, for a surface that may not need the choice.
 */
export function useClaudeCodeCustody({ enabled = true }: { enabled?: boolean } = {}) {
  const queryClient = useQueryClient();
  const status = useClaudeCodeStatus({ enabled });
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: queryKeys.user.claudeCodeCustody(),
    queryFn: () => getClaudeCodeCustody(),
    enabled: enabled && status.enabled,
    staleTime: 60_000,
    retry: 1,
  });
  const choice = useMutation({
    mutationFn: async ({ mode, disclosure }: { mode: ClaudeCodeCustodyMode; disclosure?: string }) => {
      const sameAccount = authSessionCheck();
      const custody = await setClaudeCodeCustody(mode, disclosure);
      if (!sameAccount()) throw new Error('The account changed before the choice was answered');
      return custody;
    },
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
    /** The first read of the status or the choice failed, so whether the server offers a choice is unknown. */
    unread: status.unread || (status.enabled && isError && !data),
    /** Read again whichever of the two failed. */
    retry: () => (status.unread ? status.retry() : void refetch()),
    choose: choice.mutateAsync,
    /** The mode being chosen, until the server answers. */
    choosing: choice.isPending ? choice.variables.mode : null,
  };
}
