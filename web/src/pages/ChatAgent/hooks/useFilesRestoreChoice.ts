/**
 * The user's answer to a restore offer: the server found a workspace's folder
 * gone from a machine the user brought and holds its backup until told to
 * restore it or keep the folder empty.
 *
 * The offer shows in more than one place at once (above the chat input and in
 * the file panel), so the answer is read from the mutation cache by key rather
 * than from one caller's copy: answered in one, the other shows it running too,
 * and neither offers a second answer while the first is on its way.
 */
import { useMutation, useMutationState, useQueryClient, type QueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { Workspace } from '@/types/api';

import { chooseFilesRestore } from '../utils/api';
import { apiErrorReason, apiErrorStatus } from '@/api/errors';

export function filesRestoreChoiceKey(workspaceId: string) {
  return ['files-restore-choice', workspaceId] as const;
}

/** Re-read what an answer changes: the workspace rows and the folder's files. */
function settleChoice(queryClient: QueryClient, workspaceId: string) {
  void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
  void queryClient.invalidateQueries({ queryKey: queryKeys.workspaceFiles.workspace(workspaceId) });
}

export interface FilesRestoreChoice {
  /** Answer the offer: `true` restores the backup, `false` keeps the folder empty. */
  choose: (restore: boolean) => void;
  /** The answer on its way, or null when none is. */
  pending: boolean | null;
  /** Why the last answer failed, in the server's words when it gave some. */
  error: string | null;
}

export function useFilesRestoreChoice(workspaceId: string, fallbackError: string): FilesRestoreChoice {
  const queryClient = useQueryClient();
  const mutationKey = filesRestoreChoiceKey(workspaceId);
  const mutation = useMutation({
    mutationKey,
    mutationFn: (restore: boolean) => chooseFilesRestore(workspaceId, restore),
    // On the hook, not on `mutate`: a restore can outlast the surface that
    // asked for it, and the row must still drop the offer when it lands.
    onSuccess: async (row) => {
      const key = queryKeys.workspaces.detail(workspaceId);
      await queryClient.cancelQueries({ queryKey: key });
      queryClient.setQueryData<Workspace>(key, (prev) => (prev ? { ...prev, ...row } : row));
    },
    onSettled: () => settleChoice(queryClient, workspaceId),
  });

  const runs = useMutationState({
    filters: { mutationKey },
    select: (m) => ({ status: m.state.status, restore: m.state.variables as boolean, error: m.state.error }),
  });
  const running = runs.find((run) => run.status === 'pending');
  const last = runs.at(-1);
  // A 409 means no offer stands: it was answered elsewhere, and the re-read
  // the settle started drops it here too. Nothing went wrong to tell.
  const failed = !running && last?.status === 'error' && apiErrorStatus(last.error) !== 409 ? last.error : null;

  return {
    choose: (restore) => {
      if (!running) mutation.mutate(restore);
    },
    pending: running ? running.restore : null,
    error: failed ? apiErrorReason(failed) ?? fallbackError : null,
  };
}
