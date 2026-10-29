import { useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { CommandRun, CommandsResponse } from '@/types/commands';
import { fetchCommands, stopCommand } from '../utils/api';
import { isValidUuid } from '../utils/uuid';
import { patchListingAfterMutation } from './utils/patchListing';

/** How often a running command can be seen to move: the server looks at one
 *  about this often, so the listing is read again at this pace while one
 *  runs, and the minutes it shows tick at it. */
export const COMMAND_TICK_MS = 15_000;

const NO_COMMANDS: readonly CommandRun[] = [];

function anyRunning(data: CommandsResponse | undefined): boolean {
  return !!data?.commands.some((command) => command.status === 'running');
}

/**
 * The thread's background commands, newest first, read once per view: the
 * tray takes them from here and every tool card watches this read in the
 * cache (`useCommandForToolCall`). A command ends on the server, so the
 * listing is read again every `COMMAND_TICK_MS` while one runs and not at all
 * once none does; what the stream sees start or end one says so through
 * `invalidateThreadCommands`, and a page shown again reads it afresh.
 *
 * `active` gates the reads only, as it does for the wakeups: a chat kept warm
 * behind another one keeps what it showed and stops polling.
 */
export function useThreadCommands(threadId: string | null | undefined, active = true): readonly CommandRun[] {
  const thread = isValidUuid(threadId) ? threadId : null;
  const { data } = useQuery({
    queryKey: queryKeys.threads.commands(thread ?? ''),
    queryFn: () => fetchCommands(thread as string),
    enabled: thread !== null && active,
    staleTime: 0,
    // An older server has no listing; every chat reads it, so a failure
    // leaves what was shown rather than retrying.
    retry: false,
    refetchInterval: (query) => (anyRunning(query.state.data) ? COMMAND_TICK_MS : false),
  });
  return data?.commands ?? NO_COMMANDS;
}

/** Re-read the thread's commands after something the view saw may have
 *  started or ended one: a hand-off, a report, a reload. */
export function invalidateThreadCommands(queryClient: QueryClient, threadId: string | null | undefined): void {
  if (!isValidUuid(threadId)) return;
  void queryClient.invalidateQueries({ queryKey: queryKeys.threads.commands(threadId) });
}

/**
 * Stops a command and shows the row the answer carries, or reads the listing
 * again when it carries none. Rejects when the request fails, for the caller
 * to say so.
 */
export function useStopCommand(threadId: string | null | undefined) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (commandId: string) => {
      if (!isValidUuid(threadId)) return Promise.reject(new Error('No thread to stop the command in'));
      return stopCommand(threadId, commandId);
    },
    onSuccess: ({ command }) => patchListingAfterMutation<CommandsResponse>(
      queryClient,
      queryKeys.threads.commands(threadId ?? ''),
      command
        ? (prev) => ({ ...prev, commands: prev.commands.map((row) => (row.command_id === command.command_id ? command : row)) })
        : null,
    ),
  });
}
