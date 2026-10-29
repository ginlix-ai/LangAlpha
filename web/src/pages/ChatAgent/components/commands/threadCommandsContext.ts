/**
 * The thread whose background commands a chat shows, provided once by the
 * chat host, which also reads its listing. A host that provides none (a
 * share, a subagent's transcript, a test) draws every tool card as it always
 * has.
 */
import { createContext, useContext } from 'react';
import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { CommandRun, CommandsResponse } from '@/types/commands';

export const ThreadCommandsContext = createContext<string | null>(null);

/**
 * The command a tool call handed off to, or null for a call that returned as
 * any call does. It watches the host's read in the cache and never reads
 * itself, so a change to the listing re-renders only the row it changed.
 */
export function useCommandForToolCall(toolCallId: string): CommandRun | null {
  const threadId = useContext(ThreadCommandsContext);
  const { data } = useQuery({
    queryKey: queryKeys.threads.commands(threadId ?? ''),
    enabled: false,
    select: (listing: CommandsResponse) => listing.commands.find((row) => row.tool_call_id === toolCallId) ?? null,
  });
  return data ?? null;
}
