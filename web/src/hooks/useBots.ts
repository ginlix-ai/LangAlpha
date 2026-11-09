/**
 * React Query hooks for Bots.
 *
 * One list answer carries every workspace's bots, which is what the Settings
 * section groups; a workspace-scoped list is the same request narrowed. Every
 * write lands the server's answer in the single-row cache and reads the list
 * again, so a row is never drawn from a locally patched guess.
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import {
  createBot,
  deleteBot,
  getBot,
  getBots,
  updateBot,
  type BotCreateInput,
  type BotUpdateInput,
} from '@/api/bots';

/**
 * A bot's identity is drawn in a room's member chips and its log, so a bots
 * write also re-reads the rooms that show it: a rename, a pause or a delete
 * must not leave an open room naming the old face.
 */
function invalidateRoomCaches(queryClient: ReturnType<typeof useQueryClient>): void {
  queryClient.invalidateQueries({ queryKey: queryKeys.rooms.details() });
  queryClient.invalidateQueries({ queryKey: queryKeys.rooms.lists() });
}

/** Every workspace's bots when `workspaceId` is null, else one workspace's. */
export function useBots(workspaceId: string | null = null, enabled = true) {
  return useQuery({
    queryKey: queryKeys.bots.list(workspaceId),
    queryFn: () => getBots(workspaceId),
    enabled,
    staleTime: 30_000,
  });
}

/** One bot by id; disabled until an id is in hand. */
export function useBot(botId: string | null) {
  return useQuery({
    queryKey: queryKeys.bots.detail(botId ?? ''),
    queryFn: () => getBot(botId!),
    enabled: !!botId,
    staleTime: 30_000,
  });
}

export function useCreateBot() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: BotCreateInput) => createBot(input),
    onSuccess: (bot) => {
      queryClient.setQueryData(queryKeys.bots.detail(bot.bot_id), bot);
      queryClient.invalidateQueries({ queryKey: queryKeys.bots.lists() });
      invalidateRoomCaches(queryClient);
    },
  });
}

export function useUpdateBot() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ botId, patch }: { botId: string; patch: BotUpdateInput }) =>
      updateBot(botId, patch),
    onSuccess: (bot) => {
      queryClient.setQueryData(queryKeys.bots.detail(bot.bot_id), bot);
      queryClient.invalidateQueries({ queryKey: queryKeys.bots.lists() });
      invalidateRoomCaches(queryClient);
    },
  });
}

export function useDeleteBot() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (botId: string) => deleteBot(botId),
    onSuccess: (_answer, botId) => {
      queryClient.removeQueries({ queryKey: queryKeys.bots.detail(botId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.bots.lists() });
      invalidateRoomCaches(queryClient);
    },
  });
}
