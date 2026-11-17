/**
 * The file cards of a Home turn that ran on the cloud stand-in, pointed at
 * where each file is now (`placeCloudTurns`): the copy a hand-back brought
 * into Home, else the stand-in's own. Asks the server only in a thread with
 * such a turn, and again while one of their files has not come back or the
 * read failed. Until it answers, those turns show only the files that name
 * their workspace: the rest would open as Home's file of the same name.
 */
import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { HomeFallback } from '@/types/sse';

import type { MessageRecord } from '../components/messageList/types';
import { getHomeHandbacks } from '../utils/api/workspaces';
import { cloudTurnsSince, placeCloudTurns } from '../utils/cloudTurnFiles';
import type { TurnFile } from '../utils/turnFiles';

/** How often the hand-backs are read again while a cloud turn's file is still only on the cloud. */
const UNPLACED_REFETCH_MS = 30_000;

export function useCloudTurnFiles(
  turned: readonly { message: MessageRecord; turnIndex: number }[],
  files: Map<number, TurnFile[]>,
  { enabled }: { enabled: boolean },
): Map<number, TurnFile[]> {
  const cloudTurns = useMemo(() => {
    const byTurn = new Map<number, HomeFallback>();
    for (const { message, turnIndex } of turned) {
      const fallback = message.role === 'user' ? (message.homeFallback as HomeFallback | undefined) : undefined;
      if (fallback) byTurn.set(turnIndex, fallback);
    }
    return byTurn;
  }, [turned]);
  const reading = enabled && cloudTurns.size > 0;
  const since = useMemo(() => cloudTurnsSince(cloudTurns.values()), [cloudTurns]);
  const { data: handbacks } = useQuery({
    queryKey: queryKeys.workspaces.homeHandbacks(since),
    queryFn: () => getHomeHandbacks(since),
    enabled: reading,
    staleTime: 15_000,
    retry: false,
    refetchInterval: (query) =>
      query.state.status === 'error'
      || (query.state.data && placeCloudTurns(files, cloudTurns, query.state.data).unplaced)
        ? UNPLACED_REFETCH_MS
        : false,
  });
  return useMemo(() => {
    if (!reading) return files;
    if (handbacks) return placeCloudTurns(files, cloudTurns, handbacks).placed;
    const known = new Map(files);
    for (const turnIndex of cloudTurns.keys()) {
      const turnFiles = files.get(turnIndex);
      if (turnFiles) known.set(turnIndex, turnFiles.filter((file) => file.workspaceId));
    }
    return known;
  }, [reading, handbacks, cloudTurns, files]);
}
