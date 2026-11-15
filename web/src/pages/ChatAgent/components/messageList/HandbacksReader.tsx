/**
 * Reads where Home's hand-backs put the files of its cloud turns, for
 * `useCloudTurnFiles`, which mounts it only in a thread with such a turn.
 */
import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';

import { getHomeHandbacks, type HomeHandbacks } from '../../utils/api/workspaces';

/** How often the hand-backs are read again while a cloud turn's file is still only on the cloud. */
const UNPLACED_REFETCH_MS = 30_000;

export function HandbacksReader({ poll, onData }: { poll: boolean; onData: (data: HomeHandbacks) => void }) {
  const { data } = useQuery({
    queryKey: queryKeys.workspaces.homeHandbacks(),
    queryFn: getHomeHandbacks,
    staleTime: 15_000,
    retry: false,
    refetchInterval: poll ? UNPLACED_REFETCH_MS : false,
  });
  useEffect(() => {
    if (data) onData(data);
  }, [data, onData]);
  return null;
}

