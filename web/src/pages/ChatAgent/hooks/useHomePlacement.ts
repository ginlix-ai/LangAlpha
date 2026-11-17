/**
 * Where Home lives, and where its turns run while that machine can't be
 * reached: on the user's cloud computer standing in for it, or nowhere, when
 * they have none and Home waits.
 *
 * A pure read on the server, so polling it starts no computer. Polled only
 * while something about it can change by itself: Home is on a machine the
 * user brought (which can go to sleep), files are owed back from the cloud,
 * or Home is moving. A cloud Home never goes away, so it is read once.
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { TFunction } from 'i18next';

import { formatList } from '@/lib/format';
import { queryKeys } from '@/lib/queryKeys';

import { providerIsOwnMachine } from '../components/providerUi';
import { getHomePlacement, markHomeHandbackSeen, type HomeHandback, type HomePlacement } from '../utils/api';
import { AWAY_REFETCH_MS } from '../utils/computerAway';
import { isMoveActive } from './useWorkspaceMove';

/** Paths named before the rest are counted. */
const NAMED_FILES = 3;

/** The machine as a line names it, when the server could not. */
export function machineName(t: TFunction, name: string | null | undefined): string {
  return name?.trim() || t('homePlacement.homeComputer');
}

/** The first few paths, then how many more. */
export function namedFiles(t: TFunction, paths: readonly string[], locale: string): string {
  const shown = paths.slice(0, NAMED_FILES);
  const more = paths.length - shown.length;
  return formatList(more > 0 ? [...shown, t('homePlacement.andMore', { count: more })] : shown, locale);
}

/** How often to read the placement again, or false to leave it. */
export function homePlacementPollMs(placement: HomePlacement | undefined): number | false {
  if (!placement) return false;
  const changes = providerIsOwnMachine(placement.computer_kind ?? '')
    || placement.unreachable
    || placement.handback_owed
    || isMoveActive(placement.move);
  return changes ? AWAY_REFETCH_MS : false;
}

/** Which lines the placement calls for, in the order they stack. */
export function homePlacementLines(placement: HomePlacement | undefined): {
  away: 'onCloud' | 'waiting' | null;
  handback: HomeHandback | null;
} {
  if (!placement) return { away: null, handback: null };
  const away = placement.unreachable ? (placement.stand_in ? 'onCloud' : 'waiting') : null;
  return { away, handback: placement.handback };
}

export function useHomePlacement({ enabled = true }: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: queryKeys.workspaces.homePlacement(),
    queryFn: getHomePlacement,
    enabled,
    staleTime: 10_000,
    retry: false,
    refetchInterval: (query) => homePlacementPollMs(query.state.data),
  });
}

/** Dismisses the hand-back notice at once; the server is told behind it. */
export function useDismissHomeHandback() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (handbackId: string) => markHomeHandbackSeen(handbackId),
    // A read already on its way would land after this and bring the notice back.
    onMutate: async (handbackId) => {
      await queryClient.cancelQueries({ queryKey: queryKeys.workspaces.homePlacement() });
      queryClient.setQueryData<HomePlacement>(queryKeys.workspaces.homePlacement(), (old) =>
        old && old.handback?.handback_id === handbackId ? { ...old, handback: null } : old,
      );
    },
    onError: () => void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.homePlacement() }),
  });
}
