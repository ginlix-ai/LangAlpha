/**
 * Adding a computer of the user's own from setup, with Home placed where the
 * answers say. The first computer becomes primary as it is created and Home
 * lives on the primary, so a cloud answer first saves that choice and binds
 * Home on a cloud computer (Home's start binds it, building the computer a
 * turn would). An own-computer answer, or none yet (this Mac paired before
 * the questions), moves Home onto a backed-up machine from the primary it is
 * on, as the desktop first run does, and saves `computer_choice`, so no cloud
 * computer is built for the user while their machine holds Home. Keyed as a
 * create, so turning remote control off waits for it from its first read.
 */
import { useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query';
import type { OnboardingRunsOn } from '@/api/onboarding';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { queryKeys } from '@/lib/queryKeys';
import { providerIsOwnMachine } from '@/pages/ChatAgent/components/providerUi';
import { useComputerActions } from '@/pages/ChatAgent/hooks/useComputerActions';
import { CREATE_COMPUTER_MUTATION_KEY, fetchComputerList } from '@/pages/ChatAgent/hooks/useComputers';
import { startWorkspace } from '@/pages/ChatAgent/utils/api';
import { isNeedsComputer, needsBackupOf } from '@/pages/ChatAgent/utils/needsComputer';
import { updateCurrentUser } from '@/pages/Dashboard/utils/api';
import type { Computer, ComputerCreate } from '@/types/api';

/**
 * Where Home is once the computer is added: on it, moving to it from a cloud
 * computer or from another machine of the user's own (named), or where it was.
 */
export type HomeOutcome =
  | { kind: 'here' }
  | { kind: 'moving'; fromMachine: string | null }
  | { kind: 'stays' };

async function bindHomeOnCloud(queryClient: QueryClient) {
  // An earlier own-machine choice would refuse the cloud computer.
  await saveChoice(queryClient, 'cloud');
  const home = await queryClient.fetchQuery(homeWorkspaceQuery(queryClient));
  try {
    await startWorkspace(home.workspace_id, { lazy: true, reason: 'onboarding' });
  } catch (err) {
    // A server that builds no cloud computer has none to bind Home to, so
    // the machine added next holds it.
    if (!isNeedsComputer(err) && !needsBackupOf(err)) throw err;
  }
}

async function saveChoice(queryClient: QueryClient, choice: 'cloud' | 'own_machine') {
  await updateCurrentUser({ computer_choice: choice });
  void queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
}

export function useAddOwnComputer(runsOn: OnboardingRunsOn | null | undefined) {
  const queryClient = useQueryClient();
  const { create } = useComputerActions();
  return useMutation({
    mutationKey: CREATE_COMPUTER_MUTATION_KEY,
    mutationFn: async (body: ComputerCreate): Promise<{ computer: Computer; home: HomeOutcome }> => {
      const own = runsOn !== 'cloud';
      const { computers } = await fetchComputerList(queryClient);
      const primary = computers.find((c) => c.is_primary && c.status !== 'deleted');
      if (!own) {
        if (!primary) await bindHomeOnCloud(queryClient);
        return { computer: await create.mutateAsync(body), home: { kind: 'stays' } };
      }
      const backedUp = body.files_backup !== false;
      const movesHome = backedUp && !!primary;
      const computer = await create.mutateAsync(movesHome ? { ...body, make_primary: true } : body);
      if (!backedUp) return { computer, home: { kind: 'stays' } };
      // Best effort, as in the first run: the machine holds Home either way,
      // and the answer only decides once the user has no primary.
      await saveChoice(queryClient, 'own_machine').catch(() => {});
      if (!primary) return { computer, home: { kind: 'here' } };
      return { computer, home: { kind: 'moving', fromMachine: providerIsOwnMachine(primary.kind) ? primary.name : null } };
    },
  });
}
