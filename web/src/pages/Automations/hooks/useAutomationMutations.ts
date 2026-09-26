import { useIsMutating, useMutation, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { FAIL_FAST_OFFLINE } from '@/lib/network';
import { queryKeys } from '@/lib/queryKeys';
import type { AutomationPayload, AutomationUpdatePayload } from '@/types/automation';
import { apiErrorStatus } from '@/pages/ChatAgent/utils/api/errors';
import * as automationApi from '../utils/api';
import { mutationErrorMessage } from '../utils/errors';

/** Every write carries it, so `busy` counts them all: two overlapping writes
 *  keep the buttons off until the second one settles too. */
const WRITE_KEY = ['automation-write'] as const;

/**
 * Every write invalidates the whole automations family: a pause moves a row
 * between groups, a trigger adds a run to the feed and to that automation's
 * history. Success is announced only where nothing on screen changes at once:
 * a triggered run takes a moment to appear, everything else is its own proof.
 * A failure is announced here, so a button fires a verb with `mutate` and
 * forgets it, while `mutateAsync` still rejects for a caller that branches.
 */
export function useAutomationMutations() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const shared = {
    // The form and the delete dialog wait on these, and a write paused
    // offline would hold them with no way out.
    ...FAIL_FAST_OFFLINE,
    mutationKey: WRITE_KEY,
    // Returned, so the write stays pending until the lists it changed are
    // read again and no button comes back on over stale rows.
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.automations.all }),
    onError: (err: unknown) =>
      toast({ variant: 'destructive', description: mutationErrorMessage(err, t('automation.actionFailed')) }),
  };

  const create = useMutation({
    ...shared,
    mutationFn: async (data: AutomationPayload) => (await automationApi.createAutomation(data)).data,
  });
  const update = useMutation({
    ...shared,
    mutationFn: async ({ id, data }: { id: string; data: AutomationUpdatePayload }) =>
      (await automationApi.updateAutomation(id, data)).data,
  });
  const remove = useMutation({ ...shared, mutationFn: (id: string) => automationApi.deleteAutomation(id) });
  const pause = useMutation({ ...shared, mutationFn: (id: string) => automationApi.pauseAutomation(id) });
  const resume = useMutation({ ...shared, mutationFn: (id: string) => automationApi.resumeAutomation(id) });
  const trigger = useMutation({
    ...shared,
    mutationFn: (id: string) => automationApi.triggerAutomation(id),
    onSuccess: () => {
      toast({ description: t('automation.runStarted') });
      return shared.onSuccess();
    },
  });
  // A 409 means the run started or settled before the click: there is
  // nothing left to skip, and the refetch every outcome gets shows where it went.
  const skip = useMutation({
    ...shared,
    mutationFn: async ({ automationId, executionId }: { automationId: string; executionId: string }) => {
      try {
        return await automationApi.skipRun(automationId, executionId);
      } catch (err: unknown) {
        if (apiErrorStatus(err) === 409) return null;
        throw err;
      }
    },
  });

  const dismiss = useMutation({
    ...shared,
    mutationFn: ({ automationId, executionId }: { automationId: string; executionId: string }) =>
      automationApi.dismissRun(automationId, executionId),
  });

  const busy = useIsMutating({ mutationKey: WRITE_KEY }) > 0;
  return { create, update, remove, pause, resume, trigger, skip, dismiss, busy };
}

export type AutomationMutations = ReturnType<typeof useAutomationMutations>;
