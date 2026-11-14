import { lazy, Suspense, useEffect, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { decideCliApproval, type CliApproval } from '@/api/cliApprovals';
import { apiErrorStatus } from '@/api/errors';
import { toast } from '@/components/ui/use-toast';
import { usePendingCliApprovals } from '@/hooks/useCliApprovals';
import { useNow } from '@/hooks/useNow';
import { queryKeys } from '@/lib/queryKeys';

// Off first load: most sessions never see a request.
const CliApprovalDialog = lazy(() => import('./CliApprovalDialog'));

const EMPTY: ReadonlySet<string> = new Set();

/**
 * Puts each request the agent made from a command to the user, oldest first,
 * on whatever page is open. Mounted once in the app shell beside the user
 * feed, which tells it when one arrives or is answered elsewhere.
 *
 * An answer closes the dialog at once and the next request takes its place;
 * a failed one brings it back with a toast, and one answered elsewhere or
 * run out (409) just reads the list again. Closed without an answer, a
 * request stays waiting on the server but is not shown here again.
 */
export function CliApprovalHost() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const pending = usePendingCliApprovals();
  // Answered here (until the list catches up) or closed without an answer.
  // Each change keeps only the ids still waiting, so it never outgrows the list.
  const [hidden, setHidden] = useState<ReadonlySet<string>>(EMPTY);
  const now = useNow(1000, pending.length > 0);

  const hide = (id: string, on: boolean) =>
    setHidden((prev) => {
      const next = new Set(pending.map((a) => a.approval_id).filter((h) => h !== id && prev.has(h)));
      if (on) next.add(id);
      return next;
    });

  const refresh = () => queryClient.invalidateQueries({ queryKey: queryKeys.cliApprovals.pending() });

  const decide = useMutation({
    mutationFn: ({ approval, approve }: { approval: CliApproval; approve: boolean }) =>
      decideCliApproval(approval.approval_id, approve),
    onMutate: ({ approval }) => hide(approval.approval_id, true),
    onError: (err, { approval }) => {
      const status = apiErrorStatus(err);
      // Answered somewhere else, or no longer waiting: nothing to tell.
      if (status === 409 || status === 404) return;
      hide(approval.approval_id, false);
      toast({ variant: 'destructive', description: t('cliApproval.failed') });
    },
    onSettled: () => refresh(),
  });

  // The server lists only what still waits; this browser's clock may be off
  // from it, so it times the countdown and never decides what shows.
  const waiting = pending.filter((a) => !hidden.has(a.approval_id));
  // Once a countdown runs out here, ask the server once whether it still waits.
  const lapsed = waiting
    .filter((a) => Date.parse(a.expires_at) <= now)
    .map((a) => a.approval_id)
    .join(',');
  useEffect(() => {
    if (lapsed) void queryClient.invalidateQueries({ queryKey: queryKeys.cliApprovals.pending() });
  }, [lapsed, queryClient]);
  const current = waiting[0];
  if (!current) return null;

  return (
    <Suspense fallback={null}>
      <CliApprovalDialog
        key={current.approval_id}
        approval={current}
        more={waiting.length - 1}
        now={now}
        onAnswer={(approval, approve) => decide.mutate({ approval, approve })}
        onDismiss={(approval) => hide(approval.approval_id, true)}
      />
    </Suspense>
  );
}
