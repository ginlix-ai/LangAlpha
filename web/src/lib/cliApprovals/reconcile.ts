/**
 * Keeps the cached list of the agent's waiting requests in step with the user
 * feed, which tells of each request as it arrives or is answered and lists the
 * waiting ids in every snapshot.
 */
import type { QueryClient } from '@tanstack/react-query';
import type { CliApproval } from '@/api/cliApprovals';
import { queryKeys } from '@/lib/queryKeys';

/** A request arrived or was answered, here or anywhere else. */
export function refreshPendingApprovals(qc: QueryClient): void {
  void qc.invalidateQueries({ queryKey: queryKeys.cliApprovals.pending() });
}

/**
 * Read the waiting requests again when a snapshot's ids differ from the cached
 * ones. The server sends a snapshot only when something in it moved, often a
 * thread, so an unchanged list is not read again. A connection's first
 * snapshot (`resync`) from a server that names none still reads them, since an
 * event may have been missed while detached.
 */
export function reconcilePendingApprovals(qc: QueryClient, ids: string[] | undefined, resync: boolean): void {
  if (!ids) {
    if (resync) refreshPendingApprovals(qc);
    return;
  }
  const cached = qc.getQueryData<CliApproval[]>(queryKeys.cliApprovals.pending());
  if (cached) {
    const known = new Set(cached.map((a) => a.approval_id));
    if (known.size === ids.length && ids.every((id) => known.has(id))) return;
  }
  refreshPendingApprovals(qc);
}
