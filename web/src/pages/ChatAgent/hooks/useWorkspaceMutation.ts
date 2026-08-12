import { useCallback, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { formatApiErrorDetail } from '../utils/api';
import { patchCachedWorkspace, rollbackCachedWorkspaces } from './workspaceRowActions';

export interface UseWorkspaceMutationOptions<A> {
  /** Runs the network request for one workspace. */
  mutationFn: (wsId: string, args: A) => Promise<unknown>;
  /** Optional cache patch applied optimistically before the request; rolled back on error. */
  optimisticPatch?: (args: A) => Record<string, unknown>;
  /** Also invalidate the per-tier quota query on success. */
  invalidateQuota?: boolean;
  /** i18n key for the failure toast title. */
  errorTitleKey: string;
  /** Map an error to the failure toast description (defaults to formatApiErrorDetail). */
  mapError?: (err: unknown, args: A) => string;
}

export interface UseWorkspaceMutationResult<A> {
  /** Run the mutation. Resolves true on success, false on dedupe-skip or error. */
  run: (wsId: string, args: A) => Promise<boolean>;
  /** Workspaces with a mutation currently in flight. */
  busyIds: Set<string>;
}

/**
 * Shared skeleton for per-workspace mutations: race-safe busy tracking →
 * optimistic patch → request → invalidate lists + detail (+quota) → rollback +
 * console.error + toast on error → clear busy. Success side effects (closing a
 * dialog, success toast) stay with the caller, gated on the returned boolean.
 */
export function useWorkspaceMutation<A>(
  options: UseWorkspaceMutationOptions<A>,
): UseWorkspaceMutationResult<A> {
  const queryClient = useQueryClient();
  const { t } = useTranslation();
  const [busyIds, setBusyIds] = useState<Set<string>>(() => new Set());
  const optionsRef = useRef(options);
  optionsRef.current = options;

  const run = useCallback(
    async (wsId: string, args: A): Promise<boolean> => {
      const { mutationFn, optimisticPatch, invalidateQuota, errorTitleKey, mapError } = optionsRef.current;

      // Dedupe inside the functional update so a fast double-submit (two calls in
      // one render frame, both seeing a stale closure) can't fire twice.
      let alreadyBusy = false;
      setBusyIds((prev) => {
        if (prev.has(wsId)) { alreadyBusy = true; return prev; }
        return new Set(prev).add(wsId);
      });
      if (alreadyBusy) return false;

      const previous = optimisticPatch ? patchCachedWorkspace(queryClient, wsId, optimisticPatch(args)) : null;
      try {
        await mutationFn(wsId, args);
        queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
        queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.detail(wsId) });
        if (invalidateQuota) queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() });
        return true;
      } catch (err) {
        if (previous) rollbackCachedWorkspaces(queryClient, previous);
        console.error(errorTitleKey, err);
        toast({
          variant: 'destructive',
          title: t(errorTitleKey),
          description: mapError ? mapError(err, args) : formatApiErrorDetail(err),
        });
        return false;
      } finally {
        setBusyIds((prev) => { const n = new Set(prev); n.delete(wsId); return n; });
      }
    },
    [queryClient, t],
  );

  return { run, busyIds };
}
