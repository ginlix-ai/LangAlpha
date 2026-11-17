/**
 * The workspace a call created, found in the workspace lists the app holds by
 * the id the create printed, which a rename keeps. A name is not enough: once
 * that workspace is deleted, a later one may take its name, and the line
 * would open that one instead. A workspace `la` made never passed through the
 * app's own create, so no list read before it holds it: a line that cannot
 * find its workspace reads every workspace once as it shows, which comes after
 * the call returned. That read starts afresh, since one already in flight may
 * have left before the create, and a list's update time is no proof either: a
 * patch to another workspace's row moves it too.
 */
import { useContext, useEffect, useSyncExternalStore } from 'react';
import { QueryClientContext } from '@tanstack/react-query';
import { allWorkspacesQuery } from '@/hooks/useAllWorkspaces';
import { cachedWorkspaceLists } from '../utils/warmWorkspace';

export function useCreatedWorkspace(createdId: string | undefined): string | null {
  const qc = useContext(QueryClientContext);
  const subscribe = (onChange: () => void) => (qc && createdId ? qc.getQueryCache().subscribe(onChange) : () => {});
  const read = (): string | null =>
    (qc &&
      createdId &&
      cachedWorkspaceLists(qc).find((w) => w.workspace_id === createdId && w.status !== 'deleted')?.workspace_id) ||
    null;
  const id = useSyncExternalStore(subscribe, read, read);

  useEffect(() => {
    if (!qc || !createdId || id) return;
    const query = allWorkspacesQuery('custom', false);
    void qc
      .cancelQueries({ queryKey: query.queryKey, exact: true })
      .then(() => qc.prefetchQuery({ ...query, staleTime: 0 }));
  }, [qc, id, createdId]);

  return id;
}
