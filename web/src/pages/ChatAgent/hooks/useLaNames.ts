/**
 * Names for the ids `la` steps give, from what the query cache already holds:
 * the workspace and thread lists and details the app has read for its own
 * surfaces. Nothing is fetched for a label, so a name the cache lacks stays
 * unnamed, and a step shows its title alone rather than a guess.
 */
import { useCallback, useContext, useMemo, useSyncExternalStore } from 'react';
import { QueryClientContext, type QueryClient } from '@tanstack/react-query';
import { cachedThreadTitle } from '@/lib/threadListCache';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import type { Workspace } from '@/types/api';
import { laLookupId, laStepOf } from '../utils/laCommand';
import { cachedWorkspaceLists } from '../utils/warmWorkspace';
import { cachedHomeRow, workspaceDetailQuery } from '../utils/workspaceQueries';
import { laNameKey, type LaNames } from '../components/laStepLabels';

/** A call as the rows hold it. */
export interface LaCall {
  toolName?: string;
  args?: Readonly<Record<string, unknown>> | null;
}

function cachedWorkspace(qc: QueryClient, id: string): Workspace | undefined {
  const detail = qc.getQueryData(workspaceDetailQuery(id).queryKey);
  if (detail?.workspace_id === id) return detail;
  return cachedHomeRow(qc, id) ?? cachedWorkspaceLists(qc).find((w) => w.workspace_id === id);
}

/**
 * The names for the ids the given calls' `la` steps name, read from the cache.
 * The hook re-renders only when one of those names changes: the snapshot is
 * the names found, in the calls' order, so a cache event that leaves them
 * alone keeps the same string and the same map. A row drawn with no query
 * client (a static render) has no cache to read, so it names nothing.
 */
export function useLaNames(calls: readonly LaCall[]): LaNames {
  const qc = useContext(QueryClientContext);
  const label = useWorkspaceLabel();
  const lookups = calls.flatMap((call) => laLookupId(laStepOf(call.toolName, call.args)) ?? []);
  const named = lookups.length > 0 && qc != null;
  const subscribe = useCallback(
    (onChange: () => void) => (named && qc ? qc.getQueryCache().subscribe(onChange) : () => {}),
    [qc, named],
  );
  const read = (): string => {
    if (!qc) return '[]';
    const found = lookups.flatMap((lookup): Array<[string, string]> => {
      const ws = lookup.type === 'workspace' ? cachedWorkspace(qc, lookup.id) : undefined;
      const name = lookup.type === 'thread' ? cachedThreadTitle(qc, lookup.id) : ws && label(ws);
      return name ? [[laNameKey(lookup), name]] : [];
    });
    return JSON.stringify(found);
  };
  const snapshot = useSyncExternalStore(subscribe, read, read);
  return useMemo(() => new Map(JSON.parse(snapshot) as Array<[string, string]>), [snapshot]);
}
