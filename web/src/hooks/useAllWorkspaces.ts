import { useQuery, keepPreviousData } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import type { Workspace } from '@/types/api';
import { getWorkspaces } from '../pages/ChatAgent/utils/api';

// Apart from useWorkspaces.ts because the entry imports that module, and only
// lazy pages list every workspace: code here would ride every first paint.

// The list endpoint's cap on one page.
const WORKSPACE_PAGE_SIZE = 100;
// Full reads to try before settling for a short one.
const MAX_READS = 3;

/**
 * Every workspace, one capped page at a time. The pages are offsets into an
 * order that can shift between reads: a row that moves up past the read is
 * missed and the one it pushes down comes back twice. The second copy is
 * dropped, and a read that ends with fewer rows than the total starts again
 * from the top, so a drag never saves an order built off a short list. After
 * a few tries it keeps what it has rather than read forever.
 */
export async function getAllWorkspaces(
  sortBy: string,
  includeHome: boolean,
): Promise<Workspace[]> {
  for (let read = 1; ; read++) {
    const rows: Workspace[] = [];
    const seen = new Set<string>();
    let offset = 0;
    let total: number | undefined;

    while (true) {
      const page = await getWorkspaces(
        WORKSPACE_PAGE_SIZE,
        offset,
        sortBy,
        includeHome,
      );
      offset += page.workspaces.length;
      for (const ws of page.workspaces) {
        if (seen.has(ws.workspace_id)) continue;
        seen.add(ws.workspace_id);
        rows.push(ws);
      }
      total = typeof page.total === 'number' ? page.total : undefined;
      const reachedTotal = total !== undefined && offset >= total;
      if (reachedTotal || page.workspaces.length < WORKSPACE_PAGE_SIZE) break;
    }
    if (total === undefined || rows.length >= total || read === MAX_READS) return rows;
  }
}

/** The cache key `useAllWorkspaces` keeps a list under, for a writer that patches it. */
export const allWorkspacesKey = (sortBy: string, includeHome: boolean) =>
  queryKeys.workspaces.list({ view: 'all', sortBy, includeHome });

/**
 * Every workspace, page by page, for a surface that lists or counts all of
 * them: one page stops at 100, and a count read off a truncated list is wrong
 * without looking wrong. Cached in the list shape so an optimistic row patch
 * reaches it too.
 */
export function useAllWorkspaces({ sortBy = 'custom', includeHome = false, enabled = true }: { sortBy?: string; includeHome?: boolean; enabled?: boolean } = {}) {
  return useQuery({
    queryKey: allWorkspacesKey(sortBy, includeHome),
    queryFn: async () => {
      const workspaces = await getAllWorkspaces(sortBy, includeHome);
      return { workspaces, total: workspaces.length };
    },
    enabled,
    staleTime: 30_000,
    // A new sort is a new key: hold the last list on screen while it loads
    // rather than dropping back to a loading state.
    placeholderData: keepPreviousData,
  });
}
