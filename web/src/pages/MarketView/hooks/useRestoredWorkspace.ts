import { useEffect, useMemo, useRef, useState } from 'react';
import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import type { Workspace } from '@/types/api';
import { apiErrorStatus } from '../../ChatAgent/utils/api';
import { workspaceDetailQuery } from '../../ChatAgent/utils/workspaceQueries';
import { loadPref, savePref } from '../utils/prefs';

interface RestoredWorkspaceOptions {
  /** A workspace the opening link named; it is trusted outright. */
  linkedId: string | null | undefined;
  workspaces: Workspace[];
  /** The account's workspace count, so a short page can be told from the whole list. */
  total: number | undefined;
  isFetchedAfterMount: boolean;
  isSuccess: boolean;
  /** The restored workspace is gone from the list, so the view falls back to its default state. */
  onDropped: () => void;
}

type Standing = 'open' | 'gone' | 'unknown';

/**
 * Whether this account can still select the workspace, asked of the server
 * directly. The answer lands in the detail query, so the panel that opens the
 * workspace next reads it rather than asking again. A cached copy is fetched
 * over, since only an answer from this mount counts, and a refusal is not
 * asked twice.
 */
async function checkStanding(queryClient: QueryClient, workspaceId: string): Promise<Standing> {
  try {
    const workspace = await queryClient.fetchQuery({ ...workspaceDetailQuery(workspaceId), staleTime: 0, retry: false });
    // The list this check stands in for leaves Home out.
    return isHomeWorkspace(workspace) ? 'gone' : 'open';
  } catch (err) {
    const status = apiErrorStatus(err);
    return status === 404 || status === 403 ? 'gone' : 'unknown';
  }
}

/**
 * The workspace MarketView scopes its requests to. An id restored from storage
 * can name a workspace this account cannot open (deleted since, or saved by
 * someone else on this browser), and every workspace-scoped request made with
 * it comes back 403. So it is withheld as null until the reconcile below has
 * checked it, and the raw id is never handed out.
 */
export function useRestoredWorkspace({
  linkedId, workspaces, total, isFetchedAfterMount, isSuccess, onDropped,
}: RestoredWorkspaceOptions) {
  const queryClient = useQueryClient();
  const [id, setId] = useState<string | null>(
    () => linkedId || loadPref<string | null>('selectedWorkspaceId', null),
  );
  // The id this mount restored from storage, null when the link named one. It
  // is the only id the reconcile below judges.
  const [restoredId] = useState(() => (linkedId ? null : id));
  // The restored id itself, not a flag: choosing that same id again before the
  // check has run is still a choice of an unchecked id, while any other id
  // releases the hold by no longer matching.
  const [unverifiedId, setUnverifiedId] = useState(restoredId);
  const pending = id !== null && id === unverifiedId;
  const listed = id !== null && workspaces.some((ws) => ws.workspace_id === id);

  useEffect(() => {
    savePref('selectedWorkspaceId', id);
  }, [id]);

  // Reconcile the restored selection against the list, once per mount. If the
  // selected workspace is gone (deleted between visits), drop back to a clean
  // default state (the Chief of Staff + new chat) instead of silently picking a
  // different PTC workspace the user didn't ask for. Resolve both decisions
  // before calling either setter so neither updater runs a side effect on the
  // other piece of state.
  //
  // Gated on a successful fetch that happened during this mount, not merely on
  // data being present: the query has a 30s staleTime, so a remount inside
  // that window is served from cache synchronously and a workspace deleted
  // elsewhere is still in that copy. `isFetchedAfterMount` also flips on an
  // error update, hence `isSuccess`: a failed list load must not read as
  // "your workspace is gone" and clear the selection.
  //
  // Once per mount, deliberately. Absence from this list does not mean
  // deleted: it is one page of 50 ordered by recency, so an idle selection
  // falls off it on its own as other workspaces get activity. Re-running on
  // every refetch would clear the selection mid-session on a window focus, and
  // MarketChatPanel reads a workspace change as a scope change the user made
  // and discards the open thread.
  //
  // So when the account has more workspaces than the page holds, a restored id
  // missing from it is asked about directly while still held, and dropped only
  // on a not-found or forbidden answer. That one request names the id. Storage
  // is per user, so the id is this account's own (bar an unscoped value from
  // before that, adopted once), and a drop replaces the stored id: a stale one
  // draws a single refusal, not one per load. A page that holds every
  // workspace settles it alone, naming nothing.
  //
  // A workspace named by the link is trusted outright: the chart tab built
  // that link from a workspace that was open seconds earlier, and one older
  // than the page's fifty is exactly the case this check would misread as
  // deleted, dropping the thread the link carried. So is an id selected since
  // mount, from the selector or a later link: it is the user's choice, and a
  // check that lands after it (the first success after a failed load, or the
  // direct check) must not overrule it.
  //
  // A failed load releases the restored id without judging it: its requests
  // may 403, but a list outage must not leave PTC mode unusable. A failed
  // check does the same.
  const reconciledRef = useRef(false);
  const checkedRef = useRef(false);
  const [standing, setStanding] = useState<Standing | null>(null);
  useEffect(() => {
    if (reconciledRef.current || !isFetchedAfterMount) return;
    const judged = !linkedId && id === restoredId;
    if (isSuccess && judged && id && !listed && !standing && (total ?? 0) > workspaces.length) {
      if (!checkedRef.current) {
        checkedRef.current = true;
        void checkStanding(queryClient, id).then(setStanding);
      }
      return;
    }
    setUnverifiedId(null);
    if (!isSuccess) return;
    reconciledRef.current = true;
    if (!judged || listed || (standing && standing !== 'gone')) return;
    if (id) onDropped();
    setId(workspaces[0]?.workspace_id ?? null);
  }, [isFetchedAfterMount, isSuccess, workspaces, listed, total, standing, id, linkedId, restoredId, onDropped, queryClient]);

  // A selection from past the page (one the check kept, or one a link named)
  // is still the selected workspace, so the selector lists it after the page.
  // The restored id is the check's: it fetched the detail already, or failed
  // and declined to retry. Any other id is fetched here once the page has
  // shown it missing, since on a phone no chat panel mounts to ask for it.
  const fetchUnlisted = id !== null && id !== restoredId && isSuccess && !listed;
  const { data: unlisted } = useQuery({ ...workspaceDetailQuery(id ?? ''), enabled: fetchUnlisted });
  const selectable = useMemo(
    () => (unlisted && unlisted.workspace_id === id && !listed ? [...workspaces, unlisted] : workspaces),
    [unlisted, id, listed, workspaces],
  );

  return { selectedWorkspaceId: pending ? null : id, pending, select: setId, workspaces: selectable };
}
