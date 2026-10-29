/**
 * Saving a drag in the gallery grid.
 *
 * The grid draws straight from the Custom list in the query cache, so a drop
 * rewrites that cache in place (the new order and each row's `sort_order`)
 * before the write goes out, and the card stays where it was dropped instead
 * of snapping back for the round trip. Every other cached list in the custom
 * order is rewritten the same way, the sidebar's included: its tree reads a
 * changed `sort_order` as a deliberate reordering and takes the new order at
 * once (a reorder made in the sidebar rewrites them through the same
 * `rankCachedWorkspaces`). A refused write puts every list back and says so;
 * a good one refetches them all.
 *
 * A drop made while the list is being read waits for that read and moves
 * from what it brings: the cached list may be older than the server's.
 *
 * The order is the one the sidebar writes too (`useNavigationData`'s
 * `reorderWorkspace`): the pinned block, Home counted in it, ahead of the
 * rest, a move never crossing between the two, and every row, Home included,
 * written with its slot. Home is not a card, so it never moves here: the
 * cards move around it and it keeps the slot it had.
 */
import { useCallback, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';

import { toast } from '@/components/ui/use-toast';
import { allWorkspacesKey } from '@/hooks/useAllWorkspaces';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { queryKeys } from '@/lib/queryKeys';
import type { WorkspacesResponse } from '@/types/api';

import { reorderWorkspaces } from '../../utils/api';
import { isEffectivelyPinned } from '../../hooks/useNavigationData';
import { rankCachedWorkspaces, unrankCachedWorkspaces, type WorkspaceQueriesSnapshot } from '../../hooks/workspaceRowActions';
import type { WorkspaceRecord } from './types';

/** The list the gallery shows under Custom: every workspace, Home included. */
export const CUSTOM_LIST_KEY = allWorkspacesKey('custom', true);

type Item = { workspace_id: string; sort_order: number };

/** The pinned block (Home counts as pinned) ahead of the rest, each in its own order. */
function pinnedFirst<T extends WorkspaceRecord>(rows: readonly T[]): T[] {
  const pinned: T[] = [];
  const rest: T[] = [];
  for (const ws of rows) (isEffectivelyPinned(ws) ? pinned : rest).push(ws);
  return [...pinned, ...rest];
}

/**
 * The whole order after moving `activeId` onto `overId`'s slot, or null when
 * the move is not one to make: Home on either end, a row the list does not
 * have, or a move across the pin boundary.
 */
export function reorderedList<T extends WorkspaceRecord>(rows: readonly T[], activeId: string, overId: string): T[] | null {
  if (activeId === overId) return null;
  const ordered = pinnedFirst(rows);
  const cards = ordered.filter((ws) => !isHomeWorkspace(ws));
  const from = cards.findIndex((ws) => ws.workspace_id === activeId);
  const to = cards.findIndex((ws) => ws.workspace_id === overId);
  if (from === -1 || to === -1) return null;
  if (isEffectivelyPinned(cards[from]) !== isEffectivelyPinned(cards[to])) return null;

  const moved = [...cards];
  const [row] = moved.splice(from, 1);
  moved.splice(to, 0, row);
  // Home keeps its slot; the cards fill the others in their new order.
  let next = 0;
  return ordered.map((ws) => (isHomeWorkspace(ws) ? ws : moved[next++]));
}

export function useGalleryReorder() {
  const queryClient = useQueryClient();
  const { t } = useTranslation();
  const [isSaving, setIsSaving] = useState(false);
  // A ref as well as the state: a second drop can land before the render that
  // would disable the cards, and it must not write over the first.
  const inFlightRef = useRef(false);

  /** Move one card onto another's slot and save the order. Resolves whether it was saved. */
  const move = useCallback(async (activeId: string, overId: string): Promise<boolean> => {
    if (inFlightRef.current) return false;
    /** The whole order after the move, read off the cached list; null when there is no move to make. */
    const plan = (): Item[] | null => {
      const current = queryClient.getQueryData<WorkspacesResponse>(CUSTOM_LIST_KEY);
      const next = current && reorderedList(current.workspaces as WorkspaceRecord[], activeId, overId);
      return next ? next.map((ws, i) => ({ workspace_id: ws.workspace_id, sort_order: i })) : null;
    };
    // A read of the list in flight may bring an order newer than the cached
    // one: a reorder just made in the sidebar, say. The move waits for it and
    // is read off what it brings. Cancelling it instead would put the older
    // list back, and the older order would be written over the newer.
    const reading = queryClient.getQueryState(CUSTOM_LIST_KEY)?.fetchStatus === 'fetching';
    const planned = reading ? null : plan();
    if (!reading && !planned) return false;

    inFlightRef.current = true;
    setIsSaving(true);
    let previous: WorkspaceQueriesSnapshot | null = null;
    try {
      // A read that fails leaves the older order cached, and a move read off
      // that would write it over the newer one: the drop is refused instead.
      if (reading) await queryClient.refetchQueries({ queryKey: CUSTOM_LIST_KEY, exact: true }, { cancelRefetch: false, throwOnError: true });
      const items = reading ? plan() : planned;
      if (!items) return false;
      previous = rankCachedWorkspaces(queryClient, items);
      await reorderWorkspaces(items);
      return true;
    } catch (err) {
      console.error('Error reordering workspaces:', err);
      if (previous) unrankCachedWorkspaces(queryClient, previous);
      toast({ variant: 'destructive', title: t('workspace.reorderFailed') });
      return false;
    } finally {
      inFlightRef.current = false;
      setIsSaving(false);
      if (previous) void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    }
  }, [queryClient, t]);

  /**
   * Take a drop from the grid: false, at once, when a save still in flight
   * turns it away, so it is not announced as dropped.
   */
  const drop = useCallback((activeId: string, overId: string): boolean => {
    if (inFlightRef.current) return false;
    void move(activeId, overId);
    return true;
  }, [move]);

  return { move, drop, isSaving };
}
