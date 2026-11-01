import { useState } from 'react';
import type { ArtifactComment } from '@/types/api';
import { inFilter, type ListFilter } from '../../../artifactComments/commentModel';

const NO_KEPT: ReadonlyMap<string, ArtifactComment['status']> = new Map();

export interface ListView {
  /**
   * Comments kept in the list whatever the filter now says of them: a card
   * opened here, or one shown unread and read since.
   */
  keptIds: ReadonlySet<string>;
  /** The list is open and shows the comment. */
  lists: (comment: ArtifactComment) => boolean;
}

/**
 * Which comments the list shows under `filter`, and which it keeps showing
 * once the filter says they no longer belong, so nothing moves under the
 * pointer while the reader works through it.
 */
export function useListView(
  comments: readonly ArtifactComment[],
  byId: ReadonlyMap<string, ArtifactComment>,
  listOpen: boolean,
  filter: ListFilter,
  expandedId: string | null,
): ListView {
  // The row whose card is open in the list stays in the view it was opened
  // in until it is shut or the list closes, whatever the agent or the reader
  // does to the comment meanwhile; then it moves to where its status puts it.
  const holding = listOpen ? expandedId : null;
  const [held, setHeld] = useState<{ id: string; filter: ListFilter } | null>(null);
  if ((held?.id ?? null) !== holding) {
    const comment = holding ? byId.get(holding) : undefined;
    const next = comment && inFilter(comment, filter) ? { id: comment.comment_id, filter } : null;
    if (next || held) setHeld(next);
  }
  // A card the list showed unread stays once it is read, until the reader
  // leaves the filter or closes the list, so nothing moves under the pointer:
  // in Unread the cards worked through, in Open a resolved one whose reply
  // was read. Each is kept in the status it had, so a resolve or a reopen
  // still moves it as it would any card.
  const keepScope = listOpen ? filter : null;
  const [kept, setKept] = useState<{ scope: ListFilter | null; ids: ReadonlyMap<string, ArtifactComment['status']> }>(
    () => ({ scope: keepScope, ids: NO_KEPT }),
  );
  const keptBase = kept.scope === keepScope ? kept.ids : NO_KEPT;
  const keeping = keepScope === null
    ? []
    : comments.filter((c) => c.unread > 0 && inFilter(c, keepScope) && keptBase.get(c.comment_id) !== c.status);
  if (kept.scope !== keepScope || keeping.length > 0) {
    const ids = new Map(keptBase);
    for (const c of keeping) ids.set(c.comment_id, c.status);
    setKept({ scope: keepScope, ids });
  }
  const keptIds = new Set<string>();
  for (const [id, status] of keptBase) if (byId.get(id)?.status === status) keptIds.add(id);
  if (held && held.filter === filter) keptIds.add(held.id);
  return {
    keptIds,
    lists: (c) => listOpen && (inFilter(c, filter) || keptIds.has(c.comment_id)),
  };
}
