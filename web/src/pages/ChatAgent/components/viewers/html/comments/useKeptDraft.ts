import { useEffect, useRef, useState, type Dispatch } from 'react';
import { useStorageUser } from '@/lib/userStorage';
import type { ArtifactKind } from '@/types/api';
import { boxReducer, initialBox, type BoxAction, type BoxState, type Draft } from './boxReducer';
import { dropDraft, keepDraft, readDraft } from './commentDrafts';

/** Where a draft is kept: the workspace, and the file or chart it is on. */
interface Place {
  workspaceId: string;
  kind: ArtifactKind;
  path: string;
}

/**
 * The comment being written, kept in the browser (`commentDrafts`) as it is
 * written and taken up again when the layer comes to show what it was on.
 *
 * A draft keeps its file's path but not its workspace: saved after a move to
 * another (the market page's workspace switch), it would land on that
 * workspace's file of the same path. So it stays kept for its own workspace,
 * and the layer takes up the new workspace's draft on what it shows, if any.
 * Moved to another file or chart in the same workspace, a draft in hand goes
 * along where none is kept. Where one is, that one comes back and the draft
 * in hand stays kept where it was written: a place keeps one, and a spot
 * picked there would write over it. With none in hand, the one kept there
 * comes back at once. `askedHere` is
 * a card asked for on what the layer now shows (another timeframe's, from the
 * list): it opens in the box's place, so the draft waits in the list.
 */
export function useKeptDraft({ workspaceId, kind, path, box, draft, dispatchBox, askedHere }: Place & {
  box: BoxState;
  /** The comment being written, in its box over the page or waiting in the list. */
  draft: Draft | null;
  dispatchBox: Dispatch<BoxAction>;
  askedHere: boolean;
}): void {
  // Drafts are kept per signed-in user, and an account switch in place leaves
  // the layer mounted: the next user's are not the last one's.
  const user = useStorageUser();
  const [shown, setShown] = useState({ user, workspaceId, kind, path });
  // The place of a draft in hand set down for the one kept where the layer
  // moved: it stays kept there, though the box lets go of it.
  const [setDown, setSetDown] = useState<string | null>(null);
  if (shown.user !== user || shown.workspaceId !== workspaceId || shown.kind !== kind || shown.path !== path) {
    setShown({ user, workspaceId, kind, path });
    const kept = readDraft(workspaceId, kind, path);
    const elsewhere = shown.user !== user || shown.workspaceId !== workspaceId;
    const yields = !elsewhere && !!draft && !!kept && draft.path !== path;
    setSetDown(yields ? draft.path : null);
    if (elsewhere || yields || !draft) {
      dispatchBox({ type: 'restore', kept, replace: elsewhere || yields, behind: askedHere });
    }
  }

  // Kept as it is written, and let go once discarded, emptied or sent, while
  // what is kept there is still what this layer wrote. A move to another
  // workspace or account lets go of nothing, the draft being that one's, nor
  // does a draft set down.
  const keptAt = useRef<(Place & { user: string | null; entry: string }) | null>(null);
  const { text, forAgent, to } = box;
  useEffect(() => {
    const keeping = draft && text.trim() ? draft : null;
    const was = keptAt.current;
    if (
      was && was.user === user && was.workspaceId === workspaceId && was.kind === kind
      && was.path !== keeping?.path && was.path !== setDown
    ) {
      dropDraft(was.workspaceId, was.kind, was.path, was.entry);
    }
    const entry = keeping ? keepDraft(workspaceId, kind, { draft: keeping, text, forAgent, to }) : null;
    keptAt.current = keeping && entry ? { user, workspaceId, kind, path: keeping.path, entry } : null;
  }, [user, workspaceId, kind, draft, text, forAgent, to, setDown]);
}

/** The box a layer opens with: on the comment last being written on what it shows, if any. */
export function openingBox(start: { doc: string; listOpen: boolean }, { workspaceId, kind, path }: Place): BoxState {
  return boxReducer(initialBox(start), { type: 'restore', kept: readDraft(workspaceId, kind, path), replace: false });
}
