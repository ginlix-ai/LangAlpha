import { useEffect, useEffectEvent, useState, type Dispatch } from 'react';
import type { ArtifactComment } from '@/types/api';
import type { FrameView } from '../BridgedFrame';
import type { PageBridge } from '../usePageBridge';
import type { BoxAction, BoxState } from './boxReducer';
import { DRAFT_ID, type PinPlaces } from './pinPlaces';
import type { CommentFocus } from './useCommentUi';
import type { PageReports } from './usePageReports';

/**
 * How long a card brought forward from the chat waits for the page's scroll
 * to land its pin in view before it opens anyway, or goes to the list.
 */
const LANDING_BACKSTOP_MS = 2000;

interface BoxPlacementOptions {
  /** A chat line's ask to show a comment on the file the frame holds, on the tab in front; null for none. */
  focus: CommentFocus | null;
  /** The layer showed the ask with this key. */
  focusHandled: (key: number) => void;
  /** The comments are read, or the file has none to wait for. */
  loaded: boolean;
  bridge: PageBridge;
  view: FrameView;
  reports: PageReports;
  places: PinPlaces;
  byId: ReadonlyMap<string, ArtifactComment>;
  /** Comments the list keeps whatever its filter says (`useListView`): a card opened there keeps the filter. */
  keptIds: ReadonlySet<string>;
  box: BoxState;
  /** The layer's, which opens the list when a move puts a card or the draft in it. */
  dispatch: Dispatch<BoxAction>;
}

/**
 * Where the open box goes as the page moves under it. A chat line naming a
 * comment brings it forward: it waits for the list, for the document to be
 * on screen and for the page to say where the comment is, and a page that
 * never says sends it to the list instead. A box carried onto a new version
 * is placed at its pin there, or sent to the list. Returns the move that
 * opens a card in the list, which a press on a pin or a row makes too.
 */
export function useBoxPlacement({
  focus, focusHandled, loaded, bridge, view, reports, places, byId, keptIds, box, dispatch,
}: BoxPlacementOptions): (comment: ArtifactComment, bringPin?: boolean) => void {
  const { rectOf, landedAt, whereNow, draftLanded } = places;
  const { positions, fresh, settled, doc } = reports;
  const { listOpen, open, landing, carried } = box;

  /** Open the comment's card in the list, and bring its pin into view when it has one, unless `bringPin` is false. */
  const expandInList = (comment: ArtifactComment, bringPin = true) => {
    if (bringPin && rectOf(comment)) bridge.post({ type: 'page:scroll-to', id: comment.comment_id });
    dispatch({ type: 'expand', comment, kept: keptIds.has(comment.comment_id) });
  };

  /**
   * While the list is open its card opens there, since a card beside a pin
   * would land on the list whenever the pin sits under it; otherwise at its
   * pin once the pin is in view, scrolling the page to it first, or in the
   * list when it has none here.
   */
  const showComment = (comment: ArtifactComment) => {
    if (listOpen || !rectOf(comment)) expandInList(comment);
    else if (landedAt(comment)) dispatch({ type: 'brought', id: comment.comment_id });
    else {
      bridge.post({ type: 'page:scroll-to', id: comment.comment_id });
      dispatch({ type: 'awaitLanding', id: comment.comment_id });
    }
  };

  // The card opens once its pin has come into view and the scroll has
  // stopped, popping out of the pin where the reader's eye already is. A card
  // opened at once rode the scroll hidden and cut in at the frame's edge.
  // Anything drawn can move the pin, so each render looks while a card waits.
  const landInView = useEffectEvent(() => {
    const comment = landing && !reports.scrolling ? byId.get(landing.id) : undefined;
    if (comment && landedAt(comment)) dispatch({ type: 'landed', id: comment.comment_id });
  });
  useEffect(() => {
    if (landing) landInView();
  });
  const landAnyway = useEffectEvent(() => {
    if (!landing) return;
    const comment = byId.get(landing.id);
    if (comment && !rectOf(comment)) expandInList(comment);
    else dispatch({ type: 'landed', id: comment?.comment_id ?? null });
  });
  useEffect(() => {
    if (!landing) return;
    const timer = setTimeout(() => landAnyway(), LANDING_BACKSTOP_MS);
    return () => clearTimeout(timer);
  }, [landing]);

  const focusKey = focus?.key ?? null;
  const [expired, setExpired] = useState<number | null>(null);
  useEffect(() => {
    if (focusKey === null || !loaded) return;
    const timer = setTimeout(() => setExpired(focusKey), bridge.ready ? 3000 : 8000);
    return () => clearTimeout(timer);
  }, [focusKey, loaded, bridge.ready]);
  const handleFocus = useEffectEvent(() => {
    if (!focus || !loaded) return;
    const late = expired === focus.key;
    if (!view.shown && !late) return;
    const comment = byId.get(focus.commentId);
    const where = comment ? whereNow(comment) : null;
    if (comment && where === undefined && !late) return;
    focusHandled(focus.key);
    if (!comment) return;
    if (where) showComment(comment);
    else expandInList(comment);
  });
  useEffect(() => {
    handleFocus();
  }, [focusKey, loaded, positions, reports.settledDoc, expired, view.key, view.shown, doc]);

  // A box left open across a version change goes to the list once the new
  // version is on screen, has finished building, and does not show its spot.
  // One whose spot the new version shows out of view goes there too, and the
  // page stays where the reader had it: the box was held where it was, and a
  // box at a pin out of view is hidden, so it would just have vanished. A
  // card does this whenever its spot goes; a draft, whose spot was picked on
  // the version it came from, only when carried.
  const keepBoxPlaced = useEffectEvent(() => {
    if (!view.shown) return;
    if (open?.kind === 'draft') {
      if (carried !== doc || !fresh || !positions.has(DRAFT_ID)) return;
      const rect = positions.get(DRAFT_ID) ?? null;
      if (rect === null && !settled) return;
      if (rect && draftLanded()) dispatch({ type: 'placed' });
      else dispatch({ type: 'listDraft' });
      return;
    }
    if (open?.kind !== 'card' || listOpen) return;
    const comment = byId.get(open.id);
    if (!comment) return;
    const where = whereNow(comment);
    if (where === null) expandInList(comment);
    else if (where && carried === doc) {
      if (landedAt(comment)) dispatch({ type: 'placed' });
      else expandInList(comment, false);
    }
  });
  useEffect(() => {
    keepBoxPlaced();
  }, [positions, reports.settledDoc, view.shown, view.key, doc, carried]);

  return expandInList;
}
