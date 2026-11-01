import React, { useEffect, useMemo, useReducer, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  artifactPathKey, useArtifactComments, useArtifactStatus, useMarkArtifactCommentsRead, useOwnedArtifact,
} from '@/hooks/useArtifact';
import { AnimatePresence, motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import type { ArtifactComment } from '@/types/api';
import { basename } from '../../../../utils/fileRefResolver';
import { useWorkspaceFolders } from '../../../../contexts/WorkspaceContext';
import type { FrameView } from '../BridgedFrame';
import type { PageAnchorItem, PagePick } from '../pageProtocol';
import type { PageBridge } from '../usePageBridge';
import { boxReducer, initialBox, pressCloses, type BoxAction, type BoxOf } from './boxReducer';
import { CommentCard, type CardPlace } from './CommentCard';
import { CommentComposer, type ComposerMode } from './CommentComposer';
import { CommentDestination } from './CommentDestination';
import { CommentLayerContext } from './commentLayerContext';
import { CommentList, DraftCard } from './CommentList';
import { CommentPins } from './CommentPin';
import { CommentPopover } from './CommentPopover';
import { draftHeading } from '../../../artifactComments/commentModel';
import { PickOverlays } from './PickOverlays';
import { DRAFT_ID, placePins } from './pinPlaces';
import type { CommentIntent } from '../../../artifactComments/destinations';
import { useBoxPlacement } from './useBoxPlacement';
import type { CommentUi } from './useCommentUi';
import { useContentSize } from './useContentSize';
import { useLayerSending } from './useLayerSending';
import { useEachKeptWhileFading } from './useKeptWhileFading';
import { useListView } from './useListView';
import { useModeKeys } from './useModeKeys';
import { usePageReports } from './usePageReports';
import { useReadWhileOpen } from './useReadWhileOpen';
import { useReplyPulse } from './useReplyPulse';
import './CommentLayer.css';

/** Below this width the card opens under its pin, and the list covers the page. */
const NARROW = 560;
/**
 * The box over a pin, in px. The English ticked footer ("For the agent",
 * Send now, "Add to queue ↵") measures 297 px in the UI face, and a card's
 * reply box gives it 44 px less than the box; this leaves both some slack.
 * Narrower, the buttons wrap below the label rather than squeeze it.
 */
const BOX_WIDTH = 352;
const EMPTY: ArtifactComment[] = [];
const OVER: CardPlace = { kind: 'over' };
const HINT_HIDDEN = { opacity: 0, y: -4 };
const HINT_SHOWN = { opacity: 1, y: 0, transition: TWEEN.quick };
const HINT_GONE = { opacity: 0, y: -4, transition: TWEEN.exit };

/** A box over the page, with what it is called and what it holds: kept as it was while it leaves. */
interface Box {
  key: string;
  label: string;
  of: BoxOf;
}

export interface CommentLayerProps {
  tabId: string;
  workspaceId: string;
  /** The file the frame was opened on; a link inside it can move the frame to another (`bridge.path`). */
  path: string;
  bridge: PageBridge;
  view: FrameView;
  /** The tab is the one in front of an active panel: keys and asks are its. */
  active: boolean;
  ui: CommentUi;
}

/**
 * The comment layer over the owner's live HTML file: pins where the owner
 * commented, the outline of what a click would pick while comment mode is
 * on, the offer to comment on selected words, the boxes a new comment is
 * written in and a comment's card opens in, and the list of every comment.
 *
 * The comments are the record's of the file the frame holds: the one the tab
 * opened, or another its links led to. The layer makes that file's record if
 * it has none, as opening it in a tab would, so where its comments go is the
 * server's answer from the start; the toolbar hears which file it is.
 *
 * The document only reports (`usePageReports`), and a comment is only ever
 * written from the owner's own action here. What is open over the page and
 * in the list moves through `boxReducer`.
 */
export default function CommentLayer({
  tabId, workspaceId, path, bridge, view, active, ui,
}: CommentLayerProps): React.ReactElement {
  const { t } = useTranslation();
  const [root, setRoot] = useState<HTMLDivElement | null>(null);
  const size = useContentSize(root);
  const narrow = size.width > 0 && size.width < NARROW;

  // The file the frame holds, as its hello named it; until it says, the one opened.
  const rootDir = useWorkspaceFolders()?.rootDir;
  const opened = artifactPathKey(path, rootDir);
  const docPath = artifactPathKey(bridge.path ?? path, rootDir);
  const { showDoc } = ui;
  useEffect(() => showDoc(tabId, opened, docPath), [showDoc, tabId, opened, docPath]);
  const { record: artifact, error: recordError } = useOwnedArtifact(workspaceId, docPath);
  const artifactId = artifact?.artifact_id ?? null;
  const commentsQuery = useArtifactComments(artifactId);
  const comments = commentsQuery.data ?? EMPTY;
  const byId = useMemo(() => new Map(comments.map((c) => [c.comment_id, c])), [comments]);
  // A file that cannot have a record has no comments to wait for.
  const loaded = recordError !== null || commentsQuery.isSuccess;

  const mode = ui.modeTab === tabId;
  const listOpen = ui.listTab === tabId;
  const doc = `${bridge.session}:${view.key}`;
  const [box, dispatchBox] = useReducer(boxReducer, { doc, listOpen }, initialBox);
  if (box.doc !== doc) dispatchBox({ type: 'docChanged', doc });
  if (box.listOpen !== listOpen) dispatchBox({ type: 'listShown', open: listOpen, comments });
  // A move that puts a card or the draft in the list opens it with the same
  // update, so the list shows them in the commit that took them there. Opened
  // from an effect, a commit later, the card would be open nowhere between,
  // and its read asked again once it showed.
  const dispatch = (action: BoxAction) => {
    dispatchBox(action);
    if (boxReducer(box, action).listWanted) ui.setList(tabId, true);
  };
  const { open } = box;
  const boxDraft = open?.kind === 'draft' ? open.draft : null;
  // The comment being written, in its box over the page or waiting in the list.
  const draft = boxDraft ?? box.listed?.draft ?? null;
  // The live file's hash, stored with a new comment. Read only once one is
  // being written, for a file with comments to compare or published: each
  // read copies the file's whole list off the computer.
  const { data: status } = useArtifactStatus(artifactId, {
    enabled: !!artifact && !!draft && (artifact.open_comments > 0 || artifact.latest_seq > 0),
  });
  const sending = useLayerSending({
    workspaceId,
    docPath,
    draftPath: draft?.path ?? docPath,
    ui,
    to: box.to,
    onTo: (target) => dispatch({ type: 'to', target }),
  });
  const list = useListView(comments, byId, listOpen, box.filter, box.expandedId);

  // Words picked in comment mode stay marked in the page while their spot
  // shows: under the box, or in the list. Words the chip offered keep the
  // reader's own selection, as they always have; a mark under it would show
  // through only where the quote grew past it.
  const draftMarked = draft?.selected === true && (boxDraft !== null || listOpen);
  // The spots to find on this document: every comment on its file, and the
  // draft's while the frame shows the file it was picked on, so its outline
  // follows the document as it scrolls.
  const draftHere = !!draft && draft.path === docPath;
  const anchors = useMemo(() => {
    const items: PageAnchorItem[] = comments.map((c) => ({ id: c.comment_id, anchor: c.anchor }));
    if (draft && draftHere) items.push(draftMarked ? { id: DRAFT_ID, anchor: draft.anchor, mark: true } : { id: DRAFT_ID, anchor: draft.anchor });
    return items;
  }, [comments, draft, draftHere, draftMarked]);

  const startDraft = (pick: PagePick, selected = false) => dispatch({ type: 'pick', pick, path: docPath, selected });
  const closeBox = (of: BoxOf) => dispatch({ type: 'closeBox', of, keepHover: false });
  const leaveDraft = () => dispatch({ type: 'leave' });
  const reports = usePageReports(bridge, view, mode, {
    anchors,
    draftSerial: box.serial,
    onHover: (hover) => dispatch({ type: 'hover', hover }),
    onPress: () => {
      const closes = pressCloses(box);
      dispatch({ type: 'pressAway' });
      return closes;
    },
    onPick: startDraft,
    // The page's keys reach the viewer only when they would here: C toggles
    // the mode, Esc steps back out of it. Neither writes anything.
    onKey: (key) => {
      if (key === 'c') ui.setMode(tabId, !mode);
      else if (mode && boxDraft) leaveDraft();
      else if (mode && open?.kind === 'card') closeBox(open);
      else if (mode) ui.setMode(tabId, false);
    },
  });
  const { positions, fresh, settled } = reports;
  useModeKeys(active, mode, open !== null, (on) => ui.setMode(tabId, on));

  // Words picked in comment mode stay selected in a page that cannot mark
  // them, until their draft goes.
  const draftSelected = draft?.selected === true;
  const heldSelection = useRef(false);
  useEffect(() => {
    if (heldSelection.current && !draftSelected) bridge.post({ type: 'page:clear-selection' });
    heldSelection.current = draftSelected;
  }, [bridge, draftSelected]);

  const shownId = open?.kind === 'card' ? open.id : listOpen ? box.expandedId : null;
  const places = placePins({
    comments,
    positions,
    fresh,
    settled,
    shown: view.shown,
    size,
    shownId,
    lists: list.lists,
    draft,
    draftHere,
    draftShown: boxDraft !== null || listOpen,
  });
  const { draftRect, draftPinned, rectOf, whereNow, landedAt, draftLanded } = places;
  const pinsHidden = !fresh || !view.shown;

  const expandInList = useBoxPlacement({
    focus: active && ui.focus && artifactPathKey(ui.focus.path, rootDir) === docPath ? ui.focus : null,
    focusHandled: ui.focusHandled,
    loaded,
    bridge,
    view,
    reports,
    places,
    byId,
    keptIds: list.keptIds,
    box,
    dispatch,
  });

  const saveDraft = async (intent: CommentIntent) => {
    const route = sending.draftRoute(intent);
    if (!draft || !route) return;
    const comment = await sending.layer.actions.create({
      path: draft.path,
      anchor: draft.anchor,
      body: box.text.trim(),
      route,
      fileSha: draft.path === docPath ? (status?.live_sha256 ?? null) : null,
      title: draft.title,
    });
    // The new pin takes the draft's place until the page reports it.
    if (draftRect) reports.placeSpot(comment.comment_id, draftRect);
    dispatch({ type: 'saved', id: comment.comment_id });
    if (draft.anchor.kind === 'text') {
      bridge.post({ type: 'page:clear-selection' });
      reports.clearSelection();
    }
  };

  const shownComment = shownId ? (byId.get(shownId) ?? null) : null;
  useReadWhileOpen(artifactId, shownId, shownComment, active);
  const pulse = useReplyPulse(comments, loaded && !commentsQuery.isFetching, shownId);
  const markAllRead = useMarkArtifactCommentsRead(artifactId);

  const onPin = (comment: ArtifactComment) => {
    // A press outside a draft with words in it asks before it lets them go.
    if (boxDraft && box.text.trim()) dispatch({ type: 'askDiscard' });
    else if (listOpen) expandInList(comment);
    else if (open?.kind === 'card' && open.id === comment.comment_id) closeBox(open);
    else dispatch({ type: 'pinPressed', id: comment.comment_id });
  };
  const onRow = (comment: ArtifactComment) => {
    if (box.expandedId === comment.comment_id) dispatch({ type: 'collapse' });
    else expandInList(comment);
  };

  const renderCard = (comment: ArtifactComment, outdated: boolean, isOpen: boolean, place: CardPlace) => (
    <CommentCard
      comment={comment}
      outdated={outdated}
      open={isOpen}
      place={place}
      onDeleted={() => dispatch({ type: 'deleted' })}
    />
  );

  // An area's outline settles on the draft's area, or on that of the comment
  // whose card is open, here or in the list; it goes out with the pins while
  // the document under them is being replaced.
  const shownArea = shownComment?.anchor.kind === 'area' ? (rectOf(shownComment) ?? null) : null;
  const draftArea = draft?.anchor.kind === 'area' && draftPinned ? draftRect : null;
  const openComment = open?.kind === 'card' ? (byId.get(open.id) ?? null) : null;
  const heading = draft ? draftHeading(t, draft.anchor) : null;
  const current: Box | null = open?.kind === 'draft' && heading
    ? { key: `draft:${open.serial}`, label: heading.label, of: { kind: 'draft', serial: open.serial } }
    : openComment
      ? {
          key: `card:${openComment.comment_id}`,
          label: t('pageViewer.comments.cardTitle', { number: openComment.number }),
          of: { kind: 'card', id: openComment.comment_id },
        }
      : null;
  const { shown: boxes, gone } = useEachKeptWhileFading(current);
  // Held where it was while the version on screen is not yet the one the
  // page reports for, and a box carried onto it until its pin is in view.
  const parked = box.carried === doc && (openComment ? !landedAt(openComment) : boxDraft !== null && !draftLanded());
  const currentPoint = !view.shown || parked
    ? null
    : boxDraft
      ? places.pinAt(null)
      : openComment
        ? places.pinAt(openComment.comment_id)
        : null;
  const popoverWidth = Math.max(220, Math.min(BOX_WIDTH, size.width - 24));

  // The words being written, in the draft's box or its card in the list: the
  // same words either way, and the field takes the focus wherever they go.
  // Over the page the composer is the box and names the spot; in the list
  // the card around it does.
  const composer = (inList: boolean) => {
    const writing = {
      forAgent: box.forAgent,
      onForAgentChange: (on: boolean) => dispatch({ type: 'forAgent', on }),
      destination: <CommentDestination {...sending.destination} />,
      confirmDiscard: box.confirmDiscard,
      onKeepWriting: () => dispatch({ type: 'keepWriting' }),
      onDiscard: () => dispatch({ type: 'endDraft', keepHover: false }),
    };
    const as: ComposerMode = !inList && heading
      ? { kind: 'over', head: heading.label, quote: heading.quote, fullQuote: draft?.anchor.kind === 'text', ...writing }
      : { kind: 'list', ...writing };
    return (
      <CommentComposer
        value={box.text}
        onChange={(text) => dispatch({ type: 'write', text })}
        placeholder={t('pageViewer.comments.placeholder')}
        canQueue={sending.draftRoute('queue') !== null}
        canSend={sending.draftRoute('send') !== null}
        onSubmit={saveDraft}
        mode={as}
      />
    );
  };

  return (
    <CommentLayerContext value={sending.layer}>
      <div ref={setRoot} className="page-comments" data-mode={mode || undefined} data-narrow={narrow || undefined}>
        <CommentPins
          pins={places.pins}
          shownId={shownId}
          draftSerial={open?.kind === 'draft' ? open.serial : (box.listed?.serial ?? null)}
          pulse={pulse}
          madeId={box.madeId}
          size={size}
          pointerIn={reports.pointerIn}
          hidden={pinsHidden}
          scrolling={reports.scrolling}
          onPreview={() => bridge.post({ type: 'page:pointer-watch' })}
          onPin={onPin}
        />

        <PickOverlays
          mode={mode}
          boxOpen={open !== null}
          hover={box.hover}
          areaDrag={reports.areaDrag}
          picked={boxDraft?.anchor.kind === 'element' && draftRect ? draftRect : null}
          area={pinsHidden ? null : (draftArea ?? shownArea)}
          scrolling={reports.scrolling}
          selection={boxDraft ? null : reports.selection}
          size={size}
          drafting={draft !== null}
          onComment={(selection) => startDraft(selection)}
        />

        {/* Gone while the list is open: the drawer's title sits where the hint
            is centred, and the list already says the mode is on. */}
        <AnimatePresence>
          {mode && !listOpen && (
            <motion.div key="hint" className="page-comments-hint-row" initial={HINT_HIDDEN} animate={HINT_SHOWN} exit={HINT_GONE}>
              <div className="page-comments-hint" role="status">
                {t('pageViewer.comments.modeHint')}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        <AnimatePresence initial={false}>
          {listOpen && (
            <CommentList
              key="list"
              comments={comments}
              filter={box.filter}
              onFilter={(filter) => dispatch({ type: 'filter', filter })}
              expandedId={box.expandedId}
              keptIds={list.keptIds}
              draft={box.listed && heading && (
                <DraftCard
                  label={heading.label}
                  quote={heading.quote}
                  tag={!draftHere
                    ? basename(box.listed.draft.path)
                    : draftRect ? null : t('pageViewer.comments.outdated')}
                  onLeave={leaveDraft}
                >
                  {composer(true)}
                </DraftCard>
              )}
              onToggle={onRow}
              onMarkAllRead={() => markAllRead.mutate()}
              renderCard={(c, expanded, onToggle) => renderCard(c, whereNow(c) === null, expanded, { kind: 'list', onToggle })}
              onClose={() => ui.setList(tabId, false)}
            />
          )}
        </AnimatePresence>

        {boxes.map((item) => {
          const isCurrent = item.key === current?.key;
          return (
            <CommentPopover
              key={item.key}
              open={isCurrent}
              point={isCurrent ? currentPoint : null}
              narrow={narrow}
              boundary={root}
              width={popoverWidth}
              label={item.label}
              focusSelf={item.of.kind === 'card'}
              arrival={isCurrent && openComment !== null && box.arrivalId === openComment.comment_id}
              replaced={current !== null}
              draft={isCurrent && boxDraft
                ? { words: !!box.text.trim(), onLeave: leaveDraft, onAskDiscard: () => dispatch({ type: 'askDiscard' }) }
                : null}
              onDismiss={() => closeBox(item.of)}
              onGone={() => gone(item.key)}
            >
              {!isCurrent ? null : boxDraft ? (
                composer(false)
              ) : openComment ? (
                renderCard(openComment, false, true, OVER)
              ) : null}
            </CommentPopover>
          );
        })}
      </div>
    </CommentLayerContext>
  );
}
