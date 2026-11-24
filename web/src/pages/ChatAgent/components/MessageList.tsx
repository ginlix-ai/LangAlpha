import React from 'react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { CardColumn } from './charts/TermCard';
import { useStableArray } from '@/hooks/useStableArray';
import { NotificationDivider } from './messageList/NotificationDivider';
import { WakeupRow } from './messageList/WakeupRow';
import { QuietChecksRow } from './messageList/QuietChecksRow';
import { MessageBubble } from './messageList/MessageBubble';
import { SharedThreadContext } from './messageList/SharedThreadContext';
import { bubbleSender, continuesSender, type BubbleSender } from '../session/sharedThread';
import { TurnFold } from './messageList/TurnFold';
import { AgentPlan } from './messageList/AgentPlan';
import type { ShownPlan } from './messageList/requestPlans';
import { contextHead, holdsAnswerText, projectMessageContent, visibleProjection } from './messageList/contentProjection';
import { renderNotification } from './messageList/notices';
import type { NotificationRenderBlock } from './messageList/buildRenderBlocks';
import { useMessageActions } from './messageList/MessageActionsContext';
import { isReportRow, leadingContextNotices } from './messageList/messagePredicates';
import { ReportRow, type ReportProvenance } from './messageList/ReportRow';
import { planReportChartCards } from './chartLayerGrouping';
import { HandoffPlacesContext, placeHandoffs, samePlaces } from './analystHandoff/reportLinks';
import {
  computeReplyEnds, computeTurnTails, hasTurn, hiddenOpenerTurns, liveTurns, messageTimeMs, projectTurns, turnSpans, turnStartTimes, type ProjectedMessage,
} from './messageList/turnProjection';
import { planQuietChecks, quietEndsByTurn } from './messageList/quietChecks';
import type { QuietEnd } from '../session/wakeupEvents';
import { CollapseClock, CollapseClockContext } from './messageList/transcriptMotion';
import { ProseTyping, ProseTypingContext } from './messageList/proseTyping';
import { useLiveMessages, type LiveMessages } from '../session/stream/liveMessages';
import type { ChatMessage, WakeupArrival } from '@/types/chat';
import { turnFilesByTurn } from '../utils/turnFiles';
import { useCloudTurnFiles } from '../hooks/useCloudTurnFiles';
import { useWorkspaceFolders } from '../contexts/WorkspaceContext';
import type { FeedbackResult, FoldState, MessageRecord } from './messageList/types';

const NO_QUIET_ENDS: ReadonlyMap<number, QuietEnd> = new Map();
/** Between messages of one member's run: the action row under each already
 *  spaces them, so the list's own gap would hold them a message apart. */
const RUN_GAP: React.CSSProperties = { marginTop: 8 };

/** What the fold row for one backend turn needs to know about it. */
interface TurnFoldInfo {
  /** Index into the visible list of the turn's FIRST assistant bubble: the row
   *  goes immediately above it, below the message that asked for the turn. */
  rowPos: number;
  startedAt: number;
  completedAt?: number;
  isLive: boolean;
  /** Nothing stands for the turn once folded, so it settles to `process`. */
  noAnswer: boolean;
  /** A wake turn that ended quietly: its closing line stands as its answer. */
  quietEnd: QuietEnd | null;
  /** The turn has process or commentary to fold away. One that ran here with
   *  none keeps the row it worked under, as a label. */
  foldable: boolean;
  /** The turn's opening message was sent from this view. */
  sentHere: boolean;
}

/**
 * Which turns the list draws as running, and which its view has drawn running.
 *
 * A turn that stops while its reply is still typing out is drawn running until
 * the typing ends. Settling folds the turn's work away above the reply and
 * brings the Sources pill and the deck in below it: done under a sentence still
 * being typed, that moved the line being read by the height of the work and
 * showed the files before the sentence naming them. A turn settles once: text
 * that types again afterwards (a reconcile rewriting it) does not reopen it.
 * One loaded settled was never drawn running, so it settles at once.
 *
 * The turns that ran here are also kept in `remembered`, which the view owns:
 * its main list unmounts while a subagent tab is open, and a list coming back
 * that had forgotten them dropped the row of every turn that ran here with
 * nothing to fold, moving the transcript up a row for each.
 */
function useTurnPresentation(
  live: ReadonlySet<number>,
  turned: readonly ProjectedMessage[],
  typing: ProseTyping,
  remembered?: Set<number>,
): { running: ReadonlySet<number>; ranHere: ReadonlySet<number> } {
  const [shown, setShown] = React.useState(() => ({
    running: live,
    ranHere: remembered?.size ? new Set([...remembered, ...live]) : live,
  }));
  React.useEffect(() => {
    if (remembered) for (const turn of shown.ranHere) remembered.add(turn);
  }, [remembered, shown.ranHere]);
  let next = shown;
  let running: Set<number> | null = null;
  let ranHere: Set<number> | null = null;
  for (const turn of live) {
    if (!shown.running.has(turn)) (running ??= new Set(shown.running)).add(turn);
    if (!shown.ranHere.has(turn)) (ranHere ??= new Set(shown.ranHere)).add(turn);
  }
  if (running || ranHere) {
    next = { running: running ?? shown.running, ranHere: ranHere ?? shown.ranHere };
    setShown(next);
  }
  // Only the prose knows whether it is still typing, and it says so from
  // layout effects, which run before this one in the same commit. Settling in
  // render decided before the commit's text had answered, so a reply whose
  // last words came with the end of its stream settled under them. A turn
  // still typing waits here for its prose to call back.
  const drawnRunning = next.running;
  React.useLayoutEffect(() => {
    const waiting = [...drawnRunning].filter((turn) => !live.has(turn));
    if (waiting.length === 0) return;
    const settle = () => {
      const typingTurns = new Set<number | null>();
      for (const { message, turnIndex } of turned) {
        if (typing.isTyping(message.id as string)) typingTurns.add(turnIndex);
      }
      const done = waiting.filter((turn) => !typingTurns.has(turn));
      if (done.length === 0) return;
      setShown((prev) => {
        if (!done.some((turn) => prev.running.has(turn))) return prev;
        const settled = new Set(prev.running);
        for (const turn of done) settled.delete(turn);
        return { ...prev, running: settled };
      });
    };
    settle();
    return typing.subscribe(settle);
  }, [drawnRunning, live, turned, typing]);
  return next;
}

// --- MessageList ---

interface MessageListProps {
  messages: MessageRecord[];
  isLoading?: boolean;
  isLoadingHistory?: boolean;
  isSubagentView?: boolean;
  readOnly?: boolean;
  allowFiles?: boolean;
  /** Feedback keyed by BACKEND TURN — the projection below maps each bubble to
   *  its turn, so a steering continuation shows the continued turn's rating. */
  feedbackByTurn?: Record<number, FeedbackResult>;
  dispatcherContext?: { threadId: string; workspaceId: string } | null;
  /** A first turn is about to land (the welcome flow's), so an empty list is
   *  not an empty chat and draws no prompt to type. */
  starting?: boolean;
  /** The turns this view has drawn running, kept by the view so they outlive
   *  the list (see `useTurnPresentation`). */
  ranHere?: Set<number>;
  /** Each request's plan, drawn under the message that asked for it, by
   *  message id (`threadPlans`): the host leaves out the one its tray shows. */
  plans?: ReadonlyMap<string, ShownPlan>;
  /** The thread holds its newest turn before the model can take it up: a
   *  compaction running, or a send waiting behind one, with nothing done yet
   *  (see `heldTurn`). */
  holding?: boolean;
  /** What holds it, drawn above its message: a compaction's line, where the
   *  divider its end leaves goes. */
  holdLine?: React.ReactNode;
}

interface HeldTranscript {
  messages: MessageRecord[];
  plans?: ReadonlyMap<string, ShownPlan>;
}

function MessageList({ messages: latest, isLoading, isLoadingHistory, isSubagentView, readOnly, allowFiles, feedbackByTurn, dispatcherContext, starting, ranHere: rememberedRanHere, plans: latestPlans, holding, holdLine }: MessageListProps): React.ReactElement | null {
  // A history load writes the transcript an event at a time as its response
  // arrives. Drawn as it went, a finished turn showed its work open above no
  // answer and then folded it away once the answer and its end landed. The
  // list keeps what it drew before the load until the load ends, whose last
  // commit holds the whole transcript (loadConversationHistory's reveal). A
  // list that mounts mid-load (main shown again while the thread loads) drew
  // nothing before it, and what the load has written so far is the half it
  // must not show, so it holds nothing, which draws the skeleton. The plans
  // are held with it, since they are read off the messages being rewritten.
  const [held, setHeld] = React.useState<HeldTranscript | null>(() => (isLoadingHistory ? { messages: [] } : null));
  if (isLoadingHistory && held === null) setHeld({ messages: latest, plans: latestPlans });
  if (!isLoadingHistory && held !== null) setHeld(null);
  const messages = isLoadingHistory ? held?.messages ?? latest : latest;
  const plans = isLoadingHistory && held ? held.plans : latestPlans;
  const isMobile = useIsMobile();
  const { onOpenFile } = useMessageActions();
  // The deck keeps the workspace's own notes file out on every surface; its
  // folders, when the host knows them, let it read a path that names the
  // folder, and place one that names a sibling's.
  const folders = useWorkspaceFolders();

  // Session memory, never auto-cleared: a fold the reader opened stays open
  // until they close it or reload. Only the turn they touched is addressed, so
  // a new turn arriving cannot snap an older one shut under them.
  const [expandedTurns, setExpandedTurns] = React.useState<Set<number>>(() => new Set());
  const toggleTurn = React.useCallback((turnIndex: number) => {
    setExpandedTurns((prev) => {
      const next = new Set(prev);
      if (next.has(turnIndex)) next.delete(turnIndex);
      else next.add(turnIndex);
      return next;
    });
  }, []);

  // ONE raw projection pass carries the turn semantics (edit/regenerate/
  // feedback all address backend turns); orphan filtering and the regenerate
  // tail then run over the VISIBLE list, so a hidden bubble never steals an
  // affordance from a painted one.
  const projected = React.useMemo(() => projectTurns(messages), [messages]);
  // A chart a report row cards is carded there for its whole turn, so every
  // projection of that turn's messages leaves it out, with the same set, or
  // the projection cache would answer one reader with another's build.
  const reportCharts = React.useMemo(() => planReportChartCards(projected), [projected]);
  const rootDir = folders?.rootDir;
  const projectWhole = React.useCallback(
    (message: MessageRecord) => projectMessageContent(message, isSubagentView, rootDir, reportCharts.omitByMessage.get(message)),
    [isSubagentView, rootDir, reportCharts],
  );
  const visible = React.useMemo(
    () => visibleProjection(projected, isSubagentView, rootDir, (message) => reportCharts.omitByMessage.get(message)),
    [projected, isSubagentView, rootDir, reportCharts],
  );
  // Where each turn sits in the list: its message and its replies.
  const spans = React.useMemo(() => turnSpans(visible), [visible]);
  // A turn's first reply is drawn without the context notices it opens with,
  // which go above the turn's message (`contextHead`), so nothing the turn
  // folds, times or rates counts them. Only the bubble the turn wrote first:
  // when that one is not drawn, the next one's notices came mid-turn.
  const firstReplies = React.useMemo(() => {
    const firsts = new Set([...turnSpans(projected).values()].flatMap(({ replies }) => (replies.length ? [projected[replies[0]].message] : [])));
    return new Set([...spans.values()].flatMap(({ replies }) => {
      const first = replies.length ? visible[replies[0]].message : null;
      return first && firsts.has(first) ? [first] : [];
    }));
  }, [projected, spans, visible]);
  const headOf = React.useCallback(
    (message: MessageRecord) => contextHead(projectWhole(message), leadingContextNotices(message.contentSegments)),
    [projectWhole],
  );
  const project = React.useCallback(
    (message: MessageRecord) => (firstReplies.has(message) ? headOf(message)?.rest : undefined) ?? projectWhole(message),
    [projectWhole, firstReplies, headOf],
  );
  // Each head by the entry it is drawn above: the turn's message, or its
  // first reply when no message opened it (a wake turn).
  const heads = React.useMemo(() => {
    const byEntry = new Map<number, NotificationRenderBlock[]>();
    for (const { opener, replies } of spans.values()) {
      const first = replies.length ? visible[replies[0]].message : null;
      const head = first && firstReplies.has(first) ? headOf(first)?.head : undefined;
      if (head) byEntry.set(opener ?? replies[0], head);
    }
    return byEntry;
  }, [spans, visible, firstReplies, headOf]);
  const turnTails = React.useMemo(() => computeTurnTails(visible), [visible]);
  const replyEnds = React.useMemo(() => computeReplyEnds(visible, turnTails), [visible, turnTails]);
  // The bubbles of the server's turns. A send it never stored is painted
  // like any other but has no turn to time, fold, rate or list files under.
  const turned = React.useMemo(() => projected.filter(hasTurn), [projected]);
  // A turn that opened hidden has no message a regenerate could run again.
  const hiddenTurns = React.useMemo(() => hiddenOpenerTurns(turned), [turned]);
  // Retry re-runs the thread's latest attempt and replaces everything from
  // its reply on, so only a reply nothing was sent after offers it.
  const latestReplyId = React.useMemo(() => {
    const last = visible.findLast(({ message }) => message.role !== 'notification')?.message;
    return last?.role === 'assistant' ? last.id : undefined;
  }, [visible]);
  // Each hand-off on screen with the report that answers it, so its slip can
  // link down to that report rather than away to the thread. Kept while it
  // says the same: the list rebuilds on every streamed frame, and a new map
  // each time would redraw every slip with it.
  const placeEntries = useStableArray(React.useMemo(() => placeHandoffs(visible), [visible]), samePlaces);
  const places = React.useMemo(() => new Map(placeEntries), [placeEntries]);

  // The deliverables strip reads the RAW projection: a turn's files are named
  // across its whole span, including a bubble the list never paints.
  const ownFilesByTurn = React.useMemo(
    () => turnFilesByTurn(turned, folders),
    [turned, folders],
  );
  // A Home turn that ran on the cloud names its files where they were then.
  const filesByTurn = useCloudTurnFiles(turned, ownFilesByTurn, { enabled: !readOnly });

  // Only the newest turn can still be running. A bubble's own `isStreaming`
  // is per model call, not per turn: the text handler drops it on every
  // content-free finish_reason and the next chunk re-arms it, so between two
  // model calls of one agent loop it reads false. The session's `isLoading`
  // spans the whole turn, tool execution included, and is the truth here.
  // Read from the raw projection: in that same gap a bubble whose blocks so
  // far are all hidden tools is not painted, and the visible list would hand
  // the running turn's liveness to the one before it.
  const newestTurn = React.useMemo(() => {
    let newest = -1;
    for (const { message, turnIndex } of turned) {
      if ((message.role as string) === 'assistant' && turnIndex > newest) newest = turnIndex;
    }
    return newest;
  }, [turned]);

  const live = React.useMemo(
    () => liveTurns(turned, newestTurn, !!isLoading, (message) => project(message).hasPinnedLive),
    [turned, newestTurn, isLoading, project],
  );

  // The running turn while the thread holds it ahead of any work: a
  // compaction before its first call, or a send waiting behind one. Its
  // message has not reached the model yet, so it draws as a steer not yet
  // taken up does, and the turn draws no row or dots: the hold's own line,
  // above the message, is all that is happening.
  const heldTurn = holding && live.has(newestTurn) ? newestTurn : null;
  // The turns this list drew held, whose row arrives once the hold ends
  // rather than with the send, and fades in as it would have then.
  const [wasHeld, setWasHeld] = React.useState<ReadonlySet<number>>(() => new Set());
  if (heldTurn !== null && !wasHeld.has(heldTurn)) setWasHeld(new Set([...wasHeld, heldTurn]));

  const [typing] = React.useState(() => new ProseTyping());
  const { running, ranHere } = useTurnPresentation(live, turned, typing, rememberedRanHere);
  const [collapseClock] = React.useState(() => new CollapseClock());

  // How each settled turn ended quietly. A subagent's transcript has no wake
  // turns, so it reads none.
  const quietEnds = React.useMemo(
    () => (isSubagentView ? NO_QUIET_ENDS : quietEndsByTurn(turned, live)),
    [turned, live, isSubagentView],
  );

  // Both display preferences fold finished turns; only live reasoning differs.
  // The same pass times every settled turn, whose reply shows when it ended.
  const { turnFolds, turnEnds } = React.useMemo(() => {
    const folds = new Map<number, TurnFoldInfo>();
    const ends = new Map<number, number>();

    const startedAt = turnStartTimes(visible);

    // The turn's end is stamped on its tail bubble, and that bubble is not
    // always painted: a steering continuation that settled empty is dropped
    // from `visible` by `isOrphanAssistantMessage` while still carrying the
    // stamp stream finalization and both replay paths wrote to it. Read from
    // the raw list, so a turn that ended on one is still timed. Last write
    // wins, which is the latest stamped bubble of the turn.
    const endStamps = new Map<number, MessageRecord>();
    for (const { message, turnIndex } of turned) {
      if ((message.role as string) !== 'assistant') continue;
      if (message.completedAt !== undefined || message.completionObservedAt !== undefined) {
        endStamps.set(turnIndex, message);
      }
    }

    for (let i = 0; i < visible.length; i++) {
      // The turn's end is stamped on its tail bubble, which is also the only
      // bubble that can be streaming once the turn has several.
      if (!turnTails[i]) continue;
      const { message, turnIndex } = visible[i];
      if (turnIndex === null) continue;
      const { opener = null, replies: bubbles = [] } = spans.get(turnIndex) ?? {};
      const contents = bubbles.map((b) => project(visible[b].message));
      const isLive = running.has(turnIndex);
      // A pinned call outlives the stream that launched it, so the stream-close
      // stamp was taken before the turn's work was done and a fold reading it
      // counts backwards, from `Working for 2m` to `Worked for 5s`. The server's
      // own stamp still wins outright; this only sharpens the local fallback.
      const endStamp = endStamps.get(turnIndex) ?? message;
      const observedEnd = messageTimeMs(endStamp.completionObservedAt);
      const localEnd = contents.reduce<number | undefined>(
        (end, content) => (content.pinnedSettledAt !== null && (end === undefined || content.pinnedSettledAt > end)
          ? content.pinnedSettledAt
          : end),
        observedEnd,
      );
      const completedAt = live.has(turnIndex) ? undefined : messageTimeMs(endStamp.completedAt) ?? localEnd;
      if (completedAt !== undefined) ends.set(turnIndex, completedAt);
      const start = startedAt.get(turnIndex);
      const pos = bubbles[0];
      if (start === undefined || pos === undefined) continue;
      // A running turn has its row from the first frame, whatever it has done
      // so far: a row that waited for something foldable arrived above prose
      // already typing and pushed it down a line. A settled turn with nothing
      // to fold has no row, unless it ran here and the row is already there.
      const foldable = contents.some((content) => content.hasProcess) || contents.reduce((sum, content) => sum + content.textCount, 0) >= 2;
      if (!foldable && !isLive && !ranHere.has(turnIndex)) continue;
      const hasFiles = !!onOpenFile && (!readOnly || allowFiles) && (filesByTurn.get(turnIndex)?.length ?? 0) > 0;
      const quietEnd = isLive ? null : quietEnds.get(turnIndex) ?? null;
      folds.set(turnIndex, {
        // Read only for a settled turn, and cheapest first: whether a text holds
        // an answer can mean scoring it.
        noAnswer: !isLive && !(hasFiles || quietEnd || contents.some((content) => content.hasRetained)
          || bubbles.some((b) => holdsAnswerText(project(visible[b].message), replyEnds[b]))),
        quietEnd,
        rowPos: pos,
        startedAt: start,
        completedAt,
        isLive,
        foldable,
        sentHere: opener !== null && visible[opener].message.sentHere === true,
      });
    }
    return { turnFolds: folds, turnEnds: ends };
  }, [visible, spans, turned, turnTails, replyEnds, live, running, ranHere, project, filesByTurn, onOpenFile, readOnly, allowFiles, quietEnds]);

  const items = React.useMemo(() => planQuietChecks(visible, quietEnds), [visible, quietEnds]);
  // In a shared workspace's thread, the entries that carry on another
  // member's run of messages, read against the entry drawn just above. A
  // plan drawn under a message ends the run, since it sits between.
  const sharedView = React.useContext(SharedThreadContext);
  const continuing = React.useMemo(() => {
    const runs = new Set<number>();
    if (!sharedView) return runs;
    let above: BubbleSender | undefined;
    for (const item of items) {
      if (item.kind !== 'entry') {
        above = undefined;
        continue;
      }
      const { message } = visible[item.index];
      const sender = bubbleSender(sharedView, message as ChatMessage);
      if (continuesSender(above, sender)) runs.add(item.index);
      above = plans?.has(message.id as string) ? undefined : sender;
    }
    return runs;
  }, [items, visible, sharedView, plans]);

  // Empty state - show when no messages exist (hidden in subagent view)
  if (messages.length === 0) {
    if (isSubagentView || starting) return null;
    if (isLoadingHistory) {
      return (
        <div className="space-y-6 py-4 animate-pulse">
          {/* User message skeleton */}
          <div className="flex justify-end">
            <div className="rounded-2xl" style={{ background: 'var(--color-border-muted)', width: '55%', height: 40 }} />
          </div>
          {/* Assistant message skeleton */}
          <div className="flex">
            <div className="flex-1 space-y-3">
              <div className="rounded" style={{ background: 'var(--color-border-muted)', width: '80%', height: 14 }} />
              <div className="rounded" style={{ background: 'var(--color-border-muted)', width: '65%', height: 14 }} />
              <div className="rounded" style={{ background: 'var(--color-border-muted)', width: '40%', height: 14 }} />
            </div>
          </div>
          {/* Second user message skeleton */}
          <div className="flex justify-end">
            <div className="rounded-2xl" style={{ background: 'var(--color-border-muted)', width: '40%', height: 40 }} />
          </div>
          {/* Second assistant skeleton */}
          <div className="flex">
            <div className="flex-1 space-y-3">
              <div className="rounded" style={{ background: 'var(--color-border-muted)', width: '90%', height: 14 }} />
              <div className="rounded" style={{ background: 'var(--color-border-muted)', width: '70%', height: 14 }} />
            </div>
          </div>
        </div>
      );
    }
    return (
      <div className="flex flex-col items-center justify-center min-h-full py-12">
        <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          Start a conversation by typing a message below
        </p>
      </div>
    );
  }

  // The host renders DispatchStatusProvider above the list, so every
  // hand-off in it shares one batched dispatch-liveness query and timer.
  // Here it re-rendered, and re-ran its query hook, on every streamed frame.
  // sibling-space-y, not space-y: NotificationDivider's own my-1 would
  // otherwise replace the gap below it.
  const list = (
    <ProseTypingContext value={typing}>
    <CollapseClockContext value={collapseClock}>
    {/* Measured, so the cards lay out for this column: a desktop chat panel
        beside a chart is narrower than any phone layout assumes. */}
    <CardColumn className={`font-content ${isMobile ? 'sibling-space-y-4' : 'sibling-space-y-6'}`}>
      {items.map((item) => {
        if (item.kind === 'quiet') {
          return <QuietChecksRow key={`quiet-checks-${item.checks[0].turnIndex}`} checks={item.checks} />;
        }
        const i = item.index;
        const { message, turnIndex } = visible[i];
        if ((message.role as string) === 'notification') {
          const wakeup = message.wakeup as WakeupArrival | undefined;
          return wakeup
            ? <WakeupRow key={message.id as string} wakeup={wakeup} />
            : <NotificationDivider key={message.id as string} message={message} />;
        }
        const inHeldTurn = turnIndex !== null && turnIndex === heldTurn;
        // What sits above the turn's message: a compaction before it, settled
        // or still running.
        const head = heads.get(i);
        const holdAbove = inHeldTurn && spans.get(turnIndex)?.opener === i;
        const above = (
          <>
            {head?.map((block) => <React.Fragment key={block.key}>{renderNotification(block.segment)}</React.Fragment>)}
            {holdAbove && holdLine}
          </>
        );
        if (isReportRow(message)) {
          const prev = visible[i - 1]?.message;
          // A fragment, not a wrapper: `together` closes the list's own gap.
          return (
            <React.Fragment key={message.id as string}>
              {above}
              <ReportRow
                id={message.id as string}
                provenance={message.provenance as ReportProvenance}
                together={!head && !holdAbove && !!prev && isReportRow(prev)}
                readOnly={readOnly}
                dispatcherContext={dispatcherContext}
                chartCards={reportCharts.byReport.get(message.id as string)}
              />
            </React.Fragment>
          );
        }
        // A bubble only ever folds when its turn shows the row that unfolds it:
        // a turn with no fold row (no initiating message) keeps the verbose
        // shape rather than hiding prose behind nothing, as does a settled turn
        // with nothing to fold.
        const info = turnIndex === null ? undefined : turnFolds.get(turnIndex);
        const fold: FoldState = !info
          ? 'unfolded'
          : info.isLive
            ? 'live'
            : !info.foldable
              ? 'unfolded'
              : turnIndex !== null && expandedTurns.has(turnIndex) ? 'expanded' : info.noAnswer ? 'process' : 'collapsed';
        const bubbleProps = {
          message,
          contentProjection: project(message),
          turnIndex,
          isTurnTail: turnTails[i],
          endsReply: replyEnds[i],
          canRegenerate: turnIndex === null || !hiddenTurns.has(turnIndex),
          isLatestReply: message.id === latestReplyId,
          // The fold already decided whether this turn is working, and that
          // decision now includes a background task outliving its stream.
          // Recomputing the older expression here let the bubble call the
          // turn settled while the row above it still read `Working for`,
          // which published sources, deliverables and the rating controls
          // over results the running task can still change.
          isTurnLive: info
            ? info.isLive
            : (turnIndex !== null && running.has(turnIndex)) || !!message.isStreaming,
          turnFiles: turnTails[i] && turnIndex !== null ? filesByTurn.get(turnIndex) : undefined,
          completedAt: turnTails[i] && turnIndex !== null ? turnEnds.get(turnIndex) : undefined,
          quietEnd: turnTails[i] ? info?.quietEnd ?? null : null,
          feedback: turnIndex === null ? null : feedbackByTurn?.[turnIndex] ?? null,
          isLoading,
          fold,
          isSubagentView,
          readOnly,
          allowFiles,
          isMobile,
          dispatcherContext,
          continuesSender: continuing.has(i),
          held: inHeldTurn,
        };
        const plan = plans?.get(message.id as string);
        // Every bubble gets the wrapper from its first paint: a turn's row comes
        // with its first assistant bubble, and switching a bubble from bare to
        // wrapped at the same key would remount it.
        // The row and the answer it heads are one item of the list's rhythm, so
        // the gap between them is the row's own margin, not the list's.
        // A run of one member's messages sits closer together than the
        // list's rhythm, as a group chat stacks them.
        return (
          <div key={message.id as string} style={continuing.has(i) ? RUN_GAP : undefined}>
            {above}
            {info && turnIndex !== null && info.rowPos === i && !inHeldTurn && (
              <TurnFold
                state={fold}
                startedAt={info.startedAt}
                completedAt={info.completedAt}
                sentHere={info.sentHere}
                afterHold={wasHeld.has(turnIndex)}
                turnIndex={turnIndex}
                onToggle={toggleTurn}
              />
            )}
            <MessageBubble {...bubbleProps} />
            {plan && (
              <div className={isMobile ? 'mt-4' : 'mt-6'}>
                <AgentPlan {...plan} heading />
              </div>
            )}
          </div>
        );
      })}
    </CardColumn>
    </CollapseClockContext>
    </ProseTypingContext>
  );
  return <HandoffPlacesContext value={places}>{list}</HandoffPlacesContext>;
}

/**
 * The list as of the latest streamed chunk. A component of its own so that a
 * chunk renders the list and not the view that placed it.
 */
export function LiveMessageList({ store, ...props }: Omit<MessageListProps, 'messages'> & { store: LiveMessages<ChatMessage[]> }) {
  const messages = useLiveMessages(store);
  // The session types its messages; the list reads them as records.
  return <MessageList messages={messages} {...props} />;
}

export default MessageList;
export { MessageContentSegments } from './messageList/MessageContentSegments';
// eslint-disable-next-line react-refresh/only-export-components
export { normalizeSubagentText } from './messageList/normalizeSubagentText';
// eslint-disable-next-line react-refresh/only-export-components
export { isOrphanAssistantMessage } from './messageList/contentProjection';
