import React from 'react';
import { motion } from '@/lib/framer';
import { useIsMobile } from '@/hooks/useIsMobile';
import { NotificationDivider } from './messageList/NotificationDivider';
import { MessageBubble } from './messageList/MessageBubble';
import { TurnFold } from './messageList/TurnFold';
import { projectMessageContent, visibleProjection } from './messageList/contentProjection';
import { useMessageActions } from './messageList/MessageActionsContext';
import { isSteeringUserMessage } from './messageList/messagePredicates';
import { computeTurnTails, projectTurns } from './messageList/turnProjection';
import { useLiveMessages, type LiveMessages } from '../session/stream/liveMessages';
import type { ChatMessage } from '@/types/chat';
import { turnFilesByTurn } from '../utils/turnFiles';
import { useWorkspaceFolders } from '../contexts/WorkspaceContext';
import type { FeedbackResult, FoldState, MessageRecord } from './messageList/types';

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
}

/**
 * A bubble's time arrives as a Date from the chat session and as an ISO string
 * from the market-chat reducer. Reading only the Date dropped the clock for the
 * market transcript, and a turn with no start gets no fold at all, so the whole
 * panel silently kept the pre-fold layout.
 */
function epochMs(value: unknown): number | undefined {
  const ms = value instanceof Date ? value.getTime()
    : typeof value === 'number' ? value
      : typeof value === 'string' ? Date.parse(value)
        : NaN;
  return Number.isFinite(ms) ? ms : undefined;
}

/** The activity accordion's fold spring, so the two levels move alike. */
const FOLD_ROW_ENTER = { type: 'spring' as const, stiffness: 260, damping: 30 };

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
  flashContext?: { threadId: string; workspaceId: string } | null;
}

function MessageList({ messages, isLoading, isLoadingHistory, isSubagentView, readOnly, allowFiles, feedbackByTurn, flashContext }: MessageListProps): React.ReactElement | null {
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
  const visible = React.useMemo(() => visibleProjection(projected, isSubagentView), [projected, isSubagentView]);
  const turnTails = React.useMemo(() => computeTurnTails(visible), [visible]);

  // The deliverables strip reads the RAW projection: a turn's files are named
  // across its whole span, including a bubble the list never paints.
  const filesByTurn = React.useMemo(
    () => turnFilesByTurn(projected, folders),
    [projected, folders],
  );

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
    for (const { message, turnIndex } of projected) {
      if ((message.role as string) === 'assistant' && turnIndex > newest) newest = turnIndex;
    }
    return newest;
  }, [projected]);

  // Both display preferences fold finished turns; only live reasoning differs.
  const turnFolds = React.useMemo(() => {
    const folds = new Map<number, TurnFoldInfo>();

    const startedAt = new Map<number, number>();
    const firstReplyAt = new Map<number, number>();
    const rowPos = new Map<number, number>();
    const assistantBubbles = new Map<number, MessageRecord[]>();

    // The turn's end is stamped on its tail bubble, and that bubble is not
    // always painted: a steering continuation that settled empty is dropped
    // from `visible` by `isOrphanAssistantMessage` while still carrying the
    // stamp stream finalization and both replay paths wrote to it. Read from
    // the raw list, so a turn that ended on one is still timed. Last write
    // wins, which is the latest stamped bubble of the turn.
    const endStamps = new Map<number, MessageRecord>();
    for (const { message, turnIndex } of projected) {
      if ((message.role as string) !== 'assistant') continue;
      if (message.completedAt !== undefined || message.completionObservedAt !== undefined) {
        endStamps.set(turnIndex, message);
      }
    }

    for (let i = 0; i < visible.length; i++) {
      const { message, turnIndex } = visible[i];
      const role = message.role as string;
      if (role === 'user') {
        // A steering bubble is projected onto the NEXT turn (it lands after the
        // opener already advanced the counter), and on replay its timestamp is
        // not history time. Only a turn's own initiator can start its clock.
        if (isSteeringUserMessage(message) || startedAt.has(turnIndex)) continue;
        const ts = epochMs(message.timestamp);
        if (ts !== undefined) startedAt.set(turnIndex, ts);
        continue;
      }
      if (role !== 'assistant') continue;
      if (!rowPos.has(turnIndex)) rowPos.set(turnIndex, i);
      if (!firstReplyAt.has(turnIndex)) {
        const ts = epochMs(message.timestamp);
        if (ts !== undefined) firstReplyAt.set(turnIndex, ts);
      }
      const bubbles = assistantBubbles.get(turnIndex);
      if (bubbles) bubbles.push(message);
      else assistantBubbles.set(turnIndex, [message]);
    }

    // A turn nobody typed to start, a subagent's report-back, has no user
    // bubble to take its clock from; its first reply is the nearest thing.
    for (const [turnIndex, ts] of firstReplyAt) {
      if (!startedAt.has(turnIndex)) startedAt.set(turnIndex, ts);
    }

    for (let i = 0; i < visible.length; i++) {
      // The turn's end is stamped on its tail bubble, which is also the only
      // bubble that can be streaming once the turn has several.
      if (!turnTails[i]) continue;
      const { message, turnIndex } = visible[i];
      const start = startedAt.get(turnIndex);
      const pos = rowPos.get(turnIndex);
      if (start === undefined || pos === undefined) continue;
      const contents = (assistantBubbles.get(turnIndex) ?? []).map((bubble) => projectMessageContent(bubble, isSubagentView));
      if (!contents.some((content) => content.hasProcess) && contents.reduce((sum, content) => sum + content.textCount, 0) < 2) continue;
      // A background task outlives the stream that launched it, and the row
      // it runs under is a process row: letting the turn settle here would
      // collapse the only indicator that the work is still going.
      const isLive = (!!isLoading && turnIndex === newestTurn) || !!message.isStreaming
        || contents.some((content) => content.hasPinnedLive);
      // A pinned call outlives the stream that launched it, so the stream-close
      // stamp was taken before the turn's work was done and a fold reading it
      // counts backwards, from `Working for 2m` to `Worked for 5s`. The server's
      // own stamp still wins outright; this only sharpens the local fallback.
      const endStamp = endStamps.get(turnIndex) ?? message;
      const observedEnd = epochMs(endStamp.completionObservedAt);
      const localEnd = contents.reduce<number | undefined>(
        (end, content) => (content.pinnedSettledAt !== null && (end === undefined || content.pinnedSettledAt > end)
          ? content.pinnedSettledAt
          : end),
        observedEnd,
      );
      const hasFiles = !!onOpenFile && (!readOnly || allowFiles) && (filesByTurn.get(turnIndex)?.length ?? 0) > 0;
      folds.set(turnIndex, {
        noAnswer: !(contents.at(-1)?.lastTextKey || contents.some((content) => content.hasRetained) || hasFiles),
        rowPos: pos,
        startedAt: start,
        completedAt: isLive ? undefined : epochMs(endStamp.completedAt) ?? localEnd,
        isLive,
      });
    }
    return folds;
  }, [visible, projected, turnTails, isLoading, newestTurn, isSubagentView, filesByTurn, onOpenFile, readOnly, allowFiles]);

  // Empty state - show when no messages exist (hidden in subagent view)
  if (messages.length === 0) {
    if (isSubagentView) return null;
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
  // PTCAgentCard in it shares one batched dispatch-liveness query and timer.
  // Here it re-rendered, and re-ran its query hook, on every streamed frame.
  // sibling-space-y, not space-y: NotificationDivider's own my-1 would
  // otherwise replace the gap below it.
  return (
    <div className={`font-content ${isMobile ? 'sibling-space-y-4' : 'sibling-space-y-6'}`}>
      {visible.map(({ message, turnIndex }, i) => {
        if ((message.role as string) === 'notification') {
          return <NotificationDivider key={message.id as string} message={message} />;
        }
        // A bubble only ever folds when its turn shows the row that unfolds it:
        // a turn with no fold row (no initiating message, nothing foldable yet)
        // keeps the verbose shape rather than hiding prose behind nothing.
        const info = turnFolds.get(turnIndex);
        const fold: FoldState = !info
          ? 'unfolded'
          : info.isLive
            ? 'live'
            : expandedTurns.has(turnIndex) ? 'expanded' : info.noAnswer ? 'process' : 'collapsed';
        const bubbleProps = {
          message,
          contentProjection: projectMessageContent(message, isSubagentView),
          turnIndex,
          isTurnTail: turnTails[i],
          // The fold already decided whether this turn is working, and that
          // decision now includes a background task outliving its stream.
          // Recomputing the older expression here let the bubble call the
          // turn settled while the row above it still read `Working for`,
          // which published sources, deliverables and the rating controls
          // over results the running task can still change.
          isTurnLive: info
            ? info.isLive
            : (!!isLoading && turnIndex === newestTurn) || !!message.isStreaming,
          turnFiles: turnTails[i] ? filesByTurn.get(turnIndex) : undefined,
          feedback: feedbackByTurn?.[turnIndex] ?? null,
          isLoading,
          fold,
          isSubagentView,
          readOnly,
          allowFiles,
          isMobile,
          flashContext,
        };
        // Every bubble gets the wrapper from its first paint: the fold row
        // only exists once the turn has something foldable, and switching a
        // streaming bubble from bare to wrapped at the same key would remount it.
        // The row and the answer it heads are one item of the list's rhythm, so
        // the gap between them is the row's own margin, not the list's.
        return (
          <div key={message.id as string}>
            {info && info.rowPos === i && (
              /* A row that arrives over a turn already streaming unfolds
                 rather than pushing the reply down a line in one paint. A
                 settled turn's row was there before the reader was. */
              <motion.div
                initial={info.isLive ? { height: 0, opacity: 0 } : false}
                animate={{ height: 'auto', opacity: 1 }}
                transition={FOLD_ROW_ENTER}
                style={{ overflow: 'hidden' }}
              >
                <TurnFold
                  state={fold === 'live' ? 'live' : fold === 'collapsed' ? 'collapsed' : fold === 'process' ? 'process' : 'expanded'}
                  startedAt={info.startedAt}
                  completedAt={info.completedAt}
                  onToggle={() => toggleTurn(turnIndex)}
                />
              </motion.div>
            )}
            <MessageBubble {...bubbleProps} />
          </div>
        );
      })}
    </div>
  );
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
