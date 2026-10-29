import React, { useState, useEffect, useLayoutEffect, useMemo, useCallback, memo } from 'react';
import ActivityBlock from '../ActivityBlock';
import type { ActivityItem } from './activityTypes';
import { INLINE_ARTIFACT_MAP, openCardTarget } from '../charts/InlineArtifactCards';
import { PILL_ROW_CLASS } from '../charts/pillArtifacts';
import type { OpenFileHandler } from '../../utils/fileLocation';
import UserQuestionCard from '../UserQuestionCard';
import CreateWorkspaceCard from '../CreateWorkspaceCard';
import StartQuestionCard from '../StartQuestionCard';
import SecretaryConfirmCard from '../SecretaryConfirmCard';
import CreditPauseCard from '../CreditPauseCard';
import ToolApprovalCard from '../ToolApprovalCard';
import PastPlanCard from '../PastPlanCard';
import WorkflowRunCard from '../WorkflowRunCard';
import SubagentRun, { type SubagentRunMember } from './SubagentRun';
import AnalystHandoffRun, { type AnalystHandoffMember } from './AnalystHandoffRun';
import { WORKFLOW_TASK_TYPE } from '../../session/subagents/workflowRunState';
import { SubagentStopNotices } from '../SubagentStopNotice';
import type { CreditPauseState, PendingToolCallChunk, PTCAgentProposalState, SubagentTaskRecord, ToolApprovalState } from '@/types/chat';
import TextMessageContent from '../TextMessageContent';
import { visibleParagraphPrefix } from '@/lib/paragraphGate';
import { useTranscriptDisplay } from '@/lib/transcriptDisplay';
import InlineWidget from '../viewers/InlineWidget';
import { NotificationDivider } from './NotificationDivider';
import { normalizeSubagentText } from './normalizeSubagentText';
import StructuredResultBlock from './StructuredResultBlock';
import { parseStructuredResult } from '../../utils/structuredResult';
import { useMessageActions } from './MessageActionsContext';
import { useWorkspaceFolders } from '../../contexts/WorkspaceContext';
import { FoldPanel } from './FoldPanel';
import { CardArrivals, type ArrivingCard } from './CardArrivals';
import { useMotionSafe } from '@/lib/reducedMotion';
import { useProseReport } from './proseTyping';
import { projectContent, blockIsProcess, blockVisible, streamingTextBlockIndex, type ContentProjection } from './contentProjection';
import { EMPTY_OBJ } from './types';
import type { ContentSegmentRecord, FoldState, ToolCallProcessRecord } from './types';
import type {
  CreateWorkspaceRenderBlock,
  CreditPauseRenderBlock,
  HtmlWidgetRenderBlock,
  NotificationRenderBlock,
  PastPlanRenderBlock,
  PTCAgentRenderBlock,
  RenderBlock,
  SecretaryActionRenderBlock,
  StartQuestionRenderBlock,
  SubagentTaskRenderBlock,
  TextRenderBlock,
  ToolApprovalRenderBlock,
  UserQuestionRenderBlock,
} from './buildRenderBlocks';

// --- MessageContentSegments ---

interface MessageContentSegmentsProps {
  segments: ContentSegmentRecord[];
  contentProjection?: ContentProjection;
  reasoningProcesses: Record<string, Record<string, unknown>>;
  toolCallProcesses: Record<string, ToolCallProcessRecord>;
  todoListProcesses: Record<string, Record<string, unknown>>;
  subagentTasks: Record<string, SubagentTaskRecord>;
  userQuestions?: Record<string, Record<string, unknown>>;
  workspaceProposals?: Record<string, Record<string, unknown>>;
  questionProposals?: Record<string, Record<string, unknown>>;
  pendingToolCallChunks?: Record<string, PendingToolCallChunk>;
  isStreaming?: boolean;
  /** How much of the turn this bubble draws; `unfolded` is the full transcript. */
  fold?: FoldState;
  /** The last visible bubble of its backend turn: the only one with an answer. */
  isTurnTail?: boolean;
  isSubagentView?: boolean;
  readOnly?: boolean;
  ptcAgentProposals?: Record<string, PTCAgentProposalState>;
  secretaryActionProposals?: Record<string, Record<string, unknown>>;
  creditPauses?: Record<string, CreditPauseState>;
  toolApprovals?: Record<string, ToolApprovalState>;
  htmlWidgetProcesses?: Record<string, Record<string, unknown>>;
  dispatcherContext?: { threadId: string; workspaceId: string } | null;
  /** The message these are, under which its prose reports its typing. */
  messageId?: string;
}

interface TextBlockProps {
  block: TextRenderBlock;
  isStreaming: boolean;
  isSubagentView: boolean;
  /** The message's last prose block: where a turn-end landing puts the viewport top. */
  isReplyStart: boolean;
  onOpenFile?: OpenFileHandler;
  onRevealed?: (key: string, done: boolean) => void;
  messageId?: string;
}

/** Blocks that wait for the prose above them to finish typing.
 *
 *  Everything here is non-interactive. A HITL card is not, and is deliberately
 *  absent: an approval the reader is waiting on must not be held behind a
 *  paragraph. `notification` belongs here even though a fallback is urgent,
 *  because urgency is already served elsewhere. The `ModelStatus` pill above
 *  the composer announces a retry or a fallback while it is happening; this
 *  block is the durable record of it, and a record printed above the sentence
 *  it follows reads as though the switch happened a sentence earlier than it
 *  did. Nothing is held past the turn either: the hold only applies while the
 *  message streams, so a settle or an error releases it. */
const HOLDS_FOR_PROSE = new Set<RenderBlock['type']>([
  'activity', 'subagent_task', 'html_widget', 'compact_artifact', 'notification',
]);

/** Whitespace between two blocks, such as the "\n\n" a model writes between
 *  two tool calls. It draws nothing, so it is treated as nothing: it neither
 *  renders nor parts the blocks on either side of it. */
function isBlankText(block: RenderBlock): boolean {
  return block.type === 'text' && !((block as TextRenderBlock).segment.content ?? '').trim();
}

function TextBlock({ block, isStreaming, isSubagentView, isReplyStart, onOpenFile, onRevealed, messageId }: TextBlockProps): React.ReactElement | null {
  // Told twice: the message holds what follows this block for it, and the
  // list holds the turn running while it types (see ProseTyping).
  const reportTyping = useProseReport(messageId, block.key);
  const report = useCallback((done: boolean) => {
    onRevealed?.(block.key, done);
    reportTyping(done);
  }, [onRevealed, block.key, reportTyping]);
  const { streamingMode } = useTranscriptDisplay();
  const raw = block.segment.content ?? '';
  // A schema-constrained subagent answers with one JSON object, which the
  // transcript would otherwise show as a raw dump. Mid-stream text is excluded
  // because a partial answer can parse as valid JSON and then change shape.
  const structured = useMemo(
    () =>
      isSubagentView && !isStreaming
        ? parseStructuredResult(raw)
        : null,
    [isSubagentView, isStreaming, raw]
  );
  const textContent = isSubagentView && !structured ? normalizeSubagentText(raw) : raw;
  const textEl = structured ? (
    <StructuredResultBlock result={structured} onOpenFile={onOpenFile} />
  ) : (
    <TextMessageContent
      content={textContent}
      isStreaming={isStreaming}
      onOpenFile={onOpenFile}
      onRevealed={report}
    />
  );
  // A structured result lands whole. Reported when the prose would be.
  useLayoutEffect(() => { if (structured) report(true); }, [structured, report]);
  // What will actually be painted, which is not the same as what has arrived:
  // paragraph mode holds the sentence being written and renders nothing until
  // the first boundary lands. The wrapper keys on this rather than on the raw
  // content, so it is never left standing around an empty child.
  const rendersNow = !!textContent
    && (!isStreaming || streamingMode === 'token' || visibleParagraphPrefix(textContent) !== '');
  // An empty block has no line to land on; the landing falls back to the bubble.
  return isReplyStart && rendersNow ? <div data-reply-start="">{textEl}</div> : textEl;
}

export const MessageContentSegments = memo(function MessageContentSegments({ segments, contentProjection, reasoningProcesses, toolCallProcesses, todoListProcesses: _todoListProcesses, subagentTasks, userQuestions = EMPTY_OBJ, workspaceProposals = EMPTY_OBJ, questionProposals = EMPTY_OBJ, pendingToolCallChunks = EMPTY_OBJ, isStreaming, fold = 'unfolded', isTurnTail = false, isSubagentView = false, readOnly = false, ptcAgentProposals = EMPTY_OBJ, secretaryActionProposals = EMPTY_OBJ, creditPauses = EMPTY_OBJ, toolApprovals = EMPTY_OBJ, htmlWidgetProcesses = EMPTY_OBJ, dispatcherContext, messageId }: MessageContentSegmentsProps): React.ReactElement {
  const { turnDisplay } = useTranscriptDisplay();
  const motionSafe = useMotionSafe();
  const {
    onOpenSubagentTask, onOpenFile, onToolCallDetailClick, onOpenChart,
    onAnswerQuestion, onSkipQuestion,
    onApproveCreateWorkspace, onRejectCreateWorkspace,
    onApproveStartQuestion, onRejectStartQuestion,
    onApprovePTCAgent, onRejectPTCAgent,
    onApproveSecretaryAction, onRejectSecretaryAction,
    onResumeCreditPause,
    onApproveToolCall, onRejectToolCall,
    onWidgetSendPrompt,
  } = useMessageActions();

  // Stable, because `ActivityBlock` is memoized and its other props survive a
  // render that only advanced the typewriter. A fresh closure here would hand
  // it a new identity on every streamed token and re-render every tool row.
  // An activity row's id is its tool call id (see `toolActivity`).
  const onActivityToolClick = useCallback(
    (item: ActivityItem) => onToolCallDetailClick?.(item.id),
    [onToolCallDetailClick],
  );

  // The clock live exposure is measured against. A recently completed row
  // stays live for a minimum exposure and then settles on its own, with no
  // stream event to re-render for it, so a timer advances this clock when the
  // next row is due. It is state rather than a `Date.now()` inside the build:
  // a cached computation only reruns for inputs it can see.
  const [now, setNow] = useState(Date.now);
  const root = useWorkspaceFolders()?.rootDir;
  // `resuming` still counts: the request is in flight and no run has opened,
  // so the tasks really are stopped until admission answers.
  const creditPausePending = useMemo(
    () => Object.values(creditPauses).some((p) => p.status !== 'resumed'),
    [creditPauses],
  );

  const projection = useMemo(() => {
    if (contentProjection && (contentProjection.nextExpiry === null || contentProjection.nextExpiry > now)) return contentProjection;
    return projectContent({ segments, reasoningProcesses, toolCallProcesses, isStreaming, isSubagentView, pendingToolCallChunks, now, root }, contentProjection);
  }, [contentProjection, segments, reasoningProcesses, toolCallProcesses, isStreaming, isSubagentView, pendingToolCallChunks, now, root]);
  const { blocks: renderBlocks, preparingToolCall, nextExpiry } = projection;

  useEffect(() => {
    if (nextExpiry === null) return;
    const timer = setTimeout(() => setNow(Date.now()), Math.max(0, nextExpiry - Date.now()) + 50);
    return () => clearTimeout(timer);
    // `now` too: a tick that left the same row due retries rather than stops.
  }, [nextExpiry, now]);

  // Which prose blocks are fully on screen. The typewriter trails the stream,
  // and a tool call lands the moment the model turns to it, right after the
  // last token of the prose it follows: mounted at once, its row would sit
  // under a paragraph still being typed, and the reader sees the tools land
  // before the words. So an activity block waits for the prose above it to
  // catch up, and prose with an activity block after it is finished, so it
  // types its tail out in the finish window rather than at reading pace.
  const [revealed, setRevealed] = useState<Record<string, boolean>>({});
  const onRevealed = useCallback((key: string, done: boolean) => {
    setRevealed((prev) => (prev[key] === done ? prev : { ...prev, [key]: done }));
  }, []);

  let lastActivityBlockIdx = -1;
  // Task ids in this message, for the stop notices rendered after the prose.
  const taskSubagentIds: string[] = [];
  for (let i = 0; i < renderBlocks.length; i++) {
    const b = renderBlocks[i];
    if (b.type === 'subagent_task') {
      const id = b.segment.subagentId;
      if (id) taskSubagentIds.push(id);
    }
    if (b.type === 'activity') lastActivityBlockIdx = i;
  }
  const streamingTextIdx = isStreaming ? streamingTextBlockIndex(renderBlocks) : -1;

  // Blocks that wait for the prose above them to catch up. They all narrate
  // work the prose introduces, so drawing one over a paragraph the gate is
  // still holding shows the reader the result before the sentence announcing
  // it. The wait carries forward until the next text block, so a card between
  // two of them does not let the row after it through.
  //
  // HITL cards are deliberately not in this set. Their turn is interrupted
  // waiting on the reader, so holding the control they have to answer behind a
  // typewriter would leave the turn waiting on the reader and the reader
  // waiting on the turn.
  const heldForProse: boolean[] = new Array(renderBlocks.length).fill(false);
  if (isStreaming) {
    let waiting = false;
    for (let i = 0; i < renderBlocks.length; i++) {
      const b = renderBlocks[i];
      if (isBlankText(b)) continue;
      // Unreported holds too. A text block reports from a layout effect of
      // the commit that first draws it, and reading the silence before it as
      // "revealed" mounted the card beneath it only to take it away again.
      // The report lands before that commit paints, so the wait costs nothing.
      if (b.type === 'text') waiting = revealed[b.key] !== true;
      else heldForProse[i] = waiting && HOLDS_FOR_PROSE.has(b.type);
    }
  }

  const proposalOf = (block: PTCAgentRenderBlock): PTCAgentProposalState | undefined =>
    block.proposal ?? ptcAgentProposals[block.segment.proposalId!];

  /** Blocks drawn as one row or group with their like neighbours. */
  const runKind = (block: RenderBlock): 'pill' | 'subagent' | 'analyst' | null => {
    if (block.type === 'compact_artifact') return block.card?.kind === 'pill' ? 'pill' : null;
    if (block.type === 'subagent_task') {
      const id = block.segment.subagentId;
      // A workflow run has a card of its own and breaks a run of spawns.
      return id && subagentTasks[id]?.type !== WORKFLOW_TASK_TYPE ? 'subagent' : null;
    }
    if (block.type === 'ptc_agent') return !readOnly && proposalOf(block) ? 'analyst' : null;
    return null;
  };

  const blockIsVisible = renderBlocks.map((block) => blockVisible(projection, block, fold, isTurnTail));
  const blockFolds = renderBlocks.map((block) => fold !== 'unfolded' && blockIsProcess(projection, block, isTurnTail));

  // A lookup's card draws one box with the lookup card of its type above it:
  // a series of quotes or charts reads as one, but a search under a chart
  // would read as part of it. Each stays a block of its own, keyed by itself,
  // so a call landing ahead of a card already drawn moves that card rather
  // than drawing it again, cutting its arrival short. Blank text between the
  // two parts nothing; a block held for the prose, or folding or showing
  // apart from its neighbour, does.
  const lookupType = renderBlocks.map((block) => (block.type === 'compact_artifact' && block.card?.kind === 'call'
    && projection.roles.get(block.key) === 'process' ? block.card.type : null));
  const joinsAbove: boolean[] = new Array(renderBlocks.length).fill(false);
  for (let i = 0, above = -1; i < renderBlocks.length; i++) {
    if (isBlankText(renderBlocks[i])) continue;
    joinsAbove[i] = above >= 0 && lookupType[i] !== null && lookupType[i] === lookupType[above]
      && !heldForProse[i] && !heldForProse[above]
      && blockFolds[i] === blockFolds[above] && blockIsVisible[i] === blockIsVisible[above];
    above = i;
  }

  // The cards a row can hand its place to as its call lands.
  const arrivingCards: ArrivingCard[] = [];
  renderBlocks.forEach((block, i) => {
    if (heldForProse[i] || !blockIsVisible[i] || block.type !== 'compact_artifact' || block.card?.kind !== 'call') return;
    arrivingCards.push({ key: block.key, call: block.toolCallId, settledAt: block.proc._settledAt as number | undefined });
  });

  const renderBlock = (block: RenderBlock, blockIdx: number): React.ReactElement | null => {

        if (block.type === 'activity') {
          return (
            <ActivityBlock
              key={block.key}
              items={block.items}
              preparingToolCall={blockIdx === lastActivityBlockIdx ? preparingToolCall : null}
              isStreaming={isStreaming ?? false}
              /* One status line per turn: the newest activity of a running
                 turn holds it, so it moves to a later block when one opens.
                 The turn's liveness, not the stream's, which drops between
                 model calls and would take the line away in every gap. */
              holdsLine={fold === 'live' && isTurnTail && blockIdx === lastActivityBlockIdx}
              presentation={fold === 'unfolded' ? 'inline' : fold === 'expanded' ? 'expanded' : 'folded'}
              /* Verbose: a thought still streaming shows its text; settled, it
                 folds like any other. The preference reaches no further. */
              liveReasoningOpen={turnDisplay === 'verbose'}
              onToolCallClick={onToolCallDetailClick ? onActivityToolClick : undefined}
              onOpenFile={onOpenFile}
              onOpenChart={onOpenChart}
            />
          );
        }

        if (block.type === 'compact_artifact') {
          const { artifact, card, toolCallId } = block;
          if (!card) return null;
          const Card = INLINE_ARTIFACT_MAP[card.type];
          return (
            <div
              key={block.key}
              data-card-arrival={card.kind === 'call' ? block.key : undefined}
              data-card-join={joinsAbove[blockIdx] ? '' : undefined}
            >
              <Card
                artifact={artifact}
                toolArgs={block.proc.toolCall?.args}
                onClick={() => openCardTarget(artifact, onOpenChart, () => onToolCallDetailClick?.(toolCallId))}
              />
            </div>
          );
        }

        if (block.type === 'notification') {
          const notifSeg = (block as NotificationRenderBlock).segment;
          return (
            <NotificationDivider key={block.key} content={notifSeg.content} fallback={notifSeg.fallback} detail={notifSeg.detail} detailKind={notifSeg.detailKind} />
          );
        }

        if (block.type === 'text') {
          return (
            <TextBlock
              key={block.key}
              block={block as TextRenderBlock}
              isStreaming={blockIdx === streamingTextIdx}
              isReplyStart={block.key === projection.lastTextKey}
              isSubagentView={isSubagentView}
              onOpenFile={onOpenFile}
              onRevealed={onRevealed}
              messageId={messageId}
            />
          );
        }

        if (block.type === 'html_widget') {
          const widgetSeg = (block as HtmlWidgetRenderBlock).segment;
          const widgetData = (htmlWidgetProcesses as Record<string, { html: string; title: string; data?: Record<string, string> }> | undefined)?.[widgetSeg.widgetId!];
          if (!widgetData) return null;
          return (
            // Keep the widget's own margins inside a stable transcript block.
            // Otherwise the list's gap overrides them until the preceding fold hides,
            // then restores them in one frame at the end of the animation.
            <div key={block.key} className="flow-root">
              <InlineWidget
                html={widgetData.html}
                title={widgetData.title}
                onSendPrompt={onWidgetSendPrompt}
                data={widgetData.data}
              />
            </div>
          );
        }

        // A workflow run; a subagent spawn is drawn by its run below.
        if (block.type === 'subagent_task') {
          const subId = (block as SubagentTaskRenderBlock).segment.subagentId!;
          const task = subagentTasks[subId];
          // Absent while the card is still being derived.
          if (!task) return null;
          const launchFailed = task.status === 'error' && !!task.launchFailed;
          return (
            <WorkflowRunCard
              key={block.key}
              subagentId={subId}
              description={task.description}
              status={task.status}
              launchReply={task.result}
              launchFailed={launchFailed}
              // A launch that never ran has nothing to open: opening it would
              // mint a tab for a run that does not exist.
              onOpen={readOnly || launchFailed ? undefined : onOpenSubagentTask}
            />
          );
        }

        if (block.type === 'user_question') {
          const qd = userQuestions[(block as UserQuestionRenderBlock).segment.questionId!];
          if (!qd) return null;
          return (
            <UserQuestionCard
              key={block.key}
              questionData={qd as any} // TODO: type properly, QuestionData not exported
              onAnswer={readOnly ? undefined : (answer: string) => onAnswerQuestion!(answer, (block as UserQuestionRenderBlock).segment.questionId!, qd.interruptId as string)}
              onSkip={readOnly ? undefined : () => onSkipQuestion!((block as UserQuestionRenderBlock).segment.questionId!, qd.interruptId as string)}
            />
          );
        }

        if (block.type === 'create_workspace') {
          if (readOnly) return null;
          const wd = (block as CreateWorkspaceRenderBlock).proposal ?? workspaceProposals[(block as CreateWorkspaceRenderBlock).segment.proposalId!];
          if (!wd) return null;
          return (
            <CreateWorkspaceCard
              key={block.key}
              proposalData={wd as any} // TODO: type properly, ProposalData not exported
              onApprove={onApproveCreateWorkspace ? () => onApproveCreateWorkspace(wd) : undefined}
              onReject={onRejectCreateWorkspace ? () => onRejectCreateWorkspace(wd) : undefined}
            />
          );
        }

        if (block.type === 'start_question') {
          if (readOnly) return null;
          const sqd = questionProposals[(block as StartQuestionRenderBlock).segment.proposalId!];
          if (!sqd) return null;
          return (
            <StartQuestionCard
              key={block.key}
              proposalData={sqd as any} // TODO: type properly, ProposalData not exported
              onApprove={onApproveStartQuestion ? () => onApproveStartQuestion(sqd) : undefined}
              onReject={onRejectStartQuestion ? () => onRejectStartQuestion(sqd) : undefined}
            />
          );
        }

        if (block.type === 'delete_workspace' || block.type === 'stop_workspace' || block.type === 'delete_thread') {
          if (readOnly) return null;
          const sad = secretaryActionProposals[(block as SecretaryActionRenderBlock).segment.proposalId!];
          if (!sad) return null;
          return (
            <SecretaryConfirmCard
              key={block.key}
              proposalData={sad as any}
              onApprove={onApproveSecretaryAction ? () => onApproveSecretaryAction(sad) : undefined}
              onReject={onRejectSecretaryAction ? () => onRejectSecretaryAction(sad) : undefined}
            />
          );
        }

        if (block.type === 'credit_pause') {
          const pauseId = (block as CreditPauseRenderBlock).segment.proposalId!;
          const cpd = creditPauses[pauseId];
          if (!cpd) return null;
          // Renders even read-only in this app (it explains why the turn
          // stopped); only the resume affordance needs an interactive host.
          // Shared transcripts are a different reader and never reach here:
          // SharedChatView projects no interrupt cards, so a pause never
          // lands in a link anyone can open.
          return (
            <CreditPauseCard
              key={block.key}
              pauseData={cpd}
              onResume={!readOnly && onResumeCreditPause ? () => onResumeCreditPause(pauseId, cpd.interruptId) : undefined}
            />
          );
        }

        if (block.type === 'tool_approval') {
          const approvalId = (block as ToolApprovalRenderBlock).segment.proposalId!;
          const ta = toolApprovals[approvalId];
          if (!ta) return null;
          const interruptId = ta.interruptId;
          const position = { index: ta.actionIndex, count: ta.actionCount };
          const attemptId = ta.attemptId;
          // The window between the click and the tool's answer. An order card
          // holds its shape across it rather than settling on a verdict no
          // receipt has confirmed yet, and this is the only place that can
          // tell: the join is the tool call the interrupt named. `isInProgress`
          // rather than the stream's own flag, which is false while a turn sits
          // on its interrupt; a replayed call is reconstructed as complete, so
          // a turn killed before its result settles rather than waiting forever.
          const approvedCall = ta.toolCallId ? toolCallProcesses[ta.toolCallId] : undefined;
          const resultPending = !!approvedCall?.isInProgress && !approvedCall?.toolCallResult;
          // The same join read the other way. A call history rebuilt as
          // complete with no result will never get one, so the card has no
          // receipt to defer to and the ledger is the only record of what the
          // brokerage did. Absent the call entirely, nothing here can tell.
          const resultLost = !!approvedCall && !approvedCall.isInProgress && !approvedCall.toolCallResult;
          return (
            <ToolApprovalCard
              key={block.key}
              data={ta}
              resultPending={resultPending}
              resultLost={resultLost}
              onApprove={!readOnly && onApproveToolCall && interruptId ? () => onApproveToolCall(approvalId, interruptId, position, attemptId) : undefined}
              onReject={!readOnly && onRejectToolCall && interruptId ? (message?: string) => onRejectToolCall(approvalId, interruptId, position, message, attemptId) : undefined}
            />
          );
        }

        if (block.type === 'past_plan') {
          const plan = block as PastPlanRenderBlock;
          return <PastPlanCard key={block.key} description={plan.description} outcome={plan.outcome} />;
        }

        return null;
  };

  // sibling-space-y, not space-y: FoldPanel reads the gap off the top of the
  // block after it, and a folded (hidden) panel must leave none behind.
  return (
    <CardArrivals cards={arrivingCards} live={fold === 'live' || !!isStreaming} motionSafe={motionSafe} className="sibling-space-y-3">
      {(() => {
        // One element per block, except that a run of consecutive blocks of
        // one kind (pills, subagent spawns, analyst hand-offs) sharing a fold
        // and visibility state is gathered into one element.
        const wrap = (blockIdx: number, content: React.ReactElement) => {
          const key = renderBlocks[blockIdx].key;
          if (blockFolds[blockIdx]) {
            return <FoldPanel key={key} open={blockIsVisible[blockIdx]}>{content}</FoldPanel>;
          }
          return blockIsVisible[blockIdx] ? content : null;
        };
        const out: React.ReactNode[] = [];
        for (let blockIdx = 0; blockIdx < renderBlocks.length; blockIdx++) {
          if (heldForProse[blockIdx]) continue;
          const block = renderBlocks[blockIdx];
          if (isBlankText(block)) continue;
          const kind = runKind(block);
          if (kind) {
            const first = blockIdx;
            const run: number[] = [];
            let j = first;
            for (; j < renderBlocks.length; j++) {
              if (isBlankText(renderBlocks[j])) continue;
              if (
                runKind(renderBlocks[j]) !== kind
                || heldForProse[j]
                || blockFolds[j] !== blockFolds[first]
                || blockIsVisible[j] !== blockIsVisible[first]
              ) break;
              run.push(j);
            }
            blockIdx = j - 1;
            // Keyed by the first member, so a block joining the run later grows
            // it without remounting the ones already there.
            const firstKey = renderBlocks[run[0]].key;
            let content: React.ReactElement | null;
            if (kind === 'subagent') {
              const members: SubagentRunMember[] = run.map((idx) => {
                const subId = (renderBlocks[idx] as SubagentTaskRenderBlock).segment.subagentId!;
                return { subagentId: subId, task: subagentTasks[subId], toolCallProcess: toolCallProcesses[subId] || undefined };
              });
              content = (
                <SubagentRun
                  key={firstKey}
                  members={members}
                  pausing={creditPausePending}
                  onOpen={readOnly ? undefined : onOpenSubagentTask}
                />
              );
            } else if (kind === 'analyst') {
              const members: AnalystHandoffMember[] = run.map((idx) => {
                const b = renderBlocks[idx] as PTCAgentRenderBlock;
                return { key: b.key, proposal: proposalOf(b)!, proposalId: b.segment.proposalId! };
              });
              content = (
                <AnalystHandoffRun
                  key={firstKey}
                  members={members}
                  onApprove={onApprovePTCAgent}
                  onReject={onRejectPTCAgent}
                  dispatcherContext={dispatcherContext}
                />
              );
            } else {
              const members = run
                .map((idx) => renderBlock(renderBlocks[idx], idx))
                .filter((c): c is React.ReactElement => c !== null);
              content = members.length === 0 ? null
                : members.length === 1 ? members[0]
                : <div key={`pills-${firstKey}`} data-pill-row className={PILL_ROW_CLASS}>{members}</div>;
            }
            if (content) out.push(wrap(run[0], content));
            continue;
          }
          const content = renderBlock(block, blockIdx);
          // Missing artifact renderers and pending proposals must not leave a
          // spaced fold shell behind when they have nothing to display.
          if (content === null) continue;
          out.push(wrap(blockIdx, content));
        }
        return out;
      })()}
      {/* At the foot of the message, not beside the card that stopped. The
          agent's closing prose was written before the gate fired and still
          promises a result, so a notice above it is read first and contradicted
          second. Last is where the turn's outcome belongs. It renders nothing
          unless a task here was stopped for credits. */}
      <SubagentStopNotices subagentIds={taskSubagentIds} />
    </CardArrivals>
  );
});
