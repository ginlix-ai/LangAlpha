import React, { useState, useEffect, useMemo, useCallback, memo } from 'react';
import ActivityBlock from '../ActivityBlock';
import type { ActivityItem } from './activityTypes';
import { INLINE_ARTIFACT_MAP, openCardTarget } from '../charts/InlineArtifactCards';
import { PILL_ROW_CLASS, isPillArtifactType } from '../charts/pillArtifacts';
import type { OpenFileHandler } from '../../utils/fileLocation';
import UserQuestionCard from '../UserQuestionCard';
import CreateWorkspaceCard from '../CreateWorkspaceCard';
import StartQuestionCard from '../StartQuestionCard';
import PTCAgentCard from '../PTCAgentCard';
import SecretaryConfirmCard from '../SecretaryConfirmCard';
import CreditPauseCard from '../CreditPauseCard';
import ToolApprovalCard from '../ToolApprovalCard';
import PastPlanCard from '../PastPlanCard';
import TaskSegmentCard from './TaskSegmentCard';
import { SubagentStopNotice } from '../SubagentTaskMessageContent';
import type { CreditPauseState, SubagentTaskRecord, ToolApprovalState } from '@/types/chat';
import TextMessageContent from '../TextMessageContent';
import { visibleParagraphPrefix } from '@/lib/paragraphGate';
import { useTranscriptDisplay } from '@/lib/transcriptDisplay';
import InlineWidget from '../viewers/InlineWidget';
import { NotificationDivider } from './NotificationDivider';
import { normalizeSubagentText } from './normalizeSubagentText';
import StructuredResultBlock from './StructuredResultBlock';
import { parseStructuredResult } from '../../utils/structuredResult';
import { useMessageActions } from './MessageActionsContext';
import { FoldPanel } from './FoldPanel';
import { projectContent, blockIsProcess, blockVisible, streamingTextBlockIndex, type ContentProjection } from './contentProjection';
import { EMPTY_OBJ } from './types';
import type { ContentSegmentRecord, FoldState, ToolCallProcessRecord } from './types';
import type {
  CompactArtifactRenderBlock,
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
  pendingToolCallChunks?: Record<string, Record<string, unknown>>;
  isStreaming?: boolean;
  hasError?: boolean;
  /** Classified error data from the backend, used by TextMessageContent so
   *  inline error cards can render hints without re-parsing the raw text. */
  structuredError?: import('@/utils/rateLimitError').StructuredError;
  /** How much of the turn this bubble draws; `unfolded` is the full transcript. */
  fold?: FoldState;
  /** The last visible bubble of its backend turn: the only one with an answer. */
  isTurnTail?: boolean;
  isSubagentView?: boolean;
  readOnly?: boolean;
  ptcAgentProposals?: Record<string, Record<string, unknown>>;
  secretaryActionProposals?: Record<string, Record<string, unknown>>;
  creditPauses?: Record<string, CreditPauseState>;
  toolApprovals?: Record<string, ToolApprovalState>;
  htmlWidgetProcesses?: Record<string, Record<string, unknown>>;
  flashContext?: { threadId: string; workspaceId: string } | null;
}

interface TextBlockProps {
  block: TextRenderBlock;
  isStreaming: boolean;
  hasError: boolean;
  structuredError?: import('@/utils/rateLimitError').StructuredError;
  isSubagentView: boolean;
  /** The message's last prose block: where a turn-end landing puts the viewport top. */
  isReplyStart: boolean;
  onOpenFile?: OpenFileHandler;
  onRevealed?: (key: string, done: boolean) => void;
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

/** A compact artifact whose card is a pill, which sits beside its neighbours. */
function isPillBlock(block: RenderBlock): boolean {
  if (block.type !== 'compact_artifact') return false;
  const result = (block as CompactArtifactRenderBlock).proc.toolCallResult as Record<string, unknown> | undefined;
  return isPillArtifactType((result?.artifact as Record<string, unknown> | undefined)?.type);
}

function TextBlock({ block, isStreaming, hasError, structuredError, isSubagentView, isReplyStart, onOpenFile, onRevealed }: TextBlockProps): React.ReactElement | null {
  const report = useCallback((done: boolean) => onRevealed?.(block.key, done), [onRevealed, block.key]);
  const { streamingMode } = useTranscriptDisplay();
  const raw = block.segment.content ?? '';
  // A schema-constrained subagent answers with one JSON object, which the
  // transcript would otherwise show as a raw dump. Mid-stream text is excluded
  // because a partial answer can parse as valid JSON and then change shape, and
  // an error payload already has its own display.
  const structured = useMemo(
    () =>
      isSubagentView && !isStreaming && !hasError
        ? parseStructuredResult(raw)
        : null,
    [isSubagentView, isStreaming, hasError, raw]
  );
  const textContent = isSubagentView && !structured ? normalizeSubagentText(raw) : raw;
  const textEl = structured ? (
    <StructuredResultBlock result={structured} onOpenFile={onOpenFile} />
  ) : (
    <TextMessageContent
      content={textContent}
      isStreaming={isStreaming}
      hasError={hasError}
      structuredError={structuredError}
      onOpenFile={onOpenFile}
      onRevealed={report}
    />
  );
  // A structured result lands whole.
  useEffect(() => { if (structured) report(true); }, [structured, report]);
  // What will actually be painted, which is not the same as what has arrived:
  // paragraph mode holds the sentence being written and renders nothing until
  // the first boundary lands. The wrapper keys on this rather than on the raw
  // content, so it is never left standing around an empty child.
  const rendersNow = !!textContent
    && (!isStreaming || hasError || streamingMode === 'token' || visibleParagraphPrefix(textContent) !== '');
  // An empty block has no line to land on; the landing falls back to the bubble.
  return isReplyStart && rendersNow ? <div data-reply-start="">{textEl}</div> : textEl;
}

export const MessageContentSegments = memo(function MessageContentSegments({ segments, contentProjection, reasoningProcesses, toolCallProcesses, todoListProcesses: _todoListProcesses, subagentTasks, userQuestions = EMPTY_OBJ, workspaceProposals = EMPTY_OBJ, questionProposals = EMPTY_OBJ, pendingToolCallChunks = EMPTY_OBJ, isStreaming, hasError, structuredError, fold = 'unfolded', isTurnTail = false, isSubagentView = false, readOnly = false, ptcAgentProposals = EMPTY_OBJ, secretaryActionProposals = EMPTY_OBJ, creditPauses = EMPTY_OBJ, toolApprovals = EMPTY_OBJ, htmlWidgetProcesses = EMPTY_OBJ, flashContext }: MessageContentSegmentsProps): React.ReactElement {
  const { turnDisplay } = useTranscriptDisplay();
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

  const projection = useMemo(() => {
    if (contentProjection && (contentProjection.nextExpiry === null || contentProjection.nextExpiry > now)) return contentProjection;
    return projectContent({ segments, reasoningProcesses, toolCallProcesses, isStreaming, isSubagentView, pendingToolCallChunks, now }, contentProjection);
  }, [contentProjection, segments, reasoningProcesses, toolCallProcesses, isStreaming, isSubagentView, pendingToolCallChunks, now]);
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
  // catch up (only `false` holds: a block that has not reported is not
  // typing), and prose with an activity block after it is finished, so it
  // types its tail out in the finish window rather than at reading pace.
  const [revealed, setRevealed] = useState<Record<string, boolean>>({});
  const onRevealed = useCallback((key: string, done: boolean) => {
    setRevealed((prev) => (prev[key] === done ? prev : { ...prev, [key]: done }));
  }, []);

  let lastTextBlockIdx = -1;
  let lastActivityBlockIdx = -1;
  // Task ids in this message, for the stop notices rendered after the prose.
  const taskSubagentIds: string[] = [];
  for (let i = 0; i < renderBlocks.length; i++) {
    const b = renderBlocks[i];
    if (b.type === 'text') lastTextBlockIdx = i;
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
      // Unreported holds too. A text block reports from an effect, so on the
      // commit that first paints it there is no answer yet, and reading that
      // silence as "revealed" painted the card beneath it for one frame and
      // then took it away. Every streaming text block reports on its first
      // effect, so the wait is released a frame later at worst.
      if (b.type === 'text') waiting = revealed[b.key] !== true;
      else heldForProse[i] = waiting && HOLDS_FOR_PROSE.has(b.type);
    }
  }

  const blockIsVisible = renderBlocks.map((block) => blockVisible(projection, block, fold, isTurnTail));
  const blockFolds = renderBlocks.map((block) => fold !== 'unfolded' && blockIsProcess(projection, block, isTurnTail));

  const renderBlock = (block: RenderBlock, blockIdx: number): React.ReactElement | null => {

        if (block.type === 'activity') {
          return (
            <ActivityBlock
              key={block.key}
              items={block.items}
              preparingToolCall={blockIdx === lastActivityBlockIdx ? preparingToolCall : null}
              isStreaming={isStreaming ?? false}
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
          const artifact = ((block as CompactArtifactRenderBlock).proc.toolCallResult as Record<string, unknown> | undefined)?.artifact as Record<string, unknown> | undefined;
          const ChartComponent = artifact ? INLINE_ARTIFACT_MAP[artifact.type as string] : null;
          if (!ChartComponent) return null;
          return (
            <div key={block.key}>
              <ChartComponent
                artifact={artifact!}
                toolArgs={(block as CompactArtifactRenderBlock).proc.toolCall?.args}
                onClick={() => openCardTarget(artifact, onOpenChart, () => onToolCallDetailClick?.((block as CompactArtifactRenderBlock).toolCallId))}
              />
            </div>
          );
        }

        if (block.type === 'notification') {
          const notifSeg = (block as NotificationRenderBlock).segment;
          return (
            <NotificationDivider key={block.key} content={notifSeg.content} detail={notifSeg.detail} detailKind={notifSeg.detailKind} />
          );
        }

        if (block.type === 'text') {
          return (
            <TextBlock
              key={block.key}
              block={block as TextRenderBlock}
              isStreaming={blockIdx === streamingTextIdx}
              isReplyStart={blockIdx === lastTextBlockIdx}
              hasError={!!hasError}
              structuredError={structuredError}
              isSubagentView={isSubagentView}
              onOpenFile={onOpenFile}
              onRevealed={onRevealed}
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

        if (block.type === 'subagent_task') {
          const subId = (block as SubagentTaskRenderBlock).segment.subagentId!;
          return (
            <TaskSegmentCard
              key={block.key}
              subagentId={subId}
              task={subagentTasks[subId]}
              toolCallProcess={toolCallProcesses[subId] || undefined}
              onOpen={readOnly ? undefined : onOpenSubagentTask}
              onDetailOpen={readOnly ? undefined : onToolCallDetailClick}
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

        if (block.type === 'ptc_agent') {
          if (readOnly) return null;
          const pad = (block as PTCAgentRenderBlock).proposal ?? ptcAgentProposals[(block as PTCAgentRenderBlock).segment.proposalId!];
          if (!pad) return null;
          return (
            <PTCAgentCard
              key={block.key}
              proposalData={pad as any}
              onApprove={onApprovePTCAgent ? (overrides?: { report_back?: boolean }) => onApprovePTCAgent(pad, overrides, (block as PTCAgentRenderBlock).segment.proposalId!, pad.interruptId as string) : undefined}
              onReject={onRejectPTCAgent ? () => onRejectPTCAgent(pad, (block as PTCAgentRenderBlock).segment.proposalId!, pad.interruptId as string) : undefined}
              flashContext={flashContext}
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
    <div className="sibling-space-y-3">
      {(() => {
        // One element per block, except that a run of consecutive pill blocks
        // sharing a fold and visibility state is gathered into one wrapping row.
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
          if (isPillBlock(block)) {
            const run: number[] = [];
            let j = blockIdx;
            while (
              j < renderBlocks.length
              && isPillBlock(renderBlocks[j])
              && !heldForProse[j]
              && blockFolds[j] === blockFolds[blockIdx]
              && blockIsVisible[j] === blockIsVisible[blockIdx]
            ) {
              run.push(j);
              j++;
            }
            const members = run
              .map((idx) => renderBlock(renderBlocks[idx], idx))
              .filter((c): c is React.ReactElement => c !== null);
            blockIdx = j - 1;
            if (members.length === 0) continue;
            // Keyed by the first member, so a pill joining the run later grows
            // the row without remounting the ones already there.
            const content = members.length === 1
              ? members[0]
              : <div key={`pills-${renderBlocks[run[0]].key}`} data-pill-row className={PILL_ROW_CLASS}>{members}</div>;
            out.push(wrap(run[0], content));
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
          second. Last is where the turn's outcome belongs. Each self-hides
          unless its task was stopped for credits, so ordinary turns render
          nothing here. */}
      {taskSubagentIds.map((id) => (
        <SubagentStopNotice key={`stop-${id}`} subagentId={id} />
      ))}
    </div>
  );
});
