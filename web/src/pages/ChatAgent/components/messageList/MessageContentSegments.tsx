import React, { useState, useEffect, useRef, useMemo, memo } from 'react';
import ActivityBlock from '../ActivityBlock';
import { INLINE_ARTIFACT_MAP } from '../charts/InlineArtifactCards';
import { extractFilePaths, FileMentionCards } from '../FileCard';
import { normalizeFileRefs } from '../../utils/normalizeFileRefs';
import ReasoningMessageContent from '../ReasoningMessageContent';
import PlanApprovalCard from '../PlanApprovalCard';
import UserQuestionCard from '../UserQuestionCard';
import CreateWorkspaceCard from '../CreateWorkspaceCard';
import StartQuestionCard from '../StartQuestionCard';
import PTCAgentCard from '../PTCAgentCard';
import SecretaryConfirmCard from '../SecretaryConfirmCard';
import CreditPauseCard from '../CreditPauseCard';
import TaskSegmentCard from './TaskSegmentCard';
import { SubagentStopNotice } from '../SubagentTaskMessageContent';
import type { CreditPauseState, SubagentTaskRecord } from '@/types/chat';
import TextMessageContent from '../TextMessageContent';
import InlineWidget from '../viewers/InlineWidget';
import ToolCallMessageContent from '../ToolCallMessageContent';
import { NotificationDivider } from './NotificationDivider';
import { normalizeSubagentText } from './normalizeSubagentText';
import StructuredResultBlock from './StructuredResultBlock';
import { parseStructuredResult } from '../../utils/structuredResult';
import { useMessageActions } from './MessageActionsContext';
import { EMPTY_OBJ } from './types';
import type { ContentSegmentRecord, ToolCallProcessRecord } from './types';
import {
  buildRenderBlocks,
  groupSegments,
  type ActivityRenderBlock,
  type CompactArtifactRenderBlock,
  type CreateWorkspaceRenderBlock,
  type CreditPauseRenderBlock,
  type HtmlWidgetRenderBlock,
  type NotificationRenderBlock,
  type PlanApprovalRenderBlock,
  type PTCAgentRenderBlock,
  type SecretaryActionRenderBlock,
  type StartQuestionRenderBlock,
  type SubagentTaskRenderBlock,
  type TextRenderBlock,
  type UserQuestionRenderBlock,
} from './buildRenderBlocks';

// --- MessageContentSegments ---

interface MessageContentSegmentsProps {
  segments: ContentSegmentRecord[];
  reasoningProcesses: Record<string, Record<string, unknown>>;
  toolCallProcesses: Record<string, ToolCallProcessRecord>;
  todoListProcesses: Record<string, Record<string, unknown>>;
  subagentTasks: Record<string, SubagentTaskRecord>;
  planApprovals?: Record<string, Record<string, unknown>>;
  userQuestions?: Record<string, Record<string, unknown>>;
  workspaceProposals?: Record<string, Record<string, unknown>>;
  questionProposals?: Record<string, Record<string, unknown>>;
  pendingToolCallChunks?: Record<string, Record<string, unknown>>;
  isStreaming?: boolean;
  hasError?: boolean;
  /** Classified error data from the backend — used by TextMessageContent so
   *  inline error cards can render hints without re-parsing the raw text. */
  structuredError?: import('@/utils/rateLimitError').StructuredError;
  isAssistant?: boolean;
  compactToolCalls?: boolean;
  isSubagentView?: boolean;
  readOnly?: boolean;
  allowFiles?: boolean;
  ptcAgentProposals?: Record<string, Record<string, unknown>>;
  secretaryActionProposals?: Record<string, Record<string, unknown>>;
  creditPauses?: Record<string, CreditPauseState>;
  htmlWidgetProcesses?: Record<string, Record<string, unknown>>;
  flashContext?: { threadId: string; workspaceId: string } | null;
}

interface TextBlockProps {
  block: TextRenderBlock;
  isFirst: boolean;
  isStreaming: boolean;
  hasError: boolean;
  structuredError?: import('@/utils/rateLimitError').StructuredError;
  isSubagentView: boolean;
  onOpenFile?: (path: string, workspaceId?: string) => void;
}

function TextBlock({ block, isFirst, isStreaming, hasError, structuredError, isSubagentView, onOpenFile }: TextBlockProps): React.ReactElement | null {
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
    />
  );
  // First-block pure-text gets a −4px offset so its first-line center matches the
  // 32px logo center. Reasoning-leading blocks handle their own offset inside
  // ActivityBlock. Guard on textContent so an empty streaming block doesn't render
  // an empty wrapper that shifts later siblings.
  return isFirst && textContent ? <div className="-mt-1">{textEl}</div> : textEl;
}

export const MessageContentSegments = memo(function MessageContentSegments({ segments, reasoningProcesses, toolCallProcesses, todoListProcesses: _todoListProcesses, subagentTasks, planApprovals = EMPTY_OBJ, userQuestions = EMPTY_OBJ, workspaceProposals = EMPTY_OBJ, questionProposals = EMPTY_OBJ, pendingToolCallChunks = EMPTY_OBJ, isStreaming, hasError, structuredError, isAssistant = false, compactToolCalls = false, isSubagentView = false, readOnly = false, allowFiles = false, ptcAgentProposals = EMPTY_OBJ, secretaryActionProposals = EMPTY_OBJ, creditPauses = EMPTY_OBJ, htmlWidgetProcesses = EMPTY_OBJ, flashContext }: MessageContentSegmentsProps): React.ReactElement {
  const {
    onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick,
    onApprovePlan, onRejectPlan, onPlanDetailClick,
    onAnswerQuestion, onSkipQuestion,
    onApproveCreateWorkspace, onRejectCreateWorkspace,
    onApproveStartQuestion, onRejectStartQuestion,
    onApprovePTCAgent, onRejectPTCAgent,
    onApproveSecretaryAction, onRejectSecretaryAction,
    onResumeCreditPause,
    onWidgetSendPrompt,
  } = useMessageActions();

  // Force re-render timer for recently-completed tool calls that need minimum exposure
  const [tick, setTick] = useState(0);
  const expiryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const nextExpiryRef = useRef<number | null>(null);

  // Schedule timer for next expiry — runs after every render since nextExpiryRef
  // is set during render, from the memoized renderBlocks below.
  useEffect(() => {
    if (expiryTimerRef.current) clearTimeout(expiryTimerRef.current);
    expiryTimerRef.current = null;

    if (nextExpiryRef.current !== null) {
      const delay = Math.max(0, nextExpiryRef.current - Date.now()) + 50;
      expiryTimerRef.current = setTimeout(() => {
        setTick((n) => n + 1);
      }, delay);
    }

    return () => { if (expiryTimerRef.current) clearTimeout(expiryTimerRef.current); };
  });

  // Memoize sorted + grouped segments
  const groupedSegments = useMemo(() => groupSegments(segments), [segments]);

  // Reset expiry for this render pass (set by memoized renderBlocks below)
  nextExpiryRef.current = null;

  // Memoize the expensive renderBlocks construction.
  // tick is included so timer-driven live→completed transitions recompute correctly.
  const { blocks: renderBlocks, nextExpiry } = useMemo(
    () => buildRenderBlocks(groupedSegments, { reasoningProcesses, toolCallProcesses, isStreaming, isSubagentView }),
  // eslint-disable-next-line react-hooks/exhaustive-deps -- tick is a semantic dep: forces recomputation when timer fires for live→completed transitions
    [groupedSegments, tick, reasoningProcesses, toolCallProcesses, isStreaming, isSubagentView],
  );

  // Apply side effect: schedule timer for next expiry transition
  nextExpiryRef.current = nextExpiry;

  // Derived values
  const chunkEntries = Object.values(pendingToolCallChunks);
  const preparingToolCall = chunkEntries.length > 0 ? {
    toolName: (chunkEntries.find((c) => (c as Record<string, unknown>).toolName)?.toolName as string | undefined) ?? undefined,
    chunkCount: chunkEntries.reduce((sum, c) => sum + ((c as Record<string, unknown>).chunkCount as number || 0), 0),
    argsLength: chunkEntries.reduce((sum, c) => sum + ((c as Record<string, unknown>).argsLength as number || 0), 0),
  } : null;

  let lastTextBlockIdx = -1;
  let lastActivityBlockIdx = -1;
  let hasAnyTrulyInProgress = false;
  // Task ids in this message, for the stop notices rendered after the prose.
  const taskSubagentIds: string[] = [];
  for (let i = 0; i < renderBlocks.length; i++) {
    const b = renderBlocks[i];
    if (b.type === 'text') lastTextBlockIdx = i;
    if (b.type === 'subagent_task') {
      const id = (b as SubagentTaskRenderBlock).segment.subagentId;
      if (id) taskSubagentIds.push(id);
    }
    if (b.type === 'activity') {
      lastActivityBlockIdx = i;
      if ((b as ActivityRenderBlock).items.some(item => item._liveState === 'active' && item.type === 'tool_call')) {
        hasAnyTrulyInProgress = true;
      }
    }
  }

  const detectedFiles = isAssistant && !isStreaming
    ? extractFilePaths(normalizeFileRefs(renderBlocks.filter(b => b.type === 'text').map(b => (b as TextRenderBlock).segment.content || '').join('\n')))
    : [];

  return (
    <div className="space-y-1">
      {renderBlocks.map((block, blockIdx) => {
        if (block.type === 'activity') {
          if (compactToolCalls) {
            // Show all items in compact mode (not just completed)
            const items = (block as ActivityRenderBlock).items;
            return (
              <div key={block.key}>
                {items.map((item) => {
                  if (item.type === 'tool_call') {
                    return (
                      <ToolCallMessageContent
                        key={`tool-call-${item.toolCallId}`}
                        toolCallId={item.toolCallId as string}
                        toolName={item.toolName as string}
                        toolCall={item.toolCall as any} // TODO: type properly
                        toolCallResult={item.toolCallResult as any} // TODO: type properly
                        isInProgress={(item.isInProgress as boolean) || false}
                        isComplete={(item.isComplete as boolean) || false}
                        isFailed={(item.isFailed as boolean) || false}
                        onOpenFile={onOpenFile}
                      />
                    );
                  }
                  if (item.type === 'reasoning') {
                    return (
                      <ReasoningMessageContent
                        key={`reasoning-${item.id}`}
                        reasoningContent={(item.content as string) || ''}
                        isReasoning={item._liveState === 'active'}
                        reasoningComplete={(item.reasoningComplete as boolean) || item._liveState === 'completed'}
                        reasoningTitle={(item.reasoningTitle as string) ?? undefined}
                      />
                    );
                  }
                  return null;
                })}
              </div>
            );
          }

          return (
            <ActivityBlock
              key={block.key}
              items={(block as ActivityRenderBlock).items as any} // TODO: type properly — ActivityItem[] not exported
              preparingToolCall={blockIdx === lastActivityBlockIdx ? preparingToolCall : null}
              isStreaming={isStreaming ?? false}
              onToolCallClick={onToolCallDetailClick as any} // TODO: type properly
              onOpenFile={onOpenFile}
            />
          );
        }

        if (block.type === 'compact_artifact') {
          const artifact = ((block as CompactArtifactRenderBlock).proc.toolCallResult as Record<string, unknown> | undefined)?.artifact as Record<string, unknown> | undefined;
          const ChartComponent = artifact ? INLINE_ARTIFACT_MAP[artifact.type as string] : null;
          if (!ChartComponent) return null;
          return (
            <div key={block.key} className="mt-1 mb-1">
              <ChartComponent
                artifact={artifact!}
                onClick={() => onToolCallDetailClick?.((block as CompactArtifactRenderBlock).proc)}
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
              isFirst={blockIdx === 0}
              isStreaming={!!(isStreaming && blockIdx === lastTextBlockIdx && !hasAnyTrulyInProgress)}
              hasError={!!hasError}
              structuredError={structuredError}
              isSubagentView={isSubagentView}
              onOpenFile={onOpenFile}
            />
          );
        }

        if (block.type === 'html_widget') {
          const widgetSeg = (block as HtmlWidgetRenderBlock).segment;
          const widgetData = (htmlWidgetProcesses as Record<string, { html: string; title: string; data?: Record<string, string> }> | undefined)?.[widgetSeg.widgetId!];
          if (!widgetData) return null;
          return (
            <InlineWidget
              key={block.key}
              html={widgetData.html}
              title={widgetData.title}
              onSendPrompt={onWidgetSendPrompt}
              data={widgetData.data}
            />
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

        if (block.type === 'plan_approval') {
          const pd = planApprovals[(block as PlanApprovalRenderBlock).segment.planApprovalId!];
          if (!pd) return null;
          return (
            <PlanApprovalCard
              key={block.key}
              planData={pd as any} // TODO: type properly — PlanData not exported
              onApprove={readOnly ? undefined : onApprovePlan}
              onReject={readOnly ? undefined : onRejectPlan}
              onDetailClick={readOnly ? undefined : () => onPlanDetailClick?.(pd)}
            />
          );
        }

        if (block.type === 'user_question') {
          const qd = userQuestions[(block as UserQuestionRenderBlock).segment.questionId!];
          if (!qd) return null;
          return (
            <UserQuestionCard
              key={block.key}
              questionData={qd as any} // TODO: type properly — QuestionData not exported
              onAnswer={readOnly ? undefined : (answer: string) => onAnswerQuestion!(answer, (block as UserQuestionRenderBlock).segment.questionId!, qd.interruptId as string)}
              onSkip={readOnly ? undefined : () => onSkipQuestion!((block as UserQuestionRenderBlock).segment.questionId!, qd.interruptId as string)}
            />
          );
        }

        if (block.type === 'create_workspace') {
          if (readOnly) return null;
          const wd = workspaceProposals[(block as CreateWorkspaceRenderBlock).segment.proposalId!];
          if (!wd) return null;
          return (
            <CreateWorkspaceCard
              key={block.key}
              proposalData={wd as any} // TODO: type properly — ProposalData not exported
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
              proposalData={sqd as any} // TODO: type properly — ProposalData not exported
              onApprove={onApproveStartQuestion ? () => onApproveStartQuestion(sqd) : undefined}
              onReject={onRejectStartQuestion ? () => onRejectStartQuestion(sqd) : undefined}
            />
          );
        }

        if (block.type === 'ptc_agent') {
          if (readOnly) return null;
          const pad = ptcAgentProposals[(block as PTCAgentRenderBlock).segment.proposalId!];
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
          // SharedChatView projects plan approvals only, so a pause never
          // lands in a link anyone can open.
          return (
            <CreditPauseCard
              key={block.key}
              pauseData={cpd}
              onResume={!readOnly && onResumeCreditPause ? () => onResumeCreditPause(pauseId, cpd.interruptId) : undefined}
            />
          );
        }

        return null;
      })}
      {/* Standalone preparingToolCall when no activity blocks exist yet */}
      {preparingToolCall && lastActivityBlockIdx === -1 && (
        <ActivityBlock
          items={[]}
          preparingToolCall={preparingToolCall}
          isStreaming={isStreaming ?? false}
          onToolCallClick={onToolCallDetailClick as any} // TODO: type properly
          onOpenFile={onOpenFile}
        />
      )}
      {detectedFiles.length > 0 && (!readOnly || allowFiles) && (
        <FileMentionCards filePaths={detectedFiles} onOpenFile={((readOnly && !allowFiles) ? undefined : onOpenFile)!} onOpenDir={(readOnly && !allowFiles) ? undefined : onOpenDir} />
      )}
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
