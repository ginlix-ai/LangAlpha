import { useState, useEffect, useRef } from 'react';
import { Bot, User, FileText, ImageIcon } from 'lucide-react';
import logo from '../../../assets/img/logo.svg';
import MorphLoading from '@/components/ui/morph-loading';
import ActivityAccordion from './ActivityAccordion';
import {
  INLINE_ARTIFACT_TOOLS,
  InlineStockPriceCard,
  InlineCompanyOverviewCard,
  InlineMarketIndicesCard,
  InlineSectorPerformanceCard,
  InlineSecFilingCard,
} from './charts/InlineMarketCharts';
import { getDisplayName, getToolIcon } from './toolDisplayConfig';
import { extractFilePaths, FileMentionCards } from './FileCard';
import { useAuth } from '../../../contexts/AuthContext';
import LiveActivity from './LiveActivity';
import ReasoningMessageContent from './ReasoningMessageContent';
import PlanApprovalCard from './PlanApprovalCard';
import SubagentTaskMessageContent from './SubagentTaskMessageContent';
import TextMessageContent from './TextMessageContent';
import ToolCallMessageContent from './ToolCallMessageContent';
import TodoListMessageContent from './TodoListMessageContent';

/**
 * Returns true if a line is markdown-structural (headings, lists, blockquotes,
 * code fences, horizontal rules, or table rows) and should keep its newline.
 */
const MD_STRUCTURAL_RE =
  /^(?:#|[*\-+] |\d+[.)] |>|```|---+|___+|\*\*\*+|\|)/;

function isStructuralLine(line) {
  return MD_STRUCTURAL_RE.test(line.trimStart());
}

/**
 * Normalize text content from backend for proper display in subagent views.
 * - Unescape literal \n (backslash-n) if backend sends escaped strings
 * - Collapse single newlines to spaces ONLY between plain prose lines
 * - Preserve newlines adjacent to markdown-structural lines (headings, lists, etc.)
 * - Preserve double newlines (paragraph breaks)
 */
export function normalizeSubagentText(content) {
  if (!content || typeof content !== 'string') return '';
  const s = content
    .replace(/\\n/g, '\n')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n');

  const blocks = s.split(/\n{2,}/);
  return blocks
    .map((block) => {
      const trimmed = block.trim();
      const lines = trimmed.split('\n');
      if (lines.length <= 1) return trimmed;

      let result = lines[0];
      for (let i = 1; i < lines.length; i++) {
        const prevStructural = isStructuralLine(lines[i - 1]);
        const curStructural = isStructuralLine(lines[i]);
        result += prevStructural || curStructural ? '\n' : ' ';
        result += lines[i];
      }
      return result;
    })
    .join('\n\n');
}

/** Map artifact type → inline artifact component */
const INLINE_ARTIFACT_MAP = {
  stock_prices: InlineStockPriceCard,
  company_overview: InlineCompanyOverviewCard,
  market_indices: InlineMarketIndicesCard,
  sector_performance: InlineSectorPerformanceCard,
  sec_filing: InlineSecFilingCard,
};

/* --- Attachment helpers --- */
const formatFileSize = (bytes) => {
  if (!bytes || bytes === 0) return '';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/**
 * AttachmentCard — 96x96 preview card matching FilePreviewCard styling.
 * Handles both live attachments (with preview/dataUrl) and history
 * attachments (name/type/size only).
 */
function AttachmentCard({ attachment }) {
  const att = attachment;
  const isImage = att.type?.startsWith('image/') || att.type === 'image';
  const hasPreview = att.preview || att.dataUrl;
  const ext = att.name?.split('.').pop() || '';

  if (isImage && hasPreview) {
    return (
      <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[rgba(255,255,255,0.1)] bg-[rgba(255,255,255,0.05)]">
        <img src={att.preview || att.dataUrl} alt={att.name} className="w-full h-full object-cover" />
        <div className="absolute inset-0 bg-black/20" />
      </div>
    );
  }

  if (isImage && !hasPreview) {
    // History image — no thumbnail available, show placeholder
    return (
      <div className="relative flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[rgba(255,255,255,0.1)] bg-[rgba(255,255,255,0.05)]">
        <div className="w-full h-full p-3 flex flex-col items-center justify-center gap-2">
          <ImageIcon className="w-6 h-6 text-[rgba(255,255,255,0.4)]" />
          <p className="text-[10px] text-[rgba(255,255,255,0.45)] truncate w-full text-center">{att.name}</p>
        </div>
      </div>
    );
  }

  // PDF / generic file card
  return (
    <div className="relative flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[rgba(255,255,255,0.1)] bg-[rgba(255,255,255,0.05)]">
      <div className="w-full h-full p-3 flex flex-col justify-between">
        <div className="flex items-center gap-2">
          <div className="p-1.5 bg-[rgba(255,255,255,0.1)] rounded">
            <FileText className="w-4 h-4 text-[rgba(255,255,255,0.6)]" />
          </div>
          <span className="text-[10px] font-medium text-[rgba(255,255,255,0.45)] uppercase tracking-wider truncate">
            {ext}
          </span>
        </div>
        <div className="space-y-0.5">
          <p className="text-xs font-medium text-[#BBBBBB] truncate" title={att.name}>{att.name}</p>
          {att.size > 0 && (
            <p className="text-[10px] text-[rgba(255,255,255,0.35)]">{formatFileSize(att.size)}</p>
          )}
        </div>
      </div>
    </div>
  );
}

/**
 * MessageList Component
 *
 * Displays the chat message history with support for:
 * - Empty state when no messages exist
 * - User and assistant message bubbles
 * - Streaming indicators
 * - Error state styling
 */
function MessageList({ messages, hideAvatar, compactToolCalls, isSubagentView, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick }) {
  // Empty state - show when no messages exist (hidden in subagent view)
  if (messages.length === 0) {
    if (isSubagentView) return null;
    return (
      <div className="flex flex-col items-center justify-center min-h-full py-12">
        <Bot className="h-12 w-12 mb-4" style={{ color: '#6155F5', opacity: 0.5 }} />
        <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.65 }}>
          Start a conversation by typing a message below
        </p>
      </div>
    );
  }

  // Render message list
  return (
    <div className="space-y-6">
      {messages.map((message) => (
        <MessageBubble
          key={message.id}
          message={message}
          hideAvatar={isSubagentView || hideAvatar}
          compactToolCalls={compactToolCalls}
          isSubagentView={isSubagentView}
          onOpenSubagentTask={onOpenSubagentTask}
          onOpenFile={onOpenFile}
          onOpenDir={onOpenDir}
          onToolCallDetailClick={onToolCallDetailClick}
          onApprovePlan={onApprovePlan}
          onRejectPlan={onRejectPlan}
          onPlanDetailClick={onPlanDetailClick}
        />
      ))}
    </div>
  );
}

/**
 * MessageBubble Component
 *
 * Renders a single message bubble with appropriate styling
 * based on role (user/assistant) and state (streaming/error)
 */
function MessageBubble({ message, hideAvatar, compactToolCalls, isSubagentView, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick }) {
  const { user } = useAuth();
  const avatarUrl = user?.avatar_url;
  const isUser = message.role === 'user';
  const isAssistant = message.role === 'assistant';

  const hasAttachments = isUser && message.attachments && message.attachments.length > 0;

  return (
    <div
      className={`flex gap-4 ${isUser ? 'justify-end' : 'justify-start'}`}
    >
      {/* Assistant avatar - shown on the left */}
      {isAssistant && !hideAvatar && (
        <div className="flex-shrink-0 mt-2 w-8 h-8 flex items-center justify-center">
          <img src={logo} alt="Assistant" className="w-8 h-8" />
        </div>
      )}

      {/* Message content column — bubble + standalone attachment cards */}
      <div className={`${isUser ? 'max-w-[80%] flex flex-col items-end gap-2' : 'w-full min-w-0'}`}>
        {/* Message bubble */}
        <div
          className={`rounded-lg ${
            isUser ? 'px-4 py-3 rounded-tr-none' : 'pl-0 pr-0 pb-3 rounded-tl-none'
          } overflow-hidden`}
          style={{
            backgroundColor: isUser
              ? 'var(--color-gray-292929)'
              : 'transparent',
            border: 'none',
            color: '#FFFFFF',
          }}
        >
          {/* Render content segments in chronological order */}
          {message.contentSegments && message.contentSegments.length > 0 ? (
            <MessageContentSegments
              segments={message.contentSegments}
              reasoningProcesses={message.reasoningProcesses || {}}
              toolCallProcesses={message.toolCallProcesses || {}}
              todoListProcesses={message.todoListProcesses || {}}
              subagentTasks={message.subagentTasks || {}}
              planApprovals={message.planApprovals || {}}
              pendingToolCallChunks={message.pendingToolCallChunks || {}}
              isStreaming={message.isStreaming}
              hasError={message.error}
              isAssistant={isAssistant}
              compactToolCalls={compactToolCalls}
              isSubagentView={isSubagentView}
              onOpenSubagentTask={onOpenSubagentTask}
              onOpenFile={onOpenFile}
              onOpenDir={onOpenDir}
              onToolCallDetailClick={onToolCallDetailClick}
              onApprovePlan={onApprovePlan}
              onRejectPlan={onRejectPlan}
              onPlanDetailClick={onPlanDetailClick}
              textOnly={true}
            />
          ) : (
            // Fallback for messages without segments (backward compatibility) - main chat shows text only
            (message.contentType === 'text' || !message.contentType) && (
              <TextMessageContent
                content={message.content}
                isStreaming={message.isStreaming}
                hasError={message.error}
              />
            )
          )}

          {/* Streaming indicator — hidden when dot-loader is already showing for pending chunks */}
          {message.isStreaming && !Object.keys(message.pendingToolCallChunks || {}).length && (() => {
            const hasContent = message.contentSegments?.some(s => s.content?.trim()) || message.content?.trim();
            return <MorphLoading size="sm" className={hasContent ? "mt-2" : "mt-4"} style={{ color: '#6155F5' }} />;
          })()}
        </div>

        {/* Attachment preview cards — standalone below the bubble */}
        {hasAttachments && (
          <div className="flex gap-3 overflow-x-auto">
            {message.attachments.map((att, idx) => (
              <AttachmentCard key={idx} attachment={att} />
            ))}
          </div>
        )}
      </div>

      {/* User avatar - shown on the right */}
      {isUser && !hideAvatar && (
        <div
          className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center overflow-hidden"
          style={{ backgroundColor: 'rgba(97, 85, 245, 0.2)' }}
        >
          {avatarUrl ? (
            <img src={avatarUrl} alt="User" className="w-full h-full object-cover" />
          ) : (
            <User className="h-4 w-4" style={{ color: '#6155F5' }} />
          )}
        </div>
      )}
    </div>
  );
}

/**
 * MessageContentSegments Component
 *
 * Renders content segments in chronological order.
 * In textOnly mode (main chat): groups completed reasoning + tool calls into
 * ActivityAccordion episodes separated by text, with a LiveActivity section
 * for actively streaming items.
 *
 * @param {Object} props
 * @param {Array} props.segments - Array of content segments sorted by order
 * @param {Object} props.reasoningProcesses - Object mapping reasoningId to reasoning process data
 * @param {Object} props.toolCallProcesses - Object mapping toolCallId to tool call process data
 * @param {Object} props.todoListProcesses - Object mapping todoListId to todo list data
 * @param {Object} props.subagentTasks - Object mapping subagentId to subagent task data
 * @param {boolean} props.isStreaming - Whether the message is currently streaming
 * @param {boolean} props.hasError - Whether the message has an error
 * @param {boolean} props.textOnly - If true, render text, reasoning, and tool_call segments (for main chat view)
 * @param {Function} props.onToolCallDetailClick - Callback to open detail panel for a tool call
 */
const MIN_LIVE_EXPOSURE_MS = 5000; // minimum time a tool call stays in LiveActivity
const MAX_IN_PROGRESS_MS = 15000; // max time a tool call can stay in-progress in live view before archiving
const FADE_MS = 500; // matches LiveActivity fade duration

function MessageContentSegments({ segments, reasoningProcesses, toolCallProcesses, todoListProcesses, subagentTasks, planApprovals = {}, pendingToolCallChunks = {}, isStreaming, hasError, isAssistant = false, compactToolCalls = false, isSubagentView = false, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, textOnly = false }) {
  // Force re-render timer for recently-completed tool calls that need minimum exposure
  const [, setTick] = useState(0);
  const expiryTimerRef = useRef(null);
  const nextExpiryRef = useRef(null);

  useEffect(() => {
    clearTimeout(expiryTimerRef.current);
    expiryTimerRef.current = null;

    if (nextExpiryRef.current !== null) {
      const delay = Math.max(0, nextExpiryRef.current - Date.now()) + 50;
      expiryTimerRef.current = setTimeout(() => {
        setTick((n) => n + 1);
      }, delay);
    }

    return () => clearTimeout(expiryTimerRef.current);
  });

  // Reset for this render pass
  nextExpiryRef.current = null;

  const sortedSegments = [...segments].sort((a, b) => a.order - b.order);

  // Group consecutive text segments together for better rendering
  const groupedSegments = [];
  let currentTextGroup = null;

  for (const segment of sortedSegments) {
    if (segment.type === 'text') {
      if (currentTextGroup) {
        // Append to existing text group
        currentTextGroup.content += segment.content;
        currentTextGroup.lastOrder = segment.order; // Track last order for streaming indicator
      } else {
        // Start new text group
        currentTextGroup = {
          type: 'text',
          content: segment.content,
          order: segment.order,
          lastOrder: segment.order,
        };
        groupedSegments.push(currentTextGroup);
      }
    } else if (segment.type === 'reasoning') {
      // Finalize current text group if exists (reasoning breaks text continuity)
      currentTextGroup = null;
      // Add reasoning segment
      groupedSegments.push(segment);
    } else if (segment.type === 'tool_call') {
      // Finalize current text group if exists (tool call breaks text continuity)
      currentTextGroup = null;
      // Add tool call segment
      groupedSegments.push(segment);
    } else if (segment.type === 'todo_list') {
      // Finalize current text group if exists (todo list breaks text continuity)
      currentTextGroup = null;
      // Add todo list segment
      groupedSegments.push(segment);
    } else if (segment.type === 'subagent_task') {
      // Finalize current text group if exists (subagent task breaks text continuity)
      currentTextGroup = null;
      // Add subagent task segment
      groupedSegments.push(segment);
    } else if (segment.type === 'plan_approval') {
      currentTextGroup = null;
      groupedSegments.push(segment);
    }
  }

  // textOnly mode: use inline ActivityAccordion + LiveActivity groups
  if (textOnly) {
    const filtered = groupedSegments.filter((s) => {
      if (s.type === 'text' || s.type === 'reasoning') return true;
      if (s.type === 'subagent_task') return true;
      if (s.type === 'plan_approval') return true;
      if (s.type === 'tool_call') {
        const toolName = toolCallProcesses[s.toolCallId]?.toolName;
        if (toolName === 'TodoWrite') return false;
        if (toolName === 'task' || toolName === 'Task') return false;
        if (toolName === 'SubmitPlan') return false;
        return true;
      }
      return false;
    });

    // Build flat render blocks. Consecutive reasoning/tool_call segments are grouped
    // into activity blocks. Text, subagent, and plan_approval segments flush the
    // pending activity group and appear as their own blocks.
    const renderBlocks = [];
    let pendingCompleted = [];
    let pendingLiveReasoning = null;
    let pendingLiveToolCalls = [];
    let pendingHasFadingContent = false;
    let activityCounter = 0;
    const artifactReadyIds = new Set();

    const now = Date.now();

    const flushActivity = () => {
      if (pendingCompleted.length > 0 || pendingLiveReasoning || pendingLiveToolCalls.length > 0 || pendingHasFadingContent) {
        renderBlocks.push({
          type: 'activity',
          key: `activity-${activityCounter++}`,
          completed: pendingCompleted,
          liveReasoning: pendingLiveReasoning,
          liveToolCalls: pendingLiveToolCalls,
          _hasFadingContent: pendingHasFadingContent,
        });
        pendingCompleted = [];
        pendingLiveReasoning = null;
        pendingLiveToolCalls = [];
        pendingHasFadingContent = false;
      }
    };

    for (const seg of filtered) {
      if (seg.type === 'reasoning') {
        const proc = reasoningProcesses[seg.reasoningId];
        if (!proc) continue;
        const rawContent = proc.content || '';
        const reasoningContent = isSubagentView ? normalizeSubagentText(rawContent) : rawContent;
        if (proc.isReasoning) {
          pendingLiveReasoning = {
            content: reasoningContent,
            title: proc.reasoningTitle || null,
            isReasoning: true,
          };
        } else {
          const completedAt = proc._completedAt;
          const completedAge = completedAt ? now - completedAt : Infinity;
          if (completedAge < MIN_LIVE_EXPOSURE_MS + FADE_MS) {
            // Still in LiveActivity hold/fade — keep block alive
            pendingHasFadingContent = true;
            const expiry = completedAt + MIN_LIVE_EXPOSURE_MS + FADE_MS;
            if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
              nextExpiryRef.current = expiry;
            }
          } else {
            pendingCompleted.push({
              type: 'reasoning',
              id: seg.reasoningId,
              reasoningTitle: proc.reasoningTitle || null,
              content: reasoningContent,
              reasoningComplete: proc.reasoningComplete,
            });
          }
        }
      } else if (seg.type === 'tool_call') {
        const proc = toolCallProcesses[seg.toolCallId];
        if (!proc || proc.toolName === 'TodoWrite') continue;
        if (proc.toolName === 'task' || proc.toolName === 'Task') continue;
        if (proc.toolName === 'SubmitPlan') continue;

        const createdAt = proc._createdAt;
        const age = createdAt ? now - createdAt : Infinity;

        // Artifact tools skip exposure hold once they have a result
        const isArtifactReady = INLINE_ARTIFACT_TOOLS.has(proc.toolName) && proc.toolCallResult?.artifact;

        if (proc.isInProgress && isStreaming && age < MAX_IN_PROGRESS_MS) {
          pendingLiveToolCalls.push({
            ...proc,
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
          });
          const expiry = createdAt + MAX_IN_PROGRESS_MS;
          if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
            nextExpiryRef.current = expiry;
          }
        } else if (isArtifactReady) {
          // Artifact tool with result — render immediately, no exposure hold
          artifactReadyIds.add(seg.toolCallId);
          flushActivity();
          renderBlocks.push({
            type: 'compact_artifact',
            key: `compact-${seg.toolCallId}`,
            toolCallId: seg.toolCallId,
            proc,
          });
        } else if (age < MIN_LIVE_EXPOSURE_MS && !INLINE_ARTIFACT_TOOLS.has(proc.toolName)) {
          // Recently completed (non-artifact tools) — keep in LiveActivity until minimum exposure met.
          // Artifact tools skip this so they don't show a redundant "done" card alongside their inline chart.
          // LiveActivity's internal fade (FADE_MS) runs after item leaves here.
          pendingLiveToolCalls.push({
            ...proc,
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
            _recentlyCompleted: true,
          });
          const expiry = createdAt + MIN_LIVE_EXPOSURE_MS;
          if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
            nextExpiryRef.current = expiry;
          }
        } else {
          pendingCompleted.push({
            type: 'tool_call',
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
            ...proc,
          });
        }
      } else if (seg.type === 'subagent_task') {
        flushActivity();
        renderBlocks.push({ type: 'subagent_task', key: `subagent-${seg.subagentId}`, segment: seg });
      } else if (seg.type === 'plan_approval') {
        flushActivity();
        renderBlocks.push({ type: 'plan_approval', key: `plan-${seg.planApprovalId}`, segment: seg });
      } else if (seg.type === 'text') {
        flushActivity();
        renderBlocks.push({ type: 'text', key: `text-${seg.order}`, segment: seg });
      }
    }
    // Flush trailing activity items
    flushActivity();

    // Derived values
    const chunkEntries = Object.values(pendingToolCallChunks);
    const preparingToolCall = chunkEntries.length > 0 ? {
      toolName: chunkEntries.find((c) => c.toolName)?.toolName || null,
      chunkCount: chunkEntries.reduce((sum, c) => sum + c.chunkCount, 0),
      argsLength: chunkEntries.reduce((sum, c) => sum + c.argsLength, 0),
    } : null;

    let lastTextBlockIdx = -1;
    let lastActivityBlockIdx = -1;
    let hasAnyTrulyInProgress = false;
    for (let i = 0; i < renderBlocks.length; i++) {
      const b = renderBlocks[i];
      if (b.type === 'text') lastTextBlockIdx = i;
      if (b.type === 'activity') {
        lastActivityBlockIdx = i;
        if (b.liveToolCalls.some(tc => !tc._recentlyCompleted)) {
          hasAnyTrulyInProgress = true;
        }
      }
    }

    const detectedFiles = isAssistant && !isStreaming
      ? extractFilePaths(renderBlocks.filter(b => b.type === 'text').map(b => b.segment.content).join('\n'))
      : [];

    return (
      <div className="space-y-1">
        {renderBlocks.map((block, blockIdx) => {
          if (block.type === 'activity') {
            return (
              <div key={block.key}>
                {/* Completed items ABOVE — chronological order preserved */}
                {block.completed.length > 0 && (
                  <div className={isStreaming ? 'accordion-enter-anim' : undefined}>
                    {compactToolCalls ? (
                      block.completed.map((item) => {
                        if (item.type === 'tool_call') {
                          return (
                            <ToolCallMessageContent
                              key={`tool-call-${item.toolCallId}`}
                              toolCallId={item.toolCallId}
                              toolName={item.toolName}
                              toolCall={item.toolCall}
                              toolCallResult={item.toolCallResult}
                              isInProgress={item.isInProgress || false}
                              isComplete={item.isComplete || false}
                              isFailed={item.isFailed || false}
                              onOpenFile={onOpenFile}
                            />
                          );
                        }
                        if (item.type === 'reasoning') {
                          return (
                            <ReasoningMessageContent
                              key={`reasoning-${item.id}`}
                              reasoningContent={item.content || ''}
                              isReasoning={false}
                              reasoningComplete={item.reasoningComplete || false}
                              reasoningTitle={item.reasoningTitle ?? undefined}
                            />
                          );
                        }
                        return null;
                      })
                    ) : (
                      <ActivityAccordion
                        completedItems={block.completed}
                        onToolCallClick={onToolCallDetailClick}
                        onOpenFile={onOpenFile}
                      />
                    )}
                  </div>
                )}
                {/* Live items BELOW — in-progress items stay beneath completed ones */}
                <LiveActivity
                  activeReasoning={block.liveReasoning}
                  activeToolCalls={block.liveToolCalls}
                  preparingToolCall={blockIdx === lastActivityBlockIdx ? preparingToolCall : null}
                  artifactReadyIds={artifactReadyIds}
                />
              </div>
            );
          }

          if (block.type === 'compact_artifact') {
            const artifact = block.proc.toolCallResult?.artifact;
            const ChartComponent = artifact ? INLINE_ARTIFACT_MAP[artifact.type] : null;
            if (!ChartComponent) return null;
            return (
              <div key={block.key} className="mt-1 mb-1">
                <ChartComponent
                  artifact={artifact}
                  onClick={() => onToolCallDetailClick?.(block.proc)}
                />
              </div>
            );
          }

          if (block.type === 'text') {
            const textContent = isSubagentView ? normalizeSubagentText(block.segment.content) : block.segment.content;
            return (
              <TextMessageContent
                key={block.key}
                content={textContent}
                isStreaming={isStreaming && blockIdx === lastTextBlockIdx && !hasAnyTrulyInProgress}
                hasError={hasError}
              />
            );
          }

          if (block.type === 'subagent_task') {
            const task = subagentTasks[block.segment.subagentId];
            if (!task) return null;
            const rawToolCallProcess = toolCallProcesses[block.segment.subagentId] || null;
            const toolCallProcess = rawToolCallProcess ? {
              ...rawToolCallProcess,
              _subagentResult: task.result || null,
              _subagentStatus: task.status || null,
            } : null;
            return (
              <SubagentTaskMessageContent
                key={block.key}
                subagentId={block.segment.subagentId}
                description={task.description}
                type={task.type}
                status={task.status}
                onOpen={onOpenSubagentTask}
                onDetailOpen={onToolCallDetailClick}
                toolCallProcess={toolCallProcess}
              />
            );
          }

          if (block.type === 'plan_approval') {
            const pd = planApprovals[block.segment.planApprovalId];
            if (!pd) return null;
            return (
              <PlanApprovalCard
                key={block.key}
                planData={pd}
                onApprove={onApprovePlan}
                onReject={onRejectPlan}
                onDetailClick={() => onPlanDetailClick?.(pd)}
              />
            );
          }

          return null;
        })}
        {/* Standalone preparingToolCall when no activity blocks exist yet */}
        {preparingToolCall && lastActivityBlockIdx === -1 && (
          <LiveActivity
            activeReasoning={null}
            activeToolCalls={[]}
            preparingToolCall={preparingToolCall}
          />
        )}
        {detectedFiles.length > 0 && (
          <FileMentionCards filePaths={detectedFiles} onOpenFile={onOpenFile} onOpenDir={onOpenDir} />
        )}
      </div>
    );
  }

  // Non-textOnly mode (agent panel): render all segments individually
  return (
    <div className="space-y-2">
      {groupedSegments.map((segment, index) => {
        if (segment.type === 'text') {
          // Render text content
          const isLastSegment = index === groupedSegments.length - 1;

          return (
            <div key={`text-${segment.order}-${index}`}>
              <TextMessageContent
                content={segment.content}
                isStreaming={isStreaming && isLastSegment}
                hasError={hasError}
              />
            </div>
          );
        } else if (segment.type === 'reasoning') {
          // Render reasoning
          const proc = reasoningProcesses[segment.reasoningId];
          if (!proc) return null;
          return (
            <ReasoningMessageContent
              key={`reasoning-${segment.reasoningId}`}
              reasoningContent={proc.content || ''}
              isReasoning={proc.isReasoning || false}
              reasoningComplete={proc.reasoningComplete || false}
              reasoningTitle={proc.reasoningTitle ?? undefined}
            />
          );
        } else if (segment.type === 'tool_call') {
          // Render tool call
          const proc = toolCallProcesses[segment.toolCallId];
          if (!proc || proc.toolName === 'TodoWrite' || proc.toolName === 'SubmitPlan') return null;
          return (
            <ToolCallMessageContent
              key={`tool-call-${segment.toolCallId}`}
              toolCallId={segment.toolCallId}
              toolName={proc.toolName}
              toolCall={proc.toolCall}
              toolCallResult={proc.toolCallResult}
              isInProgress={proc.isInProgress || false}
              isComplete={proc.isComplete || false}
              isFailed={proc.isFailed || false}
              onOpenFile={onOpenFile}
            />
          );
        } else if (segment.type === 'todo_list') {
          // Render todo list
          const todoListProcess = todoListProcesses[segment.todoListId];
          if (todoListProcess) {
            return (
              <TodoListMessageContent
                key={`todo-list-${segment.todoListId}`}
                todos={todoListProcess.todos || []}
                total={todoListProcess.total || 0}
                completed={todoListProcess.completed || 0}
                in_progress={todoListProcess.in_progress || 0}
                pending={todoListProcess.pending || 0}
              />
            );
          }
          return null;
        } else if (segment.type === 'subagent_task') {
          const task = subagentTasks[segment.subagentId];
          if (task) {
            return (
              <SubagentTaskMessageContent
                key={`subagent-task-${segment.subagentId}`}
                subagentId={segment.subagentId}
                description={task.description}
                type={task.type}
                status={task.status}
                onOpen={onOpenSubagentTask}
              />
            );
          }
          return null;
        } else if (segment.type === 'plan_approval') {
          const pd = planApprovals[segment.planApprovalId];
          if (pd) {
            return (
              <PlanApprovalCard
                key={`plan-${segment.planApprovalId}`}
                planData={pd}
                onApprove={onApprovePlan}
                onReject={onRejectPlan}
                onDetailClick={() => onPlanDetailClick?.(pd)}
              />
            );
          }
          return null;
        }
        return null;
      })}
    </div>
  );
}

export default MessageList;
export { MessageContentSegments };
