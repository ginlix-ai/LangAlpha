import { useState, useEffect, useRef } from 'react';
import { Bot, User } from 'lucide-react';
import logo from '../../../assets/img/logo.svg';
import MorphLoading from '@/components/ui/morph-loading';
import ActivityAccordion from './ActivityAccordion';
import { extractFilePaths, FileMentionCards } from './FileCard';
import LiveActivity from './LiveActivity';
import ReasoningMessageContent from './ReasoningMessageContent';
import PlanApprovalCard from './PlanApprovalCard';
import SubagentTaskMessageContent from './SubagentTaskMessageContent';
import TextMessageContent from './TextMessageContent';
import ToolCallMessageContent from './ToolCallMessageContent';
import TodoListMessageContent from './TodoListMessageContent';

/**
 * MessageList Component
 *
 * Displays the chat message history with support for:
 * - Empty state when no messages exist
 * - User and assistant message bubbles
 * - Streaming indicators
 * - Error state styling
 */
function MessageList({ messages, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick }) {
  // Empty state - show when no messages exist
  if (messages.length === 0) {
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
function MessageBubble({ message, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick }) {
  const isUser = message.role === 'user';
  const isAssistant = message.role === 'assistant';

  return (
    <div
      className={`flex gap-4 ${isUser ? 'justify-end' : 'justify-start'}`}
    >
      {/* Assistant avatar - shown on the left */}
      {isAssistant && (
        <div className="flex-shrink-0 mt-2 w-8 h-8 flex items-center justify-center">
          <img src={logo} alt="Assistant" className="w-8 h-8" />
        </div>
      )}

      {/* Message bubble */}
      <div
        className={`${isUser ? 'max-w-[80%]' : 'w-full min-w-0'} rounded-lg ${
          isUser ? 'px-4 py-3 rounded-tr-none' : 'pl-0 pr-0 pb-3 rounded-tl-none'
        } overflow-hidden`}
        style={{
          backgroundColor: isUser
            ? 'var(--color-gray-292929)'
            : message.error
            ? 'rgba(255, 56, 60, 0.1)'
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

      {/* User avatar - shown on the right */}
      {isUser && (
        <div
          className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center"
          style={{ backgroundColor: 'rgba(97, 85, 245, 0.2)' }}
        >
          <User className="h-4 w-4" style={{ color: '#6155F5' }} />
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

function MessageContentSegments({ segments, reasoningProcesses, toolCallProcesses, todoListProcesses, subagentTasks, planApprovals = {}, pendingToolCallChunks = {}, isStreaming, hasError, isAssistant = false, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, textOnly = false }) {
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

  // textOnly mode: use ActivityAccordion + LiveActivity pattern
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

    // Build episodes split into before/after LiveActivity to preserve chronological order.
    // Text arriving after in-progress or recently-completed tool calls renders below them.
    const episodesBefore = [];
    const episodesAfter = [];
    let currentCompleted = [];
    let activeReasoning = null;
    const activeToolCalls = [];
    let seenLiveItem = false;

    const now = Date.now();

    const flushBeforeLive = () => {
      if (!seenLiveItem && currentCompleted.length > 0) {
        episodesBefore.push({ completed: [...currentCompleted], text: null });
        currentCompleted = [];
      }
      seenLiveItem = true;
    };

    for (const seg of filtered) {
      if (seg.type === 'reasoning') {
        const proc = reasoningProcesses[seg.reasoningId];
        if (!proc) continue;
        if (proc.isReasoning) {
          flushBeforeLive();
          activeReasoning = {
            content: proc.content || '',
            title: proc.reasoningTitle || null,
            isReasoning: true,
          };
        } else {
          const completedAt = proc._completedAt;
          const completedAge = completedAt ? now - completedAt : Infinity;
          if (completedAge < MIN_LIVE_EXPOSURE_MS + FADE_MS) {
            // Still visible in LiveActivity hold/fade — treat as live for ordering
            flushBeforeLive();
            // Schedule re-render when hold+fade period expires
            const expiry = completedAt + MIN_LIVE_EXPOSURE_MS + FADE_MS;
            if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
              nextExpiryRef.current = expiry;
            }
          } else {
            // Completed reasoning — move to accordion
            currentCompleted.push({
              type: 'reasoning',
              id: seg.reasoningId,
              reasoningTitle: proc.reasoningTitle || null,
              content: proc.content || '',
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

        if (proc.isInProgress && isStreaming && age < MAX_IN_PROGRESS_MS) {
          // Keep in live view while streaming, but archive if stuck too long
          flushBeforeLive();
          activeToolCalls.push({
            ...proc,
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
          });
          // Schedule re-render at max age so stale items get archived
          const expiry = createdAt + MAX_IN_PROGRESS_MS;
          if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
            nextExpiryRef.current = expiry;
          }
        } else if (age < MIN_LIVE_EXPOSURE_MS) {
          flushBeforeLive();
          activeToolCalls.push({
            ...proc,
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
            _recentlyCompleted: true,
          });
          // Schedule re-render for when minimum exposure expires
          const expiry = createdAt + MIN_LIVE_EXPOSURE_MS;
          if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
            nextExpiryRef.current = expiry;
          }
        } else {
          // Completed tool call — move to accordion
          currentCompleted.push({
            type: 'tool_call',
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
            ...proc,
          });
        }
      } else if (seg.type === 'subagent_task') {
        const target = seenLiveItem ? episodesAfter : episodesBefore;
        target.push({ completed: [...currentCompleted], text: null, subagentTask: seg });
        currentCompleted = [];
      } else if (seg.type === 'plan_approval') {
        const target = seenLiveItem ? episodesAfter : episodesBefore;
        target.push({ completed: [...currentCompleted], text: null, planApproval: seg });
        currentCompleted = [];
      } else if (seg.type === 'text') {
        const target = seenLiveItem ? episodesAfter : episodesBefore;
        target.push({ completed: [...currentCompleted], text: seg });
        currentCompleted = [];
      }
    }

    // Remaining completed items (no text after them)
    if (currentCompleted.length > 0) {
      const target = seenLiveItem ? episodesAfter : episodesBefore;
      target.push({ completed: currentCompleted, text: null });
    }

    // Extract file paths from all text content for assistant messages (only when done streaming)
    const allEpisodes = [...episodesBefore, ...episodesAfter];
    const detectedFiles = isAssistant && !isStreaming
      ? extractFilePaths(allEpisodes.filter(ep => ep.text).map(ep => ep.text.content).join('\n'))
      : [];

    const renderEpisode = (episode, key, isLastText) => (
      <div key={key}>
        {episode.completed.length > 0 && (
          <ActivityAccordion
            completedItems={episode.completed}
            onToolCallClick={onToolCallDetailClick}
            onOpenFile={onOpenFile}
          />
        )}
        {episode.subagentTask && (() => {
          const task = subagentTasks[episode.subagentTask.subagentId];
          if (!task) return null;
          const rawToolCallProcess = toolCallProcesses[episode.subagentTask.subagentId] || null;
          // Augment with actual subagent result (from subagent_status completed_tasks)
          const toolCallProcess = rawToolCallProcess ? {
            ...rawToolCallProcess,
            _subagentResult: task.result || null,
            _subagentStatus: task.status || null,
          } : null;
          return (
            <SubagentTaskMessageContent
              subagentId={episode.subagentTask.subagentId}
              description={task.description}
              type={task.type}
              status={task.status}
              onOpen={onOpenSubagentTask}
              onDetailOpen={onToolCallDetailClick}
              toolCallProcess={toolCallProcess}
            />
          );
        })()}
        {episode.planApproval && (() => {
          const pd = planApprovals[episode.planApproval.planApprovalId];
          if (!pd) return null;
          return (
            <PlanApprovalCard
              planData={pd}
              onApprove={onApprovePlan}
              onReject={onRejectPlan}
              onDetailClick={() => onPlanDetailClick?.(pd)}
            />
          );
        })()}
        {episode.text && (
          <TextMessageContent
            content={episode.text.content}
            isStreaming={isStreaming && isLastText && !activeReasoning && activeToolCalls.length === 0}
            hasError={hasError}
          />
        )}
      </div>
    );

    // Aggregate all pending chunks into a single preparing indicator
    const chunkEntries = Object.values(pendingToolCallChunks);
    const preparingToolCall = chunkEntries.length > 0 ? {
      // Pick the first non-null tool name across all indices
      toolName: chunkEntries.find((c) => c.toolName)?.toolName || null,
      chunkCount: chunkEntries.reduce((sum, c) => sum + c.chunkCount, 0),
      argsLength: chunkEntries.reduce((sum, c) => sum + c.argsLength, 0),
    } : null;

    const hasLiveContent = !!(activeReasoning || activeToolCalls.length > 0 || preparingToolCall);

    return (
      <div className="space-y-1">
        {episodesBefore.map((episode, idx) =>
          renderEpisode(episode, `ep-${idx}`, !hasLiveContent && episodesAfter.length === 0 && idx === episodesBefore.length - 1)
        )}
        <LiveActivity
          activeReasoning={activeReasoning}
          activeToolCalls={activeToolCalls}
          preparingToolCall={preparingToolCall}
        />
        {episodesAfter.map((episode, idx) =>
          renderEpisode(episode, `ep-after-${idx}`, idx === episodesAfter.length - 1)
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
