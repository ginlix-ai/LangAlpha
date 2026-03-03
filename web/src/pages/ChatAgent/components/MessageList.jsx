import { useState, useEffect, useRef } from 'react';
import { Bot, User, FileText, ImageIcon, Pencil, RefreshCw, RotateCcw, Copy, Check, Info } from 'lucide-react';
import logoLight from '../../../assets/img/logo.svg';
import logoDark from '../../../assets/img/logo-dark.svg';
import { useTheme } from '../../../contexts/ThemeContext';
import MorphLoading from '@/components/ui/morph-loading';
import ActivityBlock from './ActivityBlock';
import {
  INLINE_ARTIFACT_TOOLS,
  InlineStockPriceCard,
  InlineCompanyOverviewCard,
  InlineMarketIndicesCard,
  InlineSectorPerformanceCard,
  InlineSecFilingCard,
  InlineStockScreenerCard,
} from './charts/InlineMarketCharts';
import { InlineAutomationCard } from './charts/InlineAutomationCards';
import { extractFilePaths, FileMentionCards } from './FileCard';
import { useAuth } from '../../../contexts/AuthContext';
import ReasoningMessageContent from './ReasoningMessageContent';
import PlanApprovalCard from './PlanApprovalCard';
import UserQuestionCard from './UserQuestionCard';
import CreateWorkspaceCard from './CreateWorkspaceCard';
import StartQuestionCard from './StartQuestionCard';
import SubagentTaskMessageContent from './SubagentTaskMessageContent';
import TextMessageContent from './TextMessageContent';
import ToolCallMessageContent from './ToolCallMessageContent';
import TodoListMessageContent from './TodoListMessageContent';
import { TextShimmer } from '@/components/ui/text-shimmer';

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
  stock_screener: InlineStockScreenerCard,
  automations: InlineAutomationCard,
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
      <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden" style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-input)' }}>
        <img src={att.preview || att.dataUrl} alt={att.name} className="w-full h-full object-cover" />
        <div className="absolute inset-0 bg-black/20" />
      </div>
    );
  }

  if (isImage && !hasPreview) {
    // History image — no thumbnail available, show placeholder
    return (
      <div className="relative flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden" style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-input)' }}>
        <div className="w-full h-full p-3 flex flex-col items-center justify-center gap-2">
          <ImageIcon className="w-6 h-6" style={{ color: 'var(--color-icon-muted)' }} />
          <p className="text-[10px] truncate w-full text-center" style={{ color: 'var(--color-text-tertiary)' }}>{att.name}</p>
        </div>
      </div>
    );
  }

  // PDF / generic file card
  return (
    <div className="relative flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden" style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-input)' }}>
      <div className="w-full h-full p-3 flex flex-col justify-between">
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded" style={{ background: 'var(--color-border-muted)' }}>
            <FileText className="w-4 h-4" style={{ color: 'var(--color-text-tertiary)' }} />
          </div>
          <span className="text-[10px] font-medium uppercase tracking-wider truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {ext}
          </span>
        </div>
        <div className="space-y-0.5">
          <p className="text-xs font-medium truncate" style={{ color: 'var(--color-text-muted)' }} title={att.name}>{att.name}</p>
          {att.size > 0 && (
            <p className="text-[10px]" style={{ color: 'var(--color-text-tertiary)' }}>{formatFileSize(att.size)}</p>
          )}
        </div>
      </div>
    </div>
  );
}

/**
 * NotificationDivider — centered inline divider for system events
 * (e.g. summarization, offload). Renders as a muted horizontal rule
 * with text, similar to date dividers in chat apps.
 */
function NotificationDivider({ message, content }) {
  const text = content ?? message?.content;
  return (
    <div
      className="flex items-center gap-3 py-2 my-1"
    >
      <div className="flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
      <span
        className="text-xs whitespace-nowrap"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {text}
      </span>
      <div className="flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
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
function MessageList({ messages, isLoading, hideAvatar, compactToolCalls, isSubagentView, readOnly, allowFiles, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, onEditMessage, onRegenerate, onRetry }) {
  // Empty state - show when no messages exist (hidden in subagent view)
  if (messages.length === 0) {
    if (isSubagentView) return null;
    return (
      <div className="flex flex-col items-center justify-center min-h-full py-12">
        <Bot className="h-12 w-12 mb-4" style={{ color: 'var(--color-accent-primary)', opacity: 0.5 }} />
        <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          Start a conversation by typing a message below
        </p>
      </div>
    );
  }

  // Render message list
  return (
    <div className="space-y-6">
      {messages.map((message) =>
        message.role === 'notification' ? (
          <NotificationDivider key={message.id} message={message} />
        ) : (
          <MessageBubble
            key={message.id}
            message={message}
            isLoading={isLoading}
            hideAvatar={isSubagentView || hideAvatar}
            compactToolCalls={compactToolCalls}
            isSubagentView={isSubagentView}
            readOnly={readOnly}
            allowFiles={allowFiles}
            onOpenSubagentTask={onOpenSubagentTask}
            onOpenFile={onOpenFile}
            onOpenDir={onOpenDir}
            onToolCallDetailClick={onToolCallDetailClick}
            onApprovePlan={onApprovePlan}
            onRejectPlan={onRejectPlan}
            onPlanDetailClick={onPlanDetailClick}
            onAnswerQuestion={onAnswerQuestion}
            onSkipQuestion={onSkipQuestion}
            onApproveCreateWorkspace={onApproveCreateWorkspace}
            onRejectCreateWorkspace={onRejectCreateWorkspace}
            onApproveStartQuestion={onApproveStartQuestion}
            onRejectStartQuestion={onRejectStartQuestion}
            onEditMessage={onEditMessage}
            onRegenerate={onRegenerate}
            onRetry={onRetry}
          />
        )
      )}
    </div>
  );
}

/**
 * MessageBubble Component
 *
 * Renders a single message bubble with appropriate styling
 * based on role (user/assistant) and state (streaming/error)
 */
function MessageBubble({ message, isLoading, hideAvatar, compactToolCalls, isSubagentView, readOnly, allowFiles, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, onEditMessage, onRegenerate, onRetry }) {
  const { user } = useAuth();
  const { theme } = useTheme();
  const logo = theme === 'light' ? logoDark : logoLight;
  const avatarUrl = user?.avatar_url;
  const isUser = message.role === 'user';
  const isAssistant = message.role === 'assistant';
  const isPendingDelivery = isUser && (message.isPending || message.queued);
  const hasAttachments = isUser && message.attachments && message.attachments.length > 0;

  // Edit mode state
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState('');
  const editTextareaRef = useRef(null);

  // Copy state
  const [copied, setCopied] = useState(false);

  // Show action buttons only when not streaming, not in subagent view, not read-only, and not loading
  const showActions = !message.isStreaming && !isSubagentView && !readOnly && !isLoading;

  const resizeTextarea = () => {
    const el = editTextareaRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = el.scrollHeight + 'px';
  };

  const handleStartEdit = () => {
    setEditContent(message.content || '');
    setIsEditing(true);
    setTimeout(() => {
      editTextareaRef.current?.focus();
      resizeTextarea();
    }, 0);
  };

  const handleCancelEdit = () => {
    setIsEditing(false);
    setEditContent('');
  };

  const handleSubmitEdit = () => {
    const trimmed = editContent.trim();
    if (trimmed && trimmed !== (message.content || '').trim()) {
      onEditMessage?.(message.id, trimmed);
    }
    setIsEditing(false);
    setEditContent('');
  };

  const handleEditKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmitEdit();
    } else if (e.key === 'Escape') {
      handleCancelEdit();
    }
  };

  const handleCopy = () => {
    // Collect all text content from segments
    const text = message.contentSegments
      ?.filter((s) => s.type === 'text')
      .map((s) => s.content)
      .join('') || message.content || '';
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div
      className={`group flex gap-4 ${isUser ? 'justify-end' : 'justify-start'}`}
    >
      {/* Assistant avatar - shown on the left */}
      {isAssistant && !hideAvatar && (
        <div className="flex-shrink-0 mt-2 w-8 h-8 flex items-center justify-center">
          <img src={logo} alt="Assistant" className="w-8 h-8" />
        </div>
      )}

      {/* Message content column — bubble + standalone attachment cards */}
      <div className={`${isUser ? 'max-w-[80%] flex flex-col items-end gap-2' : 'w-full min-w-0'}`}>

        {/* ===== EDIT MODE (user messages) ===== */}
        {isEditing && isUser ? (
          <div className="w-full flex flex-col gap-2">
            {/* Attachment preview cards — above the edit textarea */}
            {hasAttachments && (
              <div className="flex gap-3 overflow-x-auto">
                {message.attachments.map((att, idx) => (
                  <AttachmentCard key={idx} attachment={att} />
                ))}
              </div>
            )}

            {/* Bordered textarea container */}
            <div
              className="rounded-xl px-4 py-3"
              style={{
                border: '2px solid var(--color-accent-primary, #6b7280)',
                backgroundColor: 'transparent',
                color: 'var(--color-text-primary)',
              }}
            >
              <textarea
                ref={editTextareaRef}
                value={editContent}
                onChange={(e) => {
                  setEditContent(e.target.value);
                  resizeTextarea();
                }}
                onKeyDown={handleEditKeyDown}
                className="w-full bg-transparent text-sm resize-none outline-none leading-relaxed overflow-hidden"
                style={{ color: 'var(--color-text-primary)' }}
                rows={1}
              />
            </div>

            {/* Info text + Cancel/Save row */}
            <div className="flex items-center gap-3">
              <div className="flex items-start gap-1.5 flex-1 min-w-0">
                <Info className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
                <span className="text-xs leading-snug" style={{ color: 'var(--color-text-tertiary)' }}>
                  This will branch from the current thread. Messages after this point will be replaced and cannot be recovered.
                </span>
              </div>
              <div className="flex gap-2 flex-shrink-0">
                <button
                  onClick={handleCancelEdit}
                  className="px-4 py-1.5 rounded-full text-sm font-medium transition-colors"
                  style={{
                    color: 'var(--color-text-primary)',
                    border: '1px solid var(--color-border, #d1d5db)',
                  }}
                >
                  Cancel
                </button>
                <button
                  onClick={handleSubmitEdit}
                  className="px-4 py-1.5 rounded-full text-sm font-medium transition-colors"
                  style={{
                    color: 'var(--color-text-on-accent, #fff)',
                    backgroundColor: 'var(--color-text-secondary)',
                  }}
                >
                  Save
                </button>
              </div>
            </div>
          </div>
        ) : (
        <>
        {/* ===== NORMAL MODE ===== */}
        {/* Message bubble */}
        <div
          className={`rounded-lg ${
            isUser ? 'px-4 py-3 rounded-tr-none overflow-hidden' : 'pl-0 pr-0 pb-3 rounded-tl-none'
          }`}
          style={{
            backgroundColor: isUser
              ? 'var(--color-bg-elevated)'
              : 'transparent',
            border: 'none',
            color: 'var(--color-text-primary)',
          }}
        >
          {isPendingDelivery ? (
            <TextShimmer
              as="span"
              className="text-sm [--base-color:var(--color-text-secondary)] [--base-gradient-color:var(--color-text-primary)]"
              duration={1.5}
            >
              {message.content || ''}
            </TextShimmer>
          ) : (
          <>
          {/* Render content segments in chronological order */}
          {message.contentSegments && message.contentSegments.length > 0 ? (
            <MessageContentSegments
              segments={message.contentSegments}
              reasoningProcesses={message.reasoningProcesses || {}}
              toolCallProcesses={message.toolCallProcesses || {}}
              todoListProcesses={message.todoListProcesses || {}}
              subagentTasks={message.subagentTasks || {}}
              planApprovals={message.planApprovals || {}}
              userQuestions={message.userQuestions || {}}
              workspaceProposals={message.workspaceProposals || {}}
              questionProposals={message.questionProposals || {}}
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
              onAnswerQuestion={onAnswerQuestion}
              onSkipQuestion={onSkipQuestion}
              onApproveCreateWorkspace={onApproveCreateWorkspace}
              onRejectCreateWorkspace={onRejectCreateWorkspace}
              onApproveStartQuestion={onApproveStartQuestion}
              onRejectStartQuestion={onRejectStartQuestion}
              textOnly={true}
              readOnly={readOnly}
              allowFiles={allowFiles}
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
          </>
          )}

          {/* Streaming indicator — hidden when dot-loader is already showing for pending chunks */}
          {message.isStreaming && !Object.keys(message.pendingToolCallChunks || {}).length && (() => {
            const hasContent = message.contentSegments?.some(s => s.content?.trim()) || message.content?.trim();
            return <MorphLoading size="sm" className={hasContent ? "mt-2" : "mt-4"} style={{ color: 'var(--color-accent-primary)' }} />;
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
        </>
        )}

        {/* Message action buttons — visible on hover */}
        {showActions && !isEditing && (
          <div
            className={`flex gap-1 mt-0.5 opacity-0 group-hover:opacity-100 transition-opacity ${
              isUser ? 'justify-end' : 'justify-start'
            }`}
          >
            {isUser && onEditMessage && (
              <button
                onClick={handleStartEdit}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title="Edit message"
              >
                <Pencil className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
              </button>
            )}
            {isAssistant && !message.error && onRegenerate && (
              <button
                onClick={() => onRegenerate(message.id)}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title="Regenerate response"
              >
                <RefreshCw className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
              </button>
            )}
            {isAssistant && (
              <button
                onClick={handleCopy}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title={copied ? 'Copied!' : 'Copy message'}
              >
                {copied
                  ? <Check className="h-3.5 w-3.5" style={{ color: 'var(--color-gain)' }} />
                  : <Copy className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                }
              </button>
            )}
            {isAssistant && message.error && onRetry && (
              <button
                onClick={onRetry}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title="Retry"
              >
                <RotateCcw className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
              </button>
            )}
          </div>
        )}
      </div>

      {/* User avatar - shown on the right */}
      {isUser && !hideAvatar && (
        <div
          className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center overflow-hidden"
          style={{ backgroundColor: 'var(--color-accent-soft)' }}
        >
          {avatarUrl ? (
            <img src={avatarUrl} alt="User" className="w-full h-full object-cover" />
          ) : (
            <User className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
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
 * ActivityBlock episodes separated by text, with a live zone for actively
 * streaming items.
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
const MIN_LIVE_EXPOSURE_MS = 5000; // minimum time an item stays in the live zone
const MAX_IN_PROGRESS_MS = 15000; // max time a tool call can stay in-progress in live view before archiving
/** Tools that should stay in the live zone for their entire duration (no MAX_IN_PROGRESS_MS cap) */
const ALWAYS_LIVE_TOOLS = new Set(['Wait']);

function MessageContentSegments({ segments, reasoningProcesses, toolCallProcesses, todoListProcesses, subagentTasks, planApprovals = {}, userQuestions = {}, workspaceProposals = {}, questionProposals = {}, pendingToolCallChunks = {}, isStreaming, hasError, isAssistant = false, compactToolCalls = false, isSubagentView = false, readOnly = false, allowFiles = false, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, textOnly = false }) {
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
    } else if (segment.type === 'user_question') {
      currentTextGroup = null;
      groupedSegments.push(segment);
    } else if (segment.type === 'create_workspace') {
      currentTextGroup = null;
      groupedSegments.push(segment);
    } else if (segment.type === 'start_question') {
      currentTextGroup = null;
      groupedSegments.push(segment);
    } else if (segment.type === 'notification') {
      currentTextGroup = null;
      groupedSegments.push(segment);
    }
  }

  // textOnly mode: use inline ActivityBlock groups
  if (textOnly) {
    const filtered = groupedSegments.filter((s) => {
      if (s.type === 'text' || s.type === 'reasoning') return true;
      if (s.type === 'notification') return true;
      if (s.type === 'subagent_task') return true;
      if (s.type === 'plan_approval') return true;
      if (s.type === 'user_question') return true;
      if (s.type === 'create_workspace') return true;
      if (s.type === 'start_question') return true;
      if (s.type === 'tool_call') {
        const toolName = toolCallProcesses[s.toolCallId]?.toolName;
        if (toolName === 'TodoWrite') return false;
        if (toolName === 'task' || toolName === 'Task') return false;
        if (toolName === 'SubmitPlan' || toolName === 'AskUserQuestion') return false;
        if (toolName === 'create_workspace' || toolName === 'start_question') return false;
        return true;
      }
      return false;
    });

    // Build flat render blocks. Consecutive reasoning/tool_call segments are grouped
    // into activity blocks. Text, subagent, and plan_approval segments flush the
    // pending activity group and appear as their own blocks.
    const renderBlocks = [];
    let pendingItems = [];
    let activityCounter = 0;

    const now = Date.now();

    const flushActivity = () => {
      if (pendingItems.length > 0) {
        renderBlocks.push({
          type: 'activity',
          key: `activity-${activityCounter++}`,
          items: pendingItems,
        });
        pendingItems = [];
      }
    };

    for (const seg of filtered) {
      if (seg.type === 'reasoning') {
        const proc = reasoningProcesses[seg.reasoningId];
        if (!proc) continue;
        const rawContent = proc.content || '';
        const reasoningContent = isSubagentView ? normalizeSubagentText(rawContent) : rawContent;

        if (proc.isReasoning) {
          // Active reasoning — live zone
          pendingItems.push({
            type: 'reasoning',
            id: seg.reasoningId,
            reasoningTitle: proc.reasoningTitle || null,
            content: reasoningContent,
            _liveState: 'active',
          });
        } else {
          const completedAt = proc._completedAt;
          const completedAge = completedAt ? now - completedAt : Infinity;

          if (completedAge < MIN_LIVE_EXPOSURE_MS) {
            // Still within minimum exposure — live zone as 'completing'
            pendingItems.push({
              type: 'reasoning',
              id: seg.reasoningId,
              reasoningTitle: proc.reasoningTitle || null,
              content: reasoningContent,
              reasoningComplete: proc.reasoningComplete,
              _liveState: 'completing',
            });
            const expiry = completedAt + MIN_LIVE_EXPOSURE_MS;
            if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
              nextExpiryRef.current = expiry;
            }
          } else {
            // Fully completed — accordion zone
            pendingItems.push({
              type: 'reasoning',
              id: seg.reasoningId,
              reasoningTitle: proc.reasoningTitle || null,
              content: reasoningContent,
              reasoningComplete: proc.reasoningComplete,
              _liveState: 'completed',
            });
          }
        }
      } else if (seg.type === 'tool_call') {
        const proc = toolCallProcesses[seg.toolCallId];
        if (!proc || proc.toolName === 'TodoWrite') continue;
        if (proc.toolName === 'task' || proc.toolName === 'Task') continue;
        if (proc.toolName === 'SubmitPlan' || proc.toolName === 'AskUserQuestion') continue;

        const createdAt = proc._createdAt;
        const age = createdAt ? now - createdAt : Infinity;

        // Artifact tools skip exposure hold once they have a result
        const isArtifactReady = INLINE_ARTIFACT_TOOLS.has(proc.toolName) && proc.toolCallResult?.artifact;

        const isAlwaysLive = ALWAYS_LIVE_TOOLS.has(proc.toolName);

        if (proc.isInProgress && isStreaming && (isAlwaysLive || age < MAX_IN_PROGRESS_MS)) {
          // In-progress — live zone as 'active'
          pendingItems.push({
            type: 'tool_call',
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
            ...proc,
            _liveState: 'active',
          });
          if (!isAlwaysLive) {
            const expiry = createdAt + MAX_IN_PROGRESS_MS;
            if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
              nextExpiryRef.current = expiry;
            }
          }
        } else if (isArtifactReady) {
          // Artifact tool with result — render as standalone compact_artifact
          flushActivity();
          renderBlocks.push({
            type: 'compact_artifact',
            key: `compact-${seg.toolCallId}`,
            toolCallId: seg.toolCallId,
            proc,
          });
        } else if (age < MIN_LIVE_EXPOSURE_MS && !INLINE_ARTIFACT_TOOLS.has(proc.toolName)) {
          // Recently completed — live zone as 'completing'
          pendingItems.push({
            type: 'tool_call',
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
            ...proc,
            _recentlyCompleted: true,
            _liveState: 'completing',
          });
          const expiry = createdAt + MIN_LIVE_EXPOSURE_MS;
          if (nextExpiryRef.current === null || expiry < nextExpiryRef.current) {
            nextExpiryRef.current = expiry;
          }
        } else {
          // Fully completed — accordion zone
          pendingItems.push({
            type: 'tool_call',
            id: seg.toolCallId,
            toolCallId: seg.toolCallId,
            ...proc,
            _liveState: 'completed',
          });
        }
      } else if (seg.type === 'subagent_task') {
        flushActivity();
        renderBlocks.push({ type: 'subagent_task', key: `subagent-${seg.subagentId}`, segment: seg });
      } else if (seg.type === 'plan_approval') {
        flushActivity();
        renderBlocks.push({ type: 'plan_approval', key: `plan-${seg.planApprovalId}`, segment: seg });
      } else if (seg.type === 'user_question') {
        flushActivity();
        renderBlocks.push({ type: 'user_question', key: `question-${seg.questionId}`, segment: seg });
      } else if (seg.type === 'create_workspace') {
        flushActivity();
        renderBlocks.push({ type: 'create_workspace', key: `workspace-${seg.proposalId}`, segment: seg });
      } else if (seg.type === 'start_question') {
        flushActivity();
        renderBlocks.push({ type: 'start_question', key: `start-question-${seg.proposalId}`, segment: seg });
      } else if (seg.type === 'notification') {
        flushActivity();
        renderBlocks.push({ type: 'notification', key: `notification-${seg.order}`, segment: seg });
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
        if (b.items.some(item => item._liveState === 'active' && item.type === 'tool_call')) {
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
            if (compactToolCalls) {
              // Compact mode: render completed items individually
              const completedItems = block.items.filter(i => i._liveState === 'completed');
              return (
                <div key={block.key}>
                  {completedItems.map((item) => {
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
                  })}
                </div>
              );
            }

            return (
              <ActivityBlock
                key={block.key}
                items={block.items}
                preparingToolCall={blockIdx === lastActivityBlockIdx ? preparingToolCall : null}
                isStreaming={isStreaming}
                onToolCallClick={onToolCallDetailClick}
                onOpenFile={onOpenFile}
              />
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

          if (block.type === 'notification') {
            return (
              <NotificationDivider key={block.key} content={block.segment.content} />
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
                action={task.action}
                resumeTargetId={task.resumeTargetId}
                onOpen={readOnly ? undefined : onOpenSubagentTask}
                onDetailOpen={readOnly ? undefined : onToolCallDetailClick}
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
                onApprove={readOnly ? undefined : onApprovePlan}
                onReject={readOnly ? undefined : onRejectPlan}
                onDetailClick={readOnly ? undefined : () => onPlanDetailClick?.(pd)}
              />
            );
          }

          if (block.type === 'user_question') {
            const qd = userQuestions[block.segment.questionId];
            if (!qd) return null;
            return (
              <UserQuestionCard
                key={block.key}
                questionData={qd}
                onAnswer={readOnly ? undefined : (answer) => onAnswerQuestion(answer, block.segment.questionId, qd.interruptId)}
                onSkip={readOnly ? undefined : () => onSkipQuestion(block.segment.questionId, qd.interruptId)}
              />
            );
          }

          if (block.type === 'create_workspace') {
            if (readOnly) return null;
            const wd = workspaceProposals[block.segment.proposalId];
            if (!wd) return null;
            return (
              <CreateWorkspaceCard
                key={block.key}
                proposalData={wd}
                onApprove={onApproveCreateWorkspace}
                onReject={onRejectCreateWorkspace}
              />
            );
          }

          if (block.type === 'start_question') {
            if (readOnly) return null;
            const sqd = questionProposals[block.segment.proposalId];
            if (!sqd) return null;
            return (
              <StartQuestionCard
                key={block.key}
                proposalData={sqd}
                onApprove={onApproveStartQuestion}
                onReject={onRejectStartQuestion}
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
            isStreaming={isStreaming}
            onToolCallClick={onToolCallDetailClick}
            onOpenFile={onOpenFile}
          />
        )}
        {detectedFiles.length > 0 && (!readOnly || allowFiles) && (
          <FileMentionCards filePaths={detectedFiles} onOpenFile={(readOnly && !allowFiles) ? undefined : onOpenFile} onOpenDir={(readOnly && !allowFiles) ? undefined : onOpenDir} />
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
          if (!proc || proc.toolName === 'TodoWrite' || proc.toolName === 'SubmitPlan' || proc.toolName === 'AskUserQuestion') return null;
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
                action={task.action}
                resumeTargetId={task.resumeTargetId}
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
        } else if (segment.type === 'user_question') {
          const qd = userQuestions[segment.questionId];
          if (qd) {
            return (
              <UserQuestionCard
                key={`question-${segment.questionId}`}
                questionData={qd}
                onAnswer={(answer) => onAnswerQuestion(answer, segment.questionId, qd.interruptId)}
                onSkip={() => onSkipQuestion(segment.questionId, qd.interruptId)}
              />
            );
          }
          return null;
        } else if (segment.type === 'create_workspace') {
          const wd = workspaceProposals[segment.proposalId];
          if (wd) {
            return (
              <CreateWorkspaceCard
                key={`workspace-${segment.proposalId}`}
                proposalData={wd}
                onApprove={onApproveCreateWorkspace}
                onReject={onRejectCreateWorkspace}
              />
            );
          }
          return null;
        } else if (segment.type === 'start_question') {
          const sqd = questionProposals[segment.proposalId];
          if (sqd) {
            return (
              <StartQuestionCard
                key={`start-question-${segment.proposalId}`}
                proposalData={sqd}
                onApprove={onApproveStartQuestion}
                onReject={onRejectStartQuestion}
              />
            );
          }
          return null;
        } else if (segment.type === 'notification') {
          return (
            <NotificationDivider key={`notification-${segment.order}-${index}`} content={segment.content} />
          );
        }
        return null;
      })}
    </div>
  );
}

export default MessageList;
export { MessageContentSegments };
