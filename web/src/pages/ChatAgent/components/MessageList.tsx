import React, { useState, useEffect, useRef, useMemo, memo } from 'react';
import { useTranslation } from 'react-i18next';
import { Bot, User, FileText, ImageIcon, Pencil, RefreshCw, RotateCcw, Copy, Check, Info, ThumbsUp, ThumbsDown } from 'lucide-react';
import { type WidgetContextPreviewShape } from '@/pages/Dashboard/widgets/framework/WidgetContextPreview';
import { WidgetContextDeck } from '@/pages/Dashboard/widgets/framework/WidgetContextDeck';
import { useIsMobile } from '@/hooks/useIsMobile';

import ThumbDownModal from './ThumbDownModal';
import logoLight from '../../../assets/img/logo.svg';
import logoDark from '../../../assets/img/logo-dark.svg';
import { useTheme } from '../../../contexts/ThemeContext';
import LissajousLoading from '@/components/ui/lissajous-loading';
import ActivityBlock from './ActivityBlock';
import {
  INLINE_ARTIFACT_TOOLS,
  InlineStockPriceCard,
  InlineCompanyOverviewCard,
  InlineMarketIndicesCard,
  InlineSectorPerformanceCard,
  InlineSecFilingCard,
  InlineStockScreenerCard,
  InlineWebSearchCard,
} from './charts/InlineArtifactCards';
import { InlineAutomationCard } from './charts/InlineAutomationCards';
import { InlinePreviewCard } from './charts/InlinePreviewCard';
import { extractFilePaths, FileMentionCards } from './FileCard';
import { normalizeFileRefs } from '../utils/normalizeFileRefs';
import { useUser } from '@/hooks/useUser';
import ReasoningMessageContent from './ReasoningMessageContent';
import PlanApprovalCard from './PlanApprovalCard';
import UserQuestionCard from './UserQuestionCard';
import CreateWorkspaceCard from './CreateWorkspaceCard';
import StartQuestionCard from './StartQuestionCard';
import PTCAgentCard from './PTCAgentCard';
import SecretaryConfirmCard from './SecretaryConfirmCard';
import SubagentTaskMessageContent from './SubagentTaskMessageContent';
import TextMessageContent from './TextMessageContent';
import InlineWidget from './viewers/InlineWidget';
import ToolCallMessageContent from './ToolCallMessageContent';
import { CitationMetadataProvider } from './CitationMetadataContext';

import { TextShimmer } from '@/components/ui/text-shimmer';

// Stable empty object to avoid defeating React.memo with fresh `|| {}` fallbacks
const EMPTY_OBJ = {} as Record<string, never>;

// --- Shared Types ---

/** Loosely typed message record from SSE/API */
type MessageRecord = Record<string, unknown>;

/** Loosely typed tool call process record */
type ToolCallProcessRecord = Record<string, unknown>;

/** Content segment from message data */
interface ContentSegmentRecord {
  type: string;
  content?: string;
  order: number;
  lastOrder?: number;
  reasoningId?: string;
  toolCallId?: string;
  todoListId?: string;
  subagentId?: string;
  planApprovalId?: string;
  questionId?: string;
  proposalId?: string;
  widgetId?: string;
  /** Notification-only: longer text (compaction summary) shown under toggle. */
  detail?: string;
}

/** Subagent info for opening subagent task tabs */
interface SubagentInfo {
  subagentId: string;
  description?: string;
  type?: string;
  status?: string;
}

/** Feedback result from API */
interface FeedbackResult {
  rating: string | null;
  [key: string]: unknown;
}

// --- Helpers ---

/**
 * Returns true if a line is markdown-structural (headings, lists, blockquotes,
 * code fences, horizontal rules, or table rows) and should keep its newline.
 */
const MD_STRUCTURAL_RE =
  /^(?:#|[*\-+] |\d+[.)] |>|```|---+|___+|\*\*\*+|\||\[)/;

function isStructuralLine(line: string): boolean {
  return MD_STRUCTURAL_RE.test(line.trimStart());
}

/**
 * Normalize text content from backend for proper display in subagent views.
 * - Unescape literal \n (backslash-n) if backend sends escaped strings
 * - Collapse single newlines to spaces ONLY between plain prose lines
 * - Preserve newlines adjacent to markdown-structural lines (headings, lists, etc.)
 * - Preserve double newlines (paragraph breaks)
 */
// eslint-disable-next-line react-refresh/only-export-components
export function normalizeSubagentText(content: string | null | undefined): string {
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

/** Map artifact type -> inline artifact component */
const INLINE_ARTIFACT_MAP: Record<string, React.ComponentType<{ artifact: Record<string, unknown>; onClick?: () => void }>> = {
  stock_prices: InlineStockPriceCard,
  company_overview: InlineCompanyOverviewCard,
  market_indices: InlineMarketIndicesCard,
  sector_performance: InlineSectorPerformanceCard,
  sec_filing: InlineSecFilingCard,
  stock_screener: InlineStockScreenerCard,
  automations: InlineAutomationCard,
  preview_url: InlinePreviewCard,
  web_search: InlineWebSearchCard,
};

/* --- Attachment helpers --- */
const formatFileSize = (bytes: number | null | undefined): string => {
  if (!bytes || bytes === 0) return '';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

// --- AttachmentCard ---

interface AttachmentData {
  name?: string;
  type?: string;
  size?: number;
  preview?: string;
  dataUrl?: string;
  url?: string;
  [key: string]: unknown;
}

interface AttachmentCardProps {
  attachment: AttachmentData;
}

/** Local alias for the preview shape — keeps inline-deck internals decoupled from the import path. */
type WidgetChipShape = WidgetContextPreviewShape;

/**
 * Static read-only deck rendered below the user message bubble. Reuses the
 * shared `WidgetContextDeck` — no eyebrow, no remove buttons, just the
 * stacked cards with click-to-fan / click-to-preview semantics. Width is
 * pinned at 320px since the user-message column has a `max-w-[80%]`
 * constraint that would otherwise collapse absolute-positioned cards.
 */
function InlineWidgetDeck({ snapshots }: { snapshots: WidgetChipShape[] }) {
  const [fanned, setFanned] = useState(false);
  return (
    <WidgetContextDeck
      snapshots={snapshots}
      fanned={fanned}
      onToggleFan={() => setFanned((p) => !p)}
      onCollapse={() => setFanned(false)}
      compactCardGrid
      style={{
        width: 320,
        borderBottom: 'none',
        marginBottom: 0,
        paddingBottom: 0,
      }}
    />
  );
}

/**
 * AttachmentCard -- 96x96 preview card matching FilePreviewCard styling.
 * Handles both live attachments (with preview/dataUrl) and history
 * attachments (name/type/size only).
 */
function AttachmentCard({ attachment }: AttachmentCardProps): React.ReactElement {
  const att = attachment;
  const isImage = att.type?.startsWith('image/') || att.type === 'image';
  const hasPreview = att.dataUrl || att.url || att.preview;
  const ext = att.name?.split('.').pop() || '';

  if (isImage && hasPreview) {
    return (
      <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden" style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-input)' }}>
        <img src={att.dataUrl || att.url || att.preview} alt={att.name} className="w-full h-full object-cover" />
        <div className="absolute inset-0 bg-black/20" />
      </div>
    );
  }

  if (isImage && !hasPreview) {
    // History image -- no thumbnail available, show placeholder
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
          {(att.size ?? 0) > 0 && (
            <p className="text-[10px]" style={{ color: 'var(--color-text-tertiary)' }}>{formatFileSize(att.size)}</p>
          )}
        </div>
      </div>
    </div>
  );
}

// --- NotificationDivider ---

interface NotificationDividerProps {
  message?: MessageRecord;
  content?: string;
  detail?: string;
}

/**
 * NotificationDivider -- centered inline divider for system events
 * (e.g. compaction, offload). Renders as a muted horizontal rule with
 * text, similar to date dividers in chat apps. When ``detail`` is set
 * (e.g. the compaction summary), a "View summary" toggle reveals the
 * full text in a muted panel below the divider.
 */
function NotificationDivider({ message, content, detail }: NotificationDividerProps): React.ReactElement {
  const { t } = useTranslation();
  const text = content ?? (message?.content as string | undefined);
  const effectiveDetail =
    detail ?? ((message as { detail?: string } | undefined)?.detail);
  const hasDetail = typeof effectiveDetail === 'string' && effectiveDetail.trim().length > 0;
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="my-1">
      <div className="flex items-center gap-3 py-2">
        <div className="flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
        <span
          className="text-xs whitespace-nowrap"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {text}
        </span>
        {hasDetail && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="text-xs underline-offset-2 hover:underline whitespace-nowrap"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {expanded ? t('chat.hideSummary') : t('chat.viewSummary')}
          </button>
        )}
        <div className="flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />
      </div>
      {hasDetail && expanded && (
        <div
          className="mt-1 mb-2 mx-auto max-w-3xl rounded-md px-3 py-2 text-sm whitespace-pre-wrap"
          style={{
            color: 'var(--color-text-secondary)',
            background: 'var(--color-bg-subtle)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          {effectiveDetail}
        </div>
      )}
    </div>
  );
}

// --- MessageList ---

interface MessageListProps {
  messages: MessageRecord[];
  isLoading?: boolean;
  isLoadingHistory?: boolean;
  hideAvatar?: boolean;
  compactToolCalls?: boolean;
  isSubagentView?: boolean;
  readOnly?: boolean;
  allowFiles?: boolean;
  onOpenSubagentTask?: (info: SubagentInfo) => void;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  onOpenDir?: (dirPath: string) => void;
  onToolCallDetailClick?: (proc: ToolCallProcessRecord) => void;
  onApprovePlan?: () => void;
  onRejectPlan?: () => void;
  onPlanDetailClick?: (planData: Record<string, unknown>) => void;
  onAnswerQuestion?: (answer: string, questionId: string, interruptId: string) => void;
  onSkipQuestion?: (questionId: string, interruptId: string) => void;
  onApproveCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onRejectCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onApproveStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onRejectStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onApprovePTCAgent?: (proposalData: Record<string, unknown>, overrides?: { report_back?: boolean }) => void;
  onRejectPTCAgent?: (proposalData: Record<string, unknown>) => void;
  onApproveSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  onRejectSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  onEditMessage?: (messageId: string, content: string) => void;
  onRegenerate?: (messageId: string) => void;
  onRetry?: () => void;
  onThumbUp?: (messageId: string) => Promise<FeedbackResult | null>;
  onThumbDown?: (messageId: string, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => Promise<FeedbackResult | null>;
  getFeedbackForMessage?: (messageId: string) => FeedbackResult | null;
  onReportWithAgent?: (instruction: string) => void;
  onWidgetSendPrompt?: (text: string) => void;
  flashContext?: { threadId: string; workspaceId: string } | null;
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
function MessageList({ messages, isLoading, isLoadingHistory, hideAvatar, compactToolCalls, isSubagentView, readOnly, allowFiles, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, onApprovePTCAgent, onRejectPTCAgent, onApproveSecretaryAction, onRejectSecretaryAction, onEditMessage, onRegenerate, onRetry, onThumbUp, onThumbDown, getFeedbackForMessage, onReportWithAgent, onWidgetSendPrompt, flashContext }: MessageListProps): React.ReactElement | null {
  const isMobile = useIsMobile();

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
          <div className="flex gap-4">
            <div className="w-8 h-8 rounded-full flex-shrink-0" style={{ background: 'var(--color-border-muted)' }} />
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
          <div className="flex gap-4">
            <div className="w-8 h-8 rounded-full flex-shrink-0" style={{ background: 'var(--color-border-muted)' }} />
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
        <Bot className="h-12 w-12 mb-4" style={{ color: 'var(--color-accent-primary)', opacity: 0.5 }} />
        <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          Start a conversation by typing a message below
        </p>
      </div>
    );
  }

  // Render message list
  return (
    <div className={isMobile ? 'space-y-4' : 'space-y-6'}>
      {messages.map((message) =>
        (message.role as string) === 'notification' ? (
          <NotificationDivider key={message.id as string} message={message} />
        ) : (
          <MessageBubble
            key={message.id as string}
            message={message}
            isLoading={isLoading}
            hideAvatar={isSubagentView || hideAvatar}
            compactToolCalls={compactToolCalls}
            isSubagentView={isSubagentView}
            readOnly={readOnly}
            allowFiles={allowFiles}
            isMobile={isMobile}
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
            onApprovePTCAgent={onApprovePTCAgent}
            onRejectPTCAgent={onRejectPTCAgent}
            onApproveSecretaryAction={onApproveSecretaryAction}
            onRejectSecretaryAction={onRejectSecretaryAction}
            onEditMessage={onEditMessage}
            onRegenerate={onRegenerate}
            onRetry={onRetry}
            onThumbUp={onThumbUp}
            onThumbDown={onThumbDown}
            getFeedbackForMessage={getFeedbackForMessage}
            onReportWithAgent={onReportWithAgent}
            onWidgetSendPrompt={onWidgetSendPrompt}
            flashContext={flashContext}
          />
        )
      )}
    </div>
  );
}

// --- MessageBubble ---

interface MessageBubbleProps {
  message: MessageRecord;
  isLoading?: boolean;
  hideAvatar?: boolean;
  compactToolCalls?: boolean;
  isSubagentView?: boolean;
  readOnly?: boolean;
  allowFiles?: boolean;
  onOpenSubagentTask?: (info: SubagentInfo) => void;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  onOpenDir?: (dirPath: string) => void;
  onToolCallDetailClick?: (proc: ToolCallProcessRecord) => void;
  onApprovePlan?: () => void;
  onRejectPlan?: () => void;
  onPlanDetailClick?: (planData: Record<string, unknown>) => void;
  onAnswerQuestion?: (answer: string, questionId: string, interruptId: string) => void;
  onSkipQuestion?: (questionId: string, interruptId: string) => void;
  onApproveCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onRejectCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onApproveStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onRejectStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onApprovePTCAgent?: (proposalData: Record<string, unknown>, overrides?: { report_back?: boolean }) => void;
  onRejectPTCAgent?: (proposalData: Record<string, unknown>) => void;
  onApproveSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  onRejectSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  onEditMessage?: (messageId: string, content: string) => void;
  onRegenerate?: (messageId: string) => void;
  onRetry?: () => void;
  onThumbUp?: (messageId: string) => Promise<FeedbackResult | null>;
  onThumbDown?: (messageId: string, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => Promise<FeedbackResult | null>;
  getFeedbackForMessage?: (messageId: string) => FeedbackResult | null;
  onReportWithAgent?: (instruction: string) => void;
  onWidgetSendPrompt?: (text: string) => void;
  isMobile?: boolean;
  flashContext?: { threadId: string; workspaceId: string } | null;
}

/**
 * MessageBubble Component
 *
 * Renders a single message bubble with appropriate styling
 * based on role (user/assistant) and state (streaming/error)
 *
 * Wrapped with React.memo — safe because updateMessage() in messageHelpers.ts
 * returns the same object reference for unchanged messages.
 */
const MessageBubble = memo(function MessageBubble({ message, isLoading, hideAvatar, compactToolCalls, isSubagentView, readOnly, allowFiles, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, onApprovePTCAgent, onRejectPTCAgent, onApproveSecretaryAction, onRejectSecretaryAction, onEditMessage, onRegenerate, onRetry, onThumbUp, onThumbDown, getFeedbackForMessage, onReportWithAgent, onWidgetSendPrompt, isMobile, flashContext }: MessageBubbleProps): React.ReactElement {
  const { user } = useUser();
  const { theme } = useTheme();
  const logo = theme === 'light' ? logoDark : logoLight;
  const avatarUrl = user?.avatar_url as string | undefined;
  const isUser = (message.role as string) === 'user';
  const isAssistant = (message.role as string) === 'assistant';
  const isPendingDelivery = isUser && ((message.isPending as boolean) || (message.steering as boolean));
  const attachments = message.attachments as AttachmentData[] | undefined;
  const hasAttachments = isUser && attachments && attachments.length > 0;
  const widgetSnapshots = message.widgetSnapshots as WidgetChipShape[] | undefined;
  const hasWidgetSnapshots = isUser && widgetSnapshots && widgetSnapshots.length > 0;

  // Edit mode state
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState('');
  const editTextareaRef = useRef<HTMLTextAreaElement>(null);

  // Copy state
  const [copied, setCopied] = useState(false);

  // Feedback state
  const [feedbackRating, setFeedbackRating] = useState<string | null>(null);
  const [showThumbDownModal, setShowThumbDownModal] = useState(false);

  // Load initial feedback on mount
  useEffect(() => {
    if (isAssistant && getFeedbackForMessage) {
      const fb = getFeedbackForMessage(message.id as string);
      if (fb) setFeedbackRating(fb.rating);
    }
  }, [message.id, isAssistant, getFeedbackForMessage]);

  const handleThumbUpClick = async () => {
    if (!onThumbUp) return;
    const prevRating = feedbackRating;
    const newRating = prevRating === 'thumbs_up' ? null : 'thumbs_up';
    setFeedbackRating(newRating);
    const result = await onThumbUp(message.id as string);
    if (result === null) setFeedbackRating(prevRating);
    else if (result) setFeedbackRating(result.rating);
  };

  const handleThumbDownSubmit = async (issueCategories: string[], comment: string | null, consentHumanReview: boolean) => {
    if (!onThumbDown) return;
    const prevRating = feedbackRating;
    setFeedbackRating('thumbs_down');
    setShowThumbDownModal(false);
    const result = await onThumbDown(message.id as string, issueCategories, comment, consentHumanReview);
    if (result === null) setFeedbackRating(prevRating);
  };

  // Action buttons are mounted for all normal messages (reserves layout space) and
  // only made visible after streaming settles. Keeping them mounted prevents a
  // ~32px layout jump on sibling messages when streaming ends.
  const canShowActions = !isSubagentView && !readOnly;
  const showActions = canShowActions && !(message.isStreaming as boolean) && !isLoading;

  const resizeTextarea = () => {
    const el = editTextareaRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = el.scrollHeight + 'px';
  };

  const handleStartEdit = () => {
    setEditContent((message.content as string) || '');
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
    if (trimmed && trimmed !== ((message.content as string) || '').trim()) {
      onEditMessage?.(message.id as string, trimmed);
    }
    setIsEditing(false);
    setEditContent('');
  };

  const handleEditKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmitEdit();
    } else if (e.key === 'Escape') {
      handleCancelEdit();
    }
  };

  const handleCopy = () => {
    // Collect all text content from segments
    const contentSegments = message.contentSegments as ContentSegmentRecord[] | undefined;
    const text = contentSegments
      ?.filter((s) => s.type === 'text')
      .map((s) => s.content)
      .join('') || (message.content as string) || '';
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div
      data-message-id={message.id as string}
      className={`group flex items-start ${isMobile ? 'gap-3' : 'gap-4'} ${isUser ? 'justify-end' : 'justify-start'}`}
    >
      {/* Assistant avatar - shown on the left */}
      {isAssistant && !hideAvatar && (
        <div className={`flex-shrink-0 flex items-center justify-center ${isMobile ? 'w-6 h-6' : 'w-8 h-8'}`}>
          <img src={logo} alt="Assistant" className={isMobile ? 'w-6 h-6' : 'w-8 h-8'} />
        </div>
      )}

      {/* Message content column -- bubble + standalone attachment cards */}
      <div className={`${isUser ? (isEditing && isMobile ? 'w-full' : 'max-w-[80%]') + ' flex flex-col items-end gap-2' : `w-full min-w-0${isMobile ? '' : ' pt-1'}`}`}>

        {/* ===== EDIT MODE (user messages) ===== */}
        {isEditing && isUser ? (
          <div className="w-full flex flex-col gap-2">
            {/* Attachment preview cards -- above the edit textarea */}
            {hasAttachments && (
              <div className="flex gap-3 overflow-x-auto">
                {attachments!.map((att, idx) => (
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
                onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => {
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
            <div className={`flex gap-3 ${isMobile ? 'flex-col' : 'items-center'}`}>
              <div className="flex items-start gap-1.5 flex-1 min-w-0">
                <Info className="h-3.5 w-3.5 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
                <span className="text-xs leading-snug" style={{ color: 'var(--color-text-tertiary)' }}>
                  This will branch from the current thread. Messages after this point will be replaced and cannot be recovered.
                </span>
              </div>
              <div className={`flex gap-2 flex-shrink-0 ${isMobile ? 'justify-end' : ''}`}>
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
            isUser
              ? `${isMobile ? 'px-3 py-2' : 'px-4 py-3'} rounded-tr-none overflow-hidden`
              : `pl-0 pr-0 ${isMobile ? 'pb-2' : 'pb-3'} rounded-tl-none`
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
              {(message.content as string) || ''}
            </TextShimmer>
          ) : (
          <>
          {/* Render content segments in chronological order */}
          {(message.contentSegments as ContentSegmentRecord[] | undefined) && (message.contentSegments as ContentSegmentRecord[]).length > 0 ? (
            <CitationMetadataProvider toolCallProcesses={(message.toolCallProcesses as Record<string, Record<string, unknown>>) || EMPTY_OBJ}>
            <MessageContentSegments
              segments={message.contentSegments as ContentSegmentRecord[]}
              reasoningProcesses={(message.reasoningProcesses as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              toolCallProcesses={(message.toolCallProcesses as Record<string, ToolCallProcessRecord>) || EMPTY_OBJ}
              todoListProcesses={(message.todoListProcesses as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              subagentTasks={(message.subagentTasks as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              planApprovals={(message.planApprovals as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              userQuestions={(message.userQuestions as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              workspaceProposals={(message.workspaceProposals as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              questionProposals={(message.questionProposals as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              pendingToolCallChunks={(message.pendingToolCallChunks as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              isStreaming={message.isStreaming as boolean}
              hasError={message.error as boolean}
              structuredError={message.structuredError as import('@/utils/rateLimitError').StructuredError | undefined}
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
              onApprovePTCAgent={onApprovePTCAgent}
              onRejectPTCAgent={onRejectPTCAgent}
              onApproveSecretaryAction={onApproveSecretaryAction}
              onRejectSecretaryAction={onRejectSecretaryAction}
              ptcAgentProposals={(message.ptcAgentProposals as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              secretaryActionProposals={(message.secretaryActionProposals as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              onWidgetSendPrompt={onWidgetSendPrompt}
              htmlWidgetProcesses={(message.htmlWidgetProcesses as Record<string, Record<string, unknown>>) || EMPTY_OBJ}
              textOnly={true}
              readOnly={readOnly}
              allowFiles={allowFiles}
              flashContext={flashContext}
            />
            </CitationMetadataProvider>
          ) : (
            // Fallback for messages without segments (backward compatibility) - main chat shows text only
            ((message.contentType as string) === 'text' || !(message.contentType as string)) && (
              <TextMessageContent
                content={message.content as string}
                isStreaming={message.isStreaming as boolean}
                hasError={message.error as boolean}
                structuredError={message.structuredError as import('@/utils/rateLimitError').StructuredError | undefined}
                onOpenFile={onOpenFile}
              />
            )
          )}
          </>
          )}

          {/* Streaming indicator -- hidden when dot-loader is already showing for pending chunks */}
          {(message.isStreaming as boolean) && !Object.keys((message.pendingToolCallChunks as Record<string, unknown>) || {}).length && (() => {
            const contentSegments = message.contentSegments as ContentSegmentRecord[] | undefined;
            const hasContent = contentSegments?.some(s => s.content?.trim()) || (message.content as string)?.trim();
            return <LissajousLoading className={`${hasContent ? "mt-2" : "mt-0"} ${isMobile ? 'w-5 h-5' : 'w-6 h-6'} text-neutral-500 dark:text-neutral-400`} />;
          })()}
        </div>

        {/* Attachment preview cards -- standalone below the bubble */}
        {hasAttachments && (
          <div className="flex gap-3 overflow-x-auto">
            {attachments!.map((att, idx) => (
              <AttachmentCard key={idx} attachment={att} />
            ))}
          </div>
        )}

        {/* Widget context deck -- stacked chip cards below the bubble,
            mirroring the chat-input deck visuals. Read-only: chips are
            scoped to the message that attached them. */}
        {hasWidgetSnapshots && (
          <InlineWidgetDeck snapshots={widgetSnapshots!} />
        )}
        </>
        )}

        {/* Message action buttons -- always mounted (reserves space), visibility toggled.
            aria-hidden + inert keep the buttons out of the a11y tree and tab order
            while opacity-0 is hiding them, so screen readers don't announce
            "Copy, Thumbs up, ..." for every streaming message. */}
        {canShowActions && !isEditing && (
          <div
            aria-hidden={!showActions}
            inert={!showActions || undefined}
            className={`flex gap-1 mt-0.5 transition-opacity ${
              showActions
                ? (isMobile ? 'opacity-70' : 'opacity-0 group-hover:opacity-100')
                : 'opacity-0 pointer-events-none'
            } ${
              isUser ? 'justify-end' : 'justify-start'
            }`}
          >
            {/* User message actions */}
            {isUser && onEditMessage && (
              <button
                onClick={handleStartEdit}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title="Edit message"
              >
                <Pencil className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
              </button>
            )}

            {/* Assistant message actions: Copy -> ThumbUp -> ThumbDown -> Regenerate/Retry */}
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
            {isAssistant && !(message.error as boolean) && onThumbUp && (
              <button
                onClick={handleThumbUpClick}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title={feedbackRating === 'thumbs_up' ? 'Remove rating' : 'Good response'}
              >
                <ThumbsUp
                  className="h-3.5 w-3.5"
                  fill={feedbackRating === 'thumbs_up' ? 'currentColor' : 'none'}
                  style={{ color: feedbackRating === 'thumbs_up' ? 'var(--color-gain)' : 'var(--color-text-tertiary)' }}
                />
              </button>
            )}
            {isAssistant && !(message.error as boolean) && onThumbDown && (
              <button
                onClick={() => setShowThumbDownModal(true)}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title={feedbackRating === 'thumbs_down' ? 'Feedback submitted' : 'Report issue'}
              >
                <ThumbsDown
                  className="h-3.5 w-3.5"
                  fill={feedbackRating === 'thumbs_down' ? 'currentColor' : 'none'}
                  style={{ color: feedbackRating === 'thumbs_down' ? 'var(--color-loss)' : 'var(--color-text-tertiary)' }}
                />
              </button>
            )}
            {isAssistant && !(message.error as boolean) && onRegenerate && (
              <button
                onClick={() => onRegenerate(message.id as string)}
                className="p-1 rounded transition-colors hover:bg-[var(--color-bg-elevated)]"
                title="Regenerate response"
              >
                <RefreshCw className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
              </button>
            )}
            {isAssistant && (message.error as boolean) && onRetry && (
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

        {/* ThumbDown feedback modal */}
        {showThumbDownModal && (
          <ThumbDownModal
            isOpen={showThumbDownModal}
            onSubmit={handleThumbDownSubmit}
            onCancel={() => setShowThumbDownModal(false)}
            onReportWithAgent={onReportWithAgent ? (instruction: string) => {
              setShowThumbDownModal(false);
              onReportWithAgent(instruction);
            } : undefined}
          />
        )}
      </div>

      {/* User avatar - shown on the right (hidden during edit on mobile) */}
      {isUser && !hideAvatar && !(isEditing && isMobile) && (
        <div
          className={`flex-shrink-0 rounded-full flex items-center justify-center overflow-hidden ${isMobile ? 'w-6 h-6' : 'w-8 h-8'}`}
          style={{ backgroundColor: 'var(--color-accent-soft)' }}
        >
          {avatarUrl ? (
            <img src={avatarUrl} alt="User" className="w-full h-full object-cover" />
          ) : (
            <User className={isMobile ? 'h-3 w-3' : 'h-4 w-4'} style={{ color: 'var(--color-accent-primary)' }} />
          )}
        </div>
      )}
    </div>
  );
});

// --- MessageContentSegments ---

interface MessageContentSegmentsProps {
  segments: ContentSegmentRecord[];
  reasoningProcesses: Record<string, Record<string, unknown>>;
  toolCallProcesses: Record<string, ToolCallProcessRecord>;
  todoListProcesses: Record<string, Record<string, unknown>>;
  subagentTasks: Record<string, Record<string, unknown>>;
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
  onOpenSubagentTask?: (info: SubagentInfo) => void;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  onOpenDir?: (dirPath: string) => void;
  onToolCallDetailClick?: (proc: ToolCallProcessRecord) => void;
  onApprovePlan?: () => void;
  onRejectPlan?: () => void;
  onPlanDetailClick?: (planData: Record<string, unknown>) => void;
  onAnswerQuestion?: (answer: string, questionId: string, interruptId: string) => void;
  onSkipQuestion?: (questionId: string, interruptId: string) => void;
  onApproveCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onRejectCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onApproveStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onRejectStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onApprovePTCAgent?: (proposalData: Record<string, unknown>, overrides?: { report_back?: boolean }) => void;
  onRejectPTCAgent?: (proposalData: Record<string, unknown>) => void;
  onApproveSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  onRejectSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  ptcAgentProposals?: Record<string, Record<string, unknown>>;
  secretaryActionProposals?: Record<string, Record<string, unknown>>;
  onWidgetSendPrompt?: (text: string) => void;
  htmlWidgetProcesses?: Record<string, Record<string, unknown>>;
  textOnly?: boolean;
  flashContext?: { threadId: string; workspaceId: string } | null;
}

const MIN_LIVE_EXPOSURE_MS = 5000; // minimum time an item stays in the live zone
const MAX_IN_PROGRESS_MS = 15000; // max time a tool call can stay in-progress in live view before archiving
/** Tools that should stay in the live zone for their entire duration (no MAX_IN_PROGRESS_MS cap) */
const ALWAYS_LIVE_TOOLS = new Set(['TaskOutput', 'WebFetch']);
/** Tool calls that are never rendered as visible activity items — they have dedicated UI or are internal */
const HIDDEN_TOOL_CALL_NAMES = new Set(['TodoWrite', 'task', 'Task', 'SubmitPlan', 'AskUserQuestion', 'manage_workspaces', 'ptc_agent', 'agent_output', 'manage_threads', 'ShowWidget']);

/** Render block types for the textOnly activity grouping */
interface ActivityRenderBlock {
  type: 'activity';
  key: string;
  items: Array<Record<string, unknown>>;
}
interface TextRenderBlock {
  type: 'text';
  key: string;
  segment: ContentSegmentRecord;
}
interface CompactArtifactRenderBlock {
  type: 'compact_artifact';
  key: string;
  toolCallId: string;
  proc: ToolCallProcessRecord;
}
interface SubagentTaskRenderBlock {
  type: 'subagent_task';
  key: string;
  segment: ContentSegmentRecord;
}
interface PlanApprovalRenderBlock {
  type: 'plan_approval';
  key: string;
  segment: ContentSegmentRecord;
}
interface UserQuestionRenderBlock {
  type: 'user_question';
  key: string;
  segment: ContentSegmentRecord;
}
interface CreateWorkspaceRenderBlock {
  type: 'create_workspace';
  key: string;
  segment: ContentSegmentRecord;
}
interface StartQuestionRenderBlock {
  type: 'start_question';
  key: string;
  segment: ContentSegmentRecord;
}
interface PTCAgentRenderBlock {
  type: 'ptc_agent';
  key: string;
  segment: ContentSegmentRecord;
}
interface SecretaryActionRenderBlock {
  type: 'delete_workspace' | 'stop_workspace' | 'delete_thread';
  key: string;
  segment: ContentSegmentRecord;
}
interface NotificationRenderBlock {
  type: 'notification';
  key: string;
  segment: ContentSegmentRecord;
}
interface HtmlWidgetRenderBlock {
  type: 'html_widget';
  key: string;
  segment: ContentSegmentRecord;
}

type RenderBlock =
  | ActivityRenderBlock
  | TextRenderBlock
  | CompactArtifactRenderBlock
  | SubagentTaskRenderBlock
  | PlanApprovalRenderBlock
  | UserQuestionRenderBlock
  | CreateWorkspaceRenderBlock
  | StartQuestionRenderBlock
  | PTCAgentRenderBlock
  | SecretaryActionRenderBlock
  | NotificationRenderBlock
  | HtmlWidgetRenderBlock;

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
  const textContent = isSubagentView
    ? normalizeSubagentText(block.segment.content)
    : (block.segment.content ?? '');
  const textEl = (
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

const MessageContentSegments = memo(function MessageContentSegments({ segments, reasoningProcesses, toolCallProcesses, todoListProcesses, subagentTasks, planApprovals = EMPTY_OBJ, userQuestions = EMPTY_OBJ, workspaceProposals = EMPTY_OBJ, questionProposals = EMPTY_OBJ, pendingToolCallChunks = EMPTY_OBJ, isStreaming, hasError, structuredError, isAssistant = false, compactToolCalls = false, isSubagentView = false, readOnly = false, allowFiles = false, onOpenSubagentTask, onOpenFile, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, onApprovePTCAgent, onRejectPTCAgent, onApproveSecretaryAction, onRejectSecretaryAction, ptcAgentProposals = EMPTY_OBJ, secretaryActionProposals = EMPTY_OBJ, onWidgetSendPrompt, htmlWidgetProcesses = EMPTY_OBJ, textOnly = false, flashContext }: MessageContentSegmentsProps): React.ReactElement {
  // Force re-render timer for recently-completed tool calls that need minimum exposure
  const [tick, setTick] = useState(0);
  const expiryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const nextExpiryRef = useRef<number | null>(null);

  // Schedule timer for next expiry — runs after every render since nextExpiryRef
  // is set during render (from memoized renderBlocks or non-textOnly path).
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
  const groupedSegments = useMemo(() => {
    const sorted = [...segments].sort((a, b) => a.order - b.order);
    const groups: ContentSegmentRecord[] = [];
    let currentTextGroup: ContentSegmentRecord | null = null;

    for (const segment of sorted) {
      if (segment.type === 'text') {
        if (currentTextGroup) {
          const prev: ContentSegmentRecord = currentTextGroup;
          currentTextGroup = {
            ...prev,
            content: (prev.content || '') + (segment.content || ''),
            lastOrder: segment.order,
          };
          // Replace the last entry (the current text group) with the updated one
          groups[groups.length - 1] = currentTextGroup;
        } else {
          currentTextGroup = {
            type: 'text',
            content: segment.content,
            order: segment.order,
            lastOrder: segment.order,
          };
          groups.push(currentTextGroup);
        }
      } else {
        currentTextGroup = null;
        groups.push(segment);
      }
    }
    return groups;
  }, [segments]);

  // Reset expiry for this render pass (set by memoized renderBlocks below)
  nextExpiryRef.current = null;

  // Memoize the expensive renderBlocks construction (only meaningful in textOnly mode).
  // Always call useMemo to satisfy rules-of-hooks; short-circuit when not textOnly.
  // tick is included so timer-driven live→completed transitions recompute correctly.
  const { blocks: renderBlocks, nextExpiry } = useMemo(() => {
    if (!textOnly) return { blocks: [] as RenderBlock[], nextExpiry: null };

    const filtered = groupedSegments.filter((s) => {
        if (s.type === 'text' || s.type === 'reasoning') return true;
        if (s.type === 'notification') return true;
        if (s.type === 'subagent_task') return true;
        if (s.type === 'plan_approval') return true;
        if (s.type === 'user_question') return true;
        if (s.type === 'create_workspace') return true;
        if (s.type === 'start_question') return true;
        if (s.type === 'ptc_agent') return true;
        if (s.type === 'delete_workspace') return true;
        if (s.type === 'stop_workspace') return true;
        if (s.type === 'delete_thread') return true;
        if (s.type === 'html_widget') return true;
        if (s.type === 'tool_call') {
          const toolName = toolCallProcesses[s.toolCallId!]?.toolName as string | undefined;
          if (HIDDEN_TOOL_CALL_NAMES.has(toolName || '')) return false;
          return true;
        }
        return false;
      });

      const blocks: RenderBlock[] = [];
      let pendingItems: Array<Record<string, unknown>> = [];
      let activityCounter = 0;
      let computedNextExpiry: number | null = null;

      const now = Date.now();

      const flushActivity = () => {
        if (pendingItems.length > 0) {
          blocks.push({
            type: 'activity',
            key: `activity-${activityCounter++}`,
            items: pendingItems,
          });
          pendingItems = [];
        }
      };

      for (const seg of filtered) {
        if (seg.type === 'reasoning') {
          const proc = reasoningProcesses[seg.reasoningId!];
          if (!proc) continue;
          const rawContent = (proc.content as string) || '';
          const reasoningContent = isSubagentView ? normalizeSubagentText(rawContent) : rawContent;

          if (proc.isReasoning) {
            pendingItems.push({
              type: 'reasoning',
              id: seg.reasoningId,
              reasoningTitle: proc.reasoningTitle || null,
              content: reasoningContent,
              _liveState: 'active',
            });
          } else {
            const completedAt = proc._completedAt as number | undefined;
            const completedAge = completedAt ? now - completedAt : Infinity;

            if (completedAge < MIN_LIVE_EXPOSURE_MS) {
              pendingItems.push({
                type: 'reasoning',
                id: seg.reasoningId,
                reasoningTitle: proc.reasoningTitle || null,
                content: reasoningContent,
                reasoningComplete: proc.reasoningComplete,
                _liveState: 'completing',
              });
              const expiry = completedAt! + MIN_LIVE_EXPOSURE_MS;
              if (computedNextExpiry === null || expiry < computedNextExpiry) {
                computedNextExpiry = expiry;
              }
            } else {
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
          const proc = toolCallProcesses[seg.toolCallId!];
          if (!proc) continue;

          const createdAt = proc._createdAt as number | undefined;
          const age = createdAt ? now - createdAt : Infinity;

          const artifactResult = (proc.toolCallResult as Record<string, unknown> | undefined)?.artifact as Record<string, unknown> | undefined;
          const isArtifactReady = INLINE_ARTIFACT_TOOLS.has(proc.toolName as string) && artifactResult;

          const isAlwaysLive = ALWAYS_LIVE_TOOLS.has(proc.toolName as string);

          if ((proc.isInProgress as boolean) && isStreaming && (isAlwaysLive || age < MAX_IN_PROGRESS_MS)) {
            pendingItems.push({
              type: 'tool_call',
              id: seg.toolCallId,
              toolCallId: seg.toolCallId,
              ...proc,
              _liveState: 'active',
            });
            if (!isAlwaysLive) {
              const expiry = createdAt! + MAX_IN_PROGRESS_MS;
              if (computedNextExpiry === null || expiry < computedNextExpiry) {
                computedNextExpiry = expiry;
              }
            }
          } else if (isArtifactReady) {
            flushActivity();
            blocks.push({
              type: 'compact_artifact',
              key: `compact-${seg.toolCallId}`,
              toolCallId: seg.toolCallId!,
              proc,
            });
          } else if (age < MIN_LIVE_EXPOSURE_MS && !INLINE_ARTIFACT_TOOLS.has(proc.toolName as string)) {
            pendingItems.push({
              type: 'tool_call',
              id: seg.toolCallId,
              toolCallId: seg.toolCallId,
              ...proc,
              _recentlyCompleted: true,
              _liveState: 'completing',
            });
            const expiry = createdAt! + MIN_LIVE_EXPOSURE_MS;
            if (computedNextExpiry === null || expiry < computedNextExpiry) {
              computedNextExpiry = expiry;
            }
          } else {
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
          blocks.push({ type: 'subagent_task', key: `subagent-${seg.subagentId}`, segment: seg });
        } else if (seg.type === 'plan_approval') {
          flushActivity();
          blocks.push({ type: 'plan_approval', key: `plan-${seg.planApprovalId}`, segment: seg });
        } else if (seg.type === 'user_question') {
          flushActivity();
          blocks.push({ type: 'user_question', key: `question-${seg.questionId}`, segment: seg });
        } else if (seg.type === 'create_workspace') {
          flushActivity();
          blocks.push({ type: 'create_workspace', key: `workspace-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'start_question') {
          flushActivity();
          blocks.push({ type: 'start_question', key: `start-question-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'ptc_agent') {
          flushActivity();
          blocks.push({ type: 'ptc_agent', key: `ptc-agent-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'delete_workspace' || seg.type === 'stop_workspace' || seg.type === 'delete_thread') {
          flushActivity();
          blocks.push({ type: seg.type, key: `secretary-${seg.type}-${seg.proposalId}`, segment: seg });
        } else if (seg.type === 'html_widget') {
          flushActivity();
          blocks.push({ type: 'html_widget', key: `widget-${seg.widgetId}`, segment: seg });
        } else if (seg.type === 'notification') {
          flushActivity();
          blocks.push({ type: 'notification', key: `notification-${seg.order}`, segment: seg });
        } else if (seg.type === 'text') {
          flushActivity();
          blocks.push({ type: 'text', key: `text-${seg.order}`, segment: seg });
        }
      }
      // Flush trailing activity items
      flushActivity();

      return { blocks, nextExpiry: computedNextExpiry };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- tick is a semantic dep: forces recomputation when timer fires for live→completed transitions
    }, [groupedSegments, tick, reasoningProcesses, toolCallProcesses, isStreaming, isSubagentView, textOnly]);

  // Apply side effect: schedule timer for next expiry transition
  nextExpiryRef.current = nextExpiry;

  // textOnly mode: use inline ActivityBlock groups
  if (textOnly) {
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
    for (let i = 0; i < renderBlocks.length; i++) {
      const b = renderBlocks[i];
      if (b.type === 'text') lastTextBlockIdx = i;
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
              <NotificationDivider key={block.key} content={notifSeg.content} detail={notifSeg.detail} />
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
            const task = subagentTasks[(block as SubagentTaskRenderBlock).segment.subagentId!];
            if (!task) return null;
            const rawToolCallProcess = toolCallProcesses[(block as SubagentTaskRenderBlock).segment.subagentId!] || undefined;
            const toolCallProcess = rawToolCallProcess ? {
              ...rawToolCallProcess,
              _subagentResult: (task.result as string) || null,
              _subagentStatus: (task.status as string) || null,
            } : undefined;
            return (
              <SubagentTaskMessageContent
                key={block.key}
                subagentId={(block as SubagentTaskRenderBlock).segment.subagentId!}
                description={task.description as string}
                type={task.type as string}
                status={task.status as string}
                action={task.action as 'init' | 'update' | 'resume' | undefined}
                resumeTargetId={task.resumeTargetId as string}
                onOpen={readOnly ? undefined : onOpenSubagentTask}
                onDetailOpen={readOnly ? undefined : (onToolCallDetailClick as any)} // TODO: type properly
                toolCallProcess={toolCallProcess as any} // TODO: type properly — ToolCallProcess not exported
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
                onApprove={onApprovePTCAgent ? (overrides?: { report_back?: boolean }) => onApprovePTCAgent(pad, overrides) : undefined}
                onReject={onRejectPTCAgent ? () => onRejectPTCAgent(pad) : undefined}
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
      </div>
    );
  }

  // Non-textOnly mode (agent panel): render all segments individually
  return (
    <div className="space-y-2">
      {groupedSegments.map((segment, index) => {
        if (segment.type === 'text') {
          const isLastSegment = index === groupedSegments.length - 1;
          return (
            <div key={`text-${segment.order}-${index}`}>
              <TextMessageContent
                content={segment.content ?? ''}
                isStreaming={!!(isStreaming && isLastSegment)}
                hasError={!!hasError}
                structuredError={structuredError}
                onOpenFile={onOpenFile}
              />
            </div>
          );
        } else if (segment.type === 'reasoning') {
          const proc = reasoningProcesses[segment.reasoningId!];
          if (!proc) return null;
          return (
            <ReasoningMessageContent
              key={`reasoning-${segment.reasoningId}`}
              reasoningContent={(proc.content as string) || ''}
              isReasoning={(proc.isReasoning as boolean) || false}
              reasoningComplete={(proc.reasoningComplete as boolean) || false}
              reasoningTitle={(proc.reasoningTitle as string) ?? undefined}
            />
          );
        } else if (segment.type === 'tool_call') {
          const proc = toolCallProcesses[segment.toolCallId!];
          if (!proc || HIDDEN_TOOL_CALL_NAMES.has(proc.toolName as string)) return null;
          return (
            <ToolCallMessageContent
              key={`tool-call-${segment.toolCallId}`}
              toolCallId={segment.toolCallId!}
              toolName={proc.toolName as string}
              toolCall={proc.toolCall as any} // TODO: type properly
              toolCallResult={proc.toolCallResult as any} // TODO: type properly
              isInProgress={(proc.isInProgress as boolean) || false}
              isComplete={(proc.isComplete as boolean) || false}
              isFailed={(proc.isFailed as boolean) || false}
              onOpenFile={onOpenFile}
            />
          );
        } else if (segment.type === 'subagent_task') {
          const task = subagentTasks[segment.subagentId!];
          if (task) {
            return (
              <SubagentTaskMessageContent
                key={`subagent-task-${segment.subagentId}`}
                subagentId={segment.subagentId!}
                description={task.description as string}
                type={task.type as string}
                status={task.status as string}
                action={task.action as 'init' | 'update' | 'resume' | undefined}
                resumeTargetId={task.resumeTargetId as string}
                onOpen={onOpenSubagentTask}
              />
            );
          }
          return null;
        } else if (segment.type === 'plan_approval') {
          const pd = planApprovals[segment.planApprovalId!];
          if (pd) {
            return (
              <PlanApprovalCard
                key={`plan-${segment.planApprovalId}`}
                planData={pd as any} // TODO: type properly — PlanData not exported
                onApprove={onApprovePlan}
                onReject={onRejectPlan}
                onDetailClick={() => onPlanDetailClick?.(pd)}
              />
            );
          }
          return null;
        } else if (segment.type === 'user_question') {
          const qd = userQuestions[segment.questionId!];
          if (qd) {
            return (
              <UserQuestionCard
                key={`question-${segment.questionId}`}
                questionData={qd as any} // TODO: type properly — QuestionData not exported
                onAnswer={(answer: string) => onAnswerQuestion!(answer, segment.questionId!, qd.interruptId as string)}
                onSkip={() => onSkipQuestion!(segment.questionId!, qd.interruptId as string)}
              />
            );
          }
          return null;
        } else if (segment.type === 'create_workspace') {
          const wd = workspaceProposals[segment.proposalId!];
          if (wd) {
            return (
              <CreateWorkspaceCard
                key={`workspace-${segment.proposalId}`}
                proposalData={wd as any} // TODO: type properly — ProposalData not exported
                onApprove={onApproveCreateWorkspace ? () => onApproveCreateWorkspace(wd) : undefined}
                onReject={onRejectCreateWorkspace ? () => onRejectCreateWorkspace(wd) : undefined}
              />
            );
          }
          return null;
        } else if (segment.type === 'start_question') {
          const sqd = questionProposals[segment.proposalId!];
          if (sqd) {
            return (
              <StartQuestionCard
                key={`start-question-${segment.proposalId}`}
                proposalData={sqd as any} // TODO: type properly — ProposalData not exported
                onApprove={onApproveStartQuestion ? () => onApproveStartQuestion(sqd) : undefined}
                onReject={onRejectStartQuestion ? () => onRejectStartQuestion(sqd) : undefined}
              />
            );
          }
          return null;
        } else if (segment.type === 'ptc_agent') {
          const pad = ptcAgentProposals[segment.proposalId!];
          if (pad) {
            return (
              <PTCAgentCard
                key={`ptc-agent-${segment.proposalId}`}
                proposalData={pad as any}
                onApprove={onApprovePTCAgent ? (overrides?: { report_back?: boolean }) => onApprovePTCAgent(pad, overrides) : undefined}
                onReject={onRejectPTCAgent ? () => onRejectPTCAgent(pad) : undefined}
                flashContext={flashContext}
              />
            );
          }
          return null;
        } else if (segment.type === 'delete_workspace' || segment.type === 'stop_workspace' || segment.type === 'delete_thread') {
          const sad = secretaryActionProposals[segment.proposalId!];
          if (sad) {
            return (
              <SecretaryConfirmCard
                key={`secretary-${segment.type}-${segment.proposalId}`}
                proposalData={sad as any}
                onApprove={onApproveSecretaryAction ? () => onApproveSecretaryAction(sad) : undefined}
                onReject={onRejectSecretaryAction ? () => onRejectSecretaryAction(sad) : undefined}
              />
            );
          }
          return null;
        } else if (segment.type === 'notification') {
          return (
            <NotificationDivider key={`notification-${segment.order}-${index}`} content={segment.content} detail={segment.detail} />
          );
        }
        return null;
      })}
    </div>
  );
});

export default MessageList;
export { MessageContentSegments };
