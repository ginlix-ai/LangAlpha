import React, { useState, useEffect, useRef, useMemo, memo } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, Copy, FileSearch, Info, Pencil, RefreshCw, RotateCcw, StopCircle, ThumbsDown, ThumbsUp, User } from 'lucide-react';
import ThumbDownModal from '../ThumbDownModal';
import logoLight from '../../../../assets/img/logo.svg';
import logoDark from '../../../../assets/img/logo-dark.svg';
import { useTheme } from '../../../../contexts/ThemeContext';
import LissajousLoading from '@/components/ui/lissajous-loading';
import { useUser } from '@/hooks/useUser';
import { CitationMetadataProvider } from '../CitationMetadataContext';
import TextMessageContent from '../TextMessageContent';
import { countDedupedSources, type ProvenanceRecord } from '@/types/chat';
import { TextShimmer } from '@/components/ui/text-shimmer';
import type { SelectionPreviewShape } from '../SelectionContextPreview';
import { AttachmentCard, InlineSelectionCards, InlineWidgetDeck } from './attachments';
import type { AttachmentData, WidgetChipShape } from './attachments';
import { MessageContentSegments } from './MessageContentSegments';
import { EMPTY_OBJ } from './types';
import type { ContentSegmentRecord, FeedbackResult, MessageRecord, SubagentInfo, ToolCallProcessRecord } from './types';

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
  onOpenSources?: (messageId: string) => void;
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
  onApprovePTCAgent?: (proposalData: Record<string, unknown>, overrides: { report_back?: boolean } | undefined, proposalId: string, interruptId: string) => void;
  onRejectPTCAgent?: (proposalData: Record<string, unknown>, proposalId: string, interruptId: string) => void;
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
 * Wrapped with React.memo — safe because updateMessage() in messageHelpers.ts
 * returns the same object reference for unchanged messages.
 */
export const MessageBubble = memo(function MessageBubble({ message, isLoading, hideAvatar, compactToolCalls, isSubagentView, readOnly, allowFiles, onOpenSubagentTask, onOpenFile, onOpenSources, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, onApprovePTCAgent, onRejectPTCAgent, onApproveSecretaryAction, onRejectSecretaryAction, onEditMessage, onRegenerate, onRetry, onThumbUp, onThumbDown, getFeedbackForMessage, onReportWithAgent, onWidgetSendPrompt, isMobile, flashContext }: MessageBubbleProps): React.ReactElement {
  const { t } = useTranslation();
  const { user } = useUser();
  const { theme } = useTheme();
  const logo = theme === 'light' ? logoDark : logoLight;
  const avatarUrl = user?.avatar_url as string | undefined;
  const isUser = (message.role as string) === 'user';
  const isAssistant = (message.role as string) === 'assistant';
  const isPendingDelivery = isUser && ((message.isPending as boolean) || (message.steering as boolean) || (message.queued as boolean));
  const attachments = message.attachments as AttachmentData[] | undefined;
  const hasAttachments = isUser && attachments && attachments.length > 0;
  const widgetSnapshots = message.widgetSnapshots as WidgetChipShape[] | undefined;
  const hasWidgetSnapshots = isUser && widgetSnapshots && widgetSnapshots.length > 0;
  const chartSelections = message.chartSelections as SelectionPreviewShape[] | undefined;
  const hasChartSelections = isUser && chartSelections && chartSelections.length > 0;

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

  // Provenance count for the Sources pill, deduped by (source_type, identifier)
  // — the same URL fetched twice in one turn counts once. The pill lives in its
  // own always-visible row (not the hover-gated footer), so it shows mid-stream.
  // This counts distinct sources (every result URL), so it intentionally runs
  // ahead of the panel's visible row count: the panel groups a whole web search
  // into one deck, but the pill still reports how many pages were actually read.
  const sourceCount = useMemo(() => {
    if (!isAssistant || isSubagentView) return 0;
    return countDedupedSources(message.provenanceRecords as Record<string, ProvenanceRecord> | undefined);
  }, [message.provenanceRecords, isAssistant, isSubagentView]);

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

        {/* Sources pill -- always-visible row (not the hover-gated footer below,
            which is hidden during streaming). Surfaces the turn's tracked data
            provenance; clicking opens the Sources tab in the right panel. */}
        {isAssistant && !isSubagentView && sourceCount > 0 && (
          <div className="flex justify-start mt-1">
            <button
              type="button"
              onClick={() => onOpenSources?.(message.id as string)}
              className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium transition-colors"
              style={{
                backgroundColor: 'var(--color-bg-elevated)',
                color: 'var(--color-text-secondary)',
              }}
              title={t('chat.sources.title')}
            >
              <FileSearch className="h-3.5 w-3.5" />
              {t('chat.sources.pill', { count: sourceCount })}
            </button>
          </div>
        )}

        {/* Per-message "⏹ Stopped" chip — the turn was hard-stopped by the
            user (live finalize or replay of a stopped turn). */}
        {isAssistant && (message.stopped as boolean) && (
          <div
            className="inline-flex items-center gap-1 self-start px-2 py-0.5 rounded text-xs"
            style={{ backgroundColor: 'var(--color-loss-soft)', color: 'var(--color-loss)' }}
          >
            <StopCircle className="h-3 w-3 flex-shrink-0" />
            <span>{t('chat.stoppedChip')}</span>
          </div>
        )}

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

        {/* Chart selection cards -- the regions / price levels the user picked
            on the chart and attached to this send. Read-only summary. */}
        {hasChartSelections && (
          <InlineSelectionCards selections={chartSelections!} />
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
