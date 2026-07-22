import React from 'react';
import { Bot } from 'lucide-react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { DispatchStatusProvider } from '../hooks/usePTCDispatchStatus';
import { NotificationDivider } from './messageList/NotificationDivider';
import { MessageBubble } from './messageList/MessageBubble';
import type { FeedbackResult, MessageRecord, SubagentInfo, ToolCallProcessRecord } from './messageList/types';

/**
 * An assistant bubble that settled with nothing to show. Some turns legitimately
 * finalize empty in STATE — a HITL resume whose content landed on another bubble,
 * or a history turn whose only event was a re-raised interrupt deduped by
 * interrupt_id — and they must stay in state because edit/regenerate map UI
 * position → backend turn_index by counting assistant bubbles. But painting them
 * shows an orphan avatar + action row, so the list skips rendering them.
 * Anything renderable keeps the bubble: a streaming indicator, text, segments,
 * the Sources pill, the Stopped chip, or an error.
 *
 * INVARIANT: everything an assistant bubble can render must surface through
 * content / contentSegments / provenanceRecords / error / stopped / isStreaming.
 * A future assistant field that renders OUTSIDE those (e.g. assistant-side
 * attachments) must be added to this guard or its bubbles will be hidden.
 */
// eslint-disable-next-line react-refresh/only-export-components
export function isOrphanAssistantMessage(message: MessageRecord): boolean {
  if (message.role !== 'assistant') return false;
  if (message.isStreaming) return false;
  const segments = message.contentSegments as unknown[] | undefined;
  if (segments && segments.length > 0) return false;
  if (message.content) return false;
  const provenance = message.provenanceRecords as Record<string, unknown> | undefined;
  if (provenance && Object.keys(provenance).length > 0) return false;
  return !message.error && !message.stopped;
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
  flashContext?: { threadId: string; workspaceId: string } | null;
}

function MessageList({ messages, isLoading, isLoadingHistory, hideAvatar, compactToolCalls, isSubagentView, readOnly, allowFiles, onOpenSubagentTask, onOpenFile, onOpenSources, onOpenDir, onToolCallDetailClick, onApprovePlan, onRejectPlan, onPlanDetailClick, onAnswerQuestion, onSkipQuestion, onApproveCreateWorkspace, onRejectCreateWorkspace, onApproveStartQuestion, onRejectStartQuestion, onApprovePTCAgent, onRejectPTCAgent, onApproveSecretaryAction, onRejectSecretaryAction, onEditMessage, onRegenerate, onRetry, onThumbUp, onThumbDown, getFeedbackForMessage, onReportWithAgent, onWidgetSendPrompt, flashContext }: MessageListProps): React.ReactElement | null {
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

  // Render message list. One DispatchStatusProvider for the whole list so every
  // PTCAgentCard in the turn shares a single batched dispatch-liveness query +
  // timer instead of each card polling /status on its own.
  return (
    <DispatchStatusProvider>
    <div className={isMobile ? 'space-y-4' : 'space-y-6'}>
      {messages.map((message) =>
        isOrphanAssistantMessage(message) ? null :
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
            onOpenSources={onOpenSources}
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
    </DispatchStatusProvider>
  );
}

export default MessageList;
export { MessageContentSegments } from './messageList/MessageContentSegments';
// eslint-disable-next-line react-refresh/only-export-components
export { normalizeSubagentText } from './messageList/normalizeSubagentText';
