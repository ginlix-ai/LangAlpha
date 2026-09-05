import React from 'react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { DispatchStatusProvider } from '../hooks/usePTCDispatchStatus';
import { NotificationDivider } from './messageList/NotificationDivider';
import { MessageBubble } from './messageList/MessageBubble';
import { computeTurnTails, projectTurns, visibleProjection } from './messageList/turnProjection';
import type { FeedbackResult, MessageRecord } from './messageList/types';

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
  /** Feedback keyed by BACKEND TURN — the projection below maps each bubble to
   *  its turn, so a steering continuation shows the continued turn's rating. */
  feedbackByTurn?: Record<number, FeedbackResult>;
  flashContext?: { threadId: string; workspaceId: string } | null;
}

function MessageList({ messages, isLoading, isLoadingHistory, hideAvatar, compactToolCalls, isSubagentView, readOnly, allowFiles, feedbackByTurn, flashContext }: MessageListProps): React.ReactElement | null {
  const isMobile = useIsMobile();

  // ONE raw projection pass carries the turn semantics (edit/regenerate/
  // feedback all address backend turns); orphan filtering and the regenerate
  // tail then run over the VISIBLE list, so a hidden bubble never steals an
  // affordance from a painted one.
  const visible = React.useMemo(() => visibleProjection(projectTurns(messages)), [messages]);
  const turnTails = React.useMemo(() => computeTurnTails(visible), [visible]);

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
    <div className={`font-content ${isMobile ? 'space-y-4' : 'space-y-6'}`}>
      {visible.map(({ message, turnIndex }, i) =>
        (message.role as string) === 'notification' ? (
          <NotificationDivider key={message.id as string} message={message} />
        ) : (
          <MessageBubble
            key={message.id as string}
            message={message}
            turnIndex={turnIndex}
            isTurnTail={turnTails[i]}
            feedback={feedbackByTurn?.[turnIndex] ?? null}
            isLoading={isLoading}
            hideAvatar={isSubagentView || hideAvatar}
            compactToolCalls={compactToolCalls}
            isSubagentView={isSubagentView}
            readOnly={readOnly}
            allowFiles={allowFiles}
            isMobile={isMobile}
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
// eslint-disable-next-line react-refresh/only-export-components
export { isOrphanAssistantMessage } from './messageList/messagePredicates';
