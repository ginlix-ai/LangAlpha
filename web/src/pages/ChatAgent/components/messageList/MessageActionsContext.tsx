/**
 * The transcript's action surface, provided once per MessageList host and read
 * by the leaf that actually fires it. Before this, every handler travelled
 * three identical pass-through prop lists (MessageList → MessageBubble →
 * MessageContentSegments) purely to reach a card five levels down.
 *
 * The context value MUST be identity-stable across streamed chunks — each host
 * memoizes one object built from `useStableHandler` wrappers — because
 * MessageBubble and MessageContentSegments are `memo`'d and a fresh value here
 * re-renders every settled bubble on every chunk.
 *
 * Every host provides EXPLICITLY (ChatView, its subagent transcript,
 * MarketChatPanel, and SharedChat's read-only adapter). The empty default is a
 * fallback for isolated renders in tests, not a supported host configuration.
 */
import React from 'react';
import type { FeedbackResult, SubagentInfo, ToolCallProcessRecord } from './types';

export interface MessageActions {
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
  onResumeCreditPause?: (pauseId: string, interruptId: string) => void;
  onEditMessage?: (messageId: string, content: string) => void;
  onRegenerate?: (messageId: string) => void;
  onRetry?: () => void;
  /** Feedback is addressed by BACKEND TURN, not by bubble — a steering
   *  continuation rates the turn it continues. MessageList supplies the index. */
  onThumbUp?: (turnIndex: number) => Promise<FeedbackResult | null>;
  onThumbDown?: (turnIndex: number, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => Promise<FeedbackResult | null>;
  onReportWithAgent?: (instruction: string) => void;
  onWidgetSendPrompt?: (text: string) => void;
}

const EMPTY_ACTIONS: MessageActions = Object.freeze({});

const MessageActionsContext = React.createContext<MessageActions>(EMPTY_ACTIONS);

/**
 * A transcript with navigation only — no edit/regenerate/feedback, no HITL
 * approvals. Named so a read-only host declares the contract instead of
 * relying on absent props to silently disable everything.
 */
export const READ_ONLY_MESSAGE_ACTIONS: MessageActions = Object.freeze({});

export function MessageActionsProvider({
  actions,
  children,
}: {
  actions: MessageActions;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <MessageActionsContext.Provider value={actions}>
      {children}
    </MessageActionsContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useMessageActions(): MessageActions {
  return React.useContext(MessageActionsContext);
}
