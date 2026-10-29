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
import type { FeedbackResult, SubagentInfo } from './types';
import type { PTCAgentProposalState, ToolApprovalPosition } from '@/types/chat';
import type { OpenFileHandler } from '../../utils/fileLocation';
import type { ChartTabSpec } from '../filePanel/types';
import type { EditDraftStore } from '../../hooks/utils/editDrafts';

export interface MessageActions {
  onOpenSubagentTask?: (info: SubagentInfo) => void;
  /** Opens the thread that handed a turn to this one, where the thread's own
   *  back button goes. Only the host knows how that button gets there. */
  onOpenDispatcher?: (threadId: string) => void;
  onOpenFile?: OpenFileHandler;
  /** Brings a turn's deliverables deck into view as it unfolds. The deck owns
   *  no scroll container and must not grab one: the host's scroll controller
   *  re-asserts its own position on every content growth, which a fanning deck
   *  produces on every frame. Its absence just means the deck does not scroll. */
  onRevealFiles?: (messageId: string) => void;
  /** Brings a message into view and focus, for a link to it further on: a
   *  hand-off's report. Through the host's scroll controller for the reason
   *  above. False when the message is not drawn, so the link can go elsewhere. */
  onRevealMessage?: (messageId: string) => boolean;
  /** Saves a workspace file to disk. Separate from `onOpenFile` because a host
   *  may grant reading without granting download: a copy-link share does. Its
   *  absence is what hides the deliverable card's Download item. */
  onDownloadFile?: (path: string, workspaceId?: string) => void;
  /** The key a card's save is tracked under. The host builds it because only
   *  the host knows which workspace a card without its own names, and the
   *  click and the card's busy state must agree on it. */
  downloadKeyFor?: (path: string, workspaceId?: string) => string;
  onOpenSources?: (messageId: string) => void;
  /** Opens a live chart for a symbol beside the chat; absent where there is no panel to land in. */
  onOpenChart?: (spec: ChartTabSpec) => void;
  /** Opens a tool call's result by id; the host reads the live record. */
  onToolCallDetailClick?: (toolCallId: string) => void;
  onAnswerQuestion?: (answer: string, questionId: string, interruptId: string) => void;
  onSkipQuestion?: (questionId: string, interruptId: string) => void;
  onApproveCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onRejectCreateWorkspace?: (proposalData: Record<string, unknown>) => void;
  onApproveStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onRejectStartQuestion?: (proposalData: Record<string, unknown>) => void;
  onApprovePTCAgent?: (proposal: PTCAgentProposalState, overrides: { report_back?: boolean } | undefined, proposalId: string, interruptId: string) => void;
  onRejectPTCAgent?: (proposal: PTCAgentProposalState, proposalId: string, interruptId: string) => void;
  onApproveSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  onRejectSecretaryAction?: (proposalData: Record<string, unknown>) => void;
  onResumeCreditPause?: (pauseId: string, interruptId: string) => void;
  /** `position` is the card's slot among its interrupt's action requests. The
   *  resume must answer all of them in order, and only the card knows where it
   *  sits, so it says rather than the hook looking it up. */
  /** `attemptId` rides along for a call the order gate keyed: the resume names
   *  it rather than trusting the slot, so a batch cannot misassign a verdict. */
  onApproveToolCall?: (approvalId: string, interruptId: string, position: ToolApprovalPosition, attemptId?: string) => void;
  onRejectToolCall?: (approvalId: string, interruptId: string, position: ToolApprovalPosition, message?: string, attemptId?: string) => void;
  onEditMessage?: (messageId: string, content: string) => void;
  /** The session's open edit boxes, which outlive the bubbles a reload
   *  remounts. Without it a bubble keeps its own while it lives. */
  editDrafts?: EditDraftStore;
  onRegenerate?: (messageId: string) => void;
  onRetry?: () => void;
  /** Feedback is addressed by BACKEND TURN, not by bubble — a steering
   *  continuation rates the turn it continues. MessageList supplies the index. */
  onThumbUp?: (turnIndex: number) => Promise<FeedbackResult | null>;
  onThumbDown?: (turnIndex: number, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => Promise<FeedbackResult | null>;
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
    <MessageActionsContext value={actions}>
      {children}
    </MessageActionsContext>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useMessageActions(): MessageActions {
  return React.useContext(MessageActionsContext);
}
