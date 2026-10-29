import { useMemo } from 'react';
import type { RefObject } from 'react';
import { useStableHandler } from '@/hooks/useStableHandler';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import type { MessageActions } from '../messageList/MessageActionsContext';
import type { EditDraftStore } from '../../hooks/utils/editDrafts';
import type { ModelOptions } from './types';

type Action<K extends keyof MessageActions> = NonNullable<MessageActions[K]>;

/** ChatView's raw handlers, before their identities are pinned. */
export interface MessageActionSources {
  onOpenFile: Action<'onOpenFile'>;
  onDownloadFile: Action<'onDownloadFile'>;
  downloadKeyFor: Action<'downloadKeyFor'>;
  onRevealFiles: Action<'onRevealFiles'>;
  onRevealMessage: Action<'onRevealMessage'>;
  onOpenSources: Action<'onOpenSources'>;
  onToolCallDetailClick: Action<'onToolCallDetailClick'>;
  onOpenChart: Action<'onOpenChart'>;
  onOpenSubagentTask: Action<'onOpenSubagentTask'>;
  onOpenDispatcher: Action<'onOpenDispatcher'>;
  onAnswerQuestion: Action<'onAnswerQuestion'>;
  onSkipQuestion: Action<'onSkipQuestion'>;
  onApproveCreateWorkspace: Action<'onApproveCreateWorkspace'>;
  onRejectCreateWorkspace: Action<'onRejectCreateWorkspace'>;
  onApproveStartQuestion: Action<'onApproveStartQuestion'>;
  onRejectStartQuestion: Action<'onRejectStartQuestion'>;
  onApprovePTCAgent: Action<'onApprovePTCAgent'>;
  onRejectPTCAgent: Action<'onRejectPTCAgent'>;
  onApproveSecretaryAction: Action<'onApproveSecretaryAction'>;
  onRejectSecretaryAction: Action<'onRejectSecretaryAction'>;
  onResumeCreditPause: Action<'onResumeCreditPause'>;
  onApproveToolCall: Action<'onApproveToolCall'>;
  onRejectToolCall: Action<'onRejectToolCall'>;
  onThumbUp: Action<'onThumbUp'>;
  onThumbDown: Action<'onThumbDown'>;
  /** Already identity-stable in ChatView; reused here as `onWidgetSendPrompt`. */
  onWidgetSendPrompt: Action<'onWidgetSendPrompt'>;
  /** These three read the composer's current model options at call time. */
  onEditMessage: (messageId: string, content: string, modelOptions?: ModelOptions) => unknown;
  onRegenerate: (messageId: string, modelOptions?: ModelOptions) => unknown;
  onRetry: (modelOptions?: ModelOptions) => unknown;
  /** Created once by the session, so already identity-stable. */
  editDrafts: EditDraftStore;
  onSendMessage: (text: string) => unknown;
  chatInputRef: RefObject<ChatInputHandle | null>;
}

/**
 * Stable handler identities for the memoized message tree. MessageBubble is
 * memo'd, but the handlers ChatView hands in are recreated upstream on every
 * streamed chunk (useChatMessages/useRightPanel re-render per chunk); passing
 * them straight through would re-render every settled bubble on every chunk.
 * useStableHandler pins each identity while always invoking the freshest
 * closure, which is exactly what edit/regenerate need: their turn-index math
 * must read the current messages array, never a memoized snapshot.
 */
export function useMessageActionBundles(src: MessageActionSources): {
  messageActions: MessageActions;
  subagentMessageActions: MessageActions;
} {
  const stableOpenFile = useStableHandler(src.onOpenFile);
  const stableDownloadFile = useStableHandler(src.onDownloadFile);
  const stableDownloadKeyFor = useStableHandler(src.downloadKeyFor);
  const stableRevealFiles = useStableHandler(src.onRevealFiles);
  const stableRevealMessage = useStableHandler(src.onRevealMessage);
  const stableOpenSources = useStableHandler(src.onOpenSources);
  const stableToolCallDetail = useStableHandler(src.onToolCallDetailClick);
  const stableOpenChart = useStableHandler(src.onOpenChart);
  const stableOpenSubagentTask = useStableHandler(src.onOpenSubagentTask);
  const stableOpenDispatcher = useStableHandler(src.onOpenDispatcher);
  const stableAnswerQuestion = useStableHandler(src.onAnswerQuestion);
  const stableSkipQuestion = useStableHandler(src.onSkipQuestion);
  const stableApproveCreateWorkspace = useStableHandler(src.onApproveCreateWorkspace);
  const stableRejectCreateWorkspace = useStableHandler(src.onRejectCreateWorkspace);
  const stableApproveStartQuestion = useStableHandler(src.onApproveStartQuestion);
  const stableRejectStartQuestion = useStableHandler(src.onRejectStartQuestion);
  const stableApprovePTCAgent = useStableHandler(src.onApprovePTCAgent);
  const stableRejectPTCAgent = useStableHandler(src.onRejectPTCAgent);
  const stableApproveSecretaryAction = useStableHandler(src.onApproveSecretaryAction);
  const stableRejectSecretaryAction = useStableHandler(src.onRejectSecretaryAction);
  const stableResumeCreditPause = useStableHandler(src.onResumeCreditPause);
  const stableApproveToolCall = useStableHandler(src.onApproveToolCall);
  const stableRejectToolCall = useStableHandler(src.onRejectToolCall);
  const stableEditMessage = useStableHandler((id: string, content: string) =>
    src.onEditMessage(id, content, src.chatInputRef.current?.getModelOptions?.()));
  const stableRegenerate = useStableHandler((id: string) =>
    src.onRegenerate(id, src.chatInputRef.current?.getModelOptions?.()));
  const stableRetry = useStableHandler(() => src.onRetry(src.chatInputRef.current?.getModelOptions?.()));
  const editDrafts = src.editDrafts;
  // Feedback handlers close over the stored ratings, so their raw identity
  // churns on every load/submit; wrap them or a single thumbs click would
  // re-render the whole transcript through the context value.
  const stableThumbUp = useStableHandler(src.onThumbUp);
  const stableThumbDown = useStableHandler(src.onThumbDown);
  const stableSendMessage = src.onWidgetSendPrompt;

  // The main transcript's action surface. Every member is identity-stable, so
  // this object is built once and never re-renders the memoized message tree.
  const messageActions = useMemo<MessageActions>(() => ({
    onOpenFile: stableOpenFile,
    onDownloadFile: stableDownloadFile,
    downloadKeyFor: stableDownloadKeyFor,
    onRevealFiles: stableRevealFiles,
    onRevealMessage: stableRevealMessage,
    onOpenSources: stableOpenSources,
    onToolCallDetailClick: stableToolCallDetail,
    onOpenChart: stableOpenChart,
    onOpenSubagentTask: stableOpenSubagentTask,
    onOpenDispatcher: stableOpenDispatcher,
    onAnswerQuestion: stableAnswerQuestion,
    onSkipQuestion: stableSkipQuestion,
    onApproveCreateWorkspace: stableApproveCreateWorkspace,
    onRejectCreateWorkspace: stableRejectCreateWorkspace,
    onApproveStartQuestion: stableApproveStartQuestion,
    onRejectStartQuestion: stableRejectStartQuestion,
    onApprovePTCAgent: stableApprovePTCAgent,
    onRejectPTCAgent: stableRejectPTCAgent,
    onApproveSecretaryAction: stableApproveSecretaryAction,
    onRejectSecretaryAction: stableRejectSecretaryAction,
    onResumeCreditPause: stableResumeCreditPause,
    onApproveToolCall: stableApproveToolCall,
    onRejectToolCall: stableRejectToolCall,
    onEditMessage: stableEditMessage,
    onRegenerate: stableRegenerate,
    onRetry: stableRetry,
    editDrafts,
    onThumbUp: stableThumbUp,
    onThumbDown: stableThumbDown,
    onWidgetSendPrompt: stableSendMessage,
  }), [
    stableOpenFile, stableDownloadFile, stableDownloadKeyFor, stableRevealFiles, stableRevealMessage, stableOpenSources, stableToolCallDetail,
    stableOpenChart, stableOpenSubagentTask, stableOpenDispatcher, stableAnswerQuestion, stableSkipQuestion, stableApproveCreateWorkspace,
    stableRejectCreateWorkspace, stableApproveStartQuestion, stableRejectStartQuestion,
    stableApprovePTCAgent, stableRejectPTCAgent, stableApproveSecretaryAction,
    stableRejectSecretaryAction, stableResumeCreditPause, stableApproveToolCall, stableRejectToolCall,
    stableEditMessage, stableRegenerate, stableRetry, editDrafts,
    stableThumbUp, stableThumbDown, stableSendMessage,
  ]);

  // The subagent transcript is a DIFFERENT surface: its cards belong to a task,
  // not to the main thread's turn, so the main thread's approve/reject/edit
  // handlers must not be reachable from it. Navigation only.
  // No onRevealFiles: it moves the main transcript, and the settle-aware
  // observer that follows an unfolding deck is only attached there.
  const subagentMessageActions = useMemo<MessageActions>(() => ({
    onOpenFile: stableOpenFile,
    onDownloadFile: stableDownloadFile,
    downloadKeyFor: stableDownloadKeyFor,
    onToolCallDetailClick: stableToolCallDetail,
    onOpenChart: stableOpenChart,
  }), [stableOpenFile, stableDownloadFile, stableDownloadKeyFor, stableToolCallDetail, stableOpenChart]);

  return { messageActions, subagentMessageActions };
}
