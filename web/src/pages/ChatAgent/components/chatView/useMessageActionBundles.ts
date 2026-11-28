import { useMemo } from 'react';
import type { RefObject } from 'react';
import { useStableActions } from '@/hooks/useStableActions';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import type { MessageActions } from '../messageList/MessageActionsContext';
import type { EditDraftStore } from '../../hooks/utils/editDrafts';
import type { WidgetPrompts } from '../../utils/widgetPrompt';
import type { ModelOptions } from './types';
import type { ArtifactCommentOpeners, ArtifactCommentRef } from '../artifactComments/artifactCommentReply';

type Action<K extends keyof MessageActions> = NonNullable<MessageActions[K]>;

/**
 * A host's raw handlers, before their identities are pinned. One a host
 * leaves out is absent from its bundle, so the transcript offers nothing for
 * it: the market panel opens no files and no other thread.
 */
export interface MessageActionSources {
  reportThreadId?: string;
  feedbackLoading?: boolean;
  onOpenThread?: Action<'onOpenThread'>;
  onOpenFile?: Action<'onOpenFile'>;
  onDownloadFile?: Action<'onDownloadFile'>;
  onPublishFile: Action<'onPublishFile'>;
  downloadKeyFor?: Action<'downloadKeyFor'>;
  onRevealFiles?: Action<'onRevealFiles'>;
  onRevealMessage: Action<'onRevealMessage'>;
  onOpenSources?: Action<'onOpenSources'>;
  onToolCallDetailClick: Action<'onToolCallDetailClick'>;
  onOpenChart?: Action<'onOpenChart'>;
  /** One opener for both kinds, a file's in the panel and a chart's in its
   *  tab, or openers by kind that are already identity-stable. */
  onOpenArtifactComment: ((comment: ArtifactCommentRef) => void) | ArtifactCommentOpeners | undefined;
  onOpenSubagentTask: Action<'onOpenSubagentTask'>;
  onOpenDispatcher?: Action<'onOpenDispatcher'>;
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
  onThumbDownFollowUp: Action<'onThumbDownFollowUp'>;
  /** Already identity-stable (`useWidgetPromptActions`). */
  widgetPrompts: WidgetPrompts;
  /** These three read the composer's current model options at call time. */
  onEditMessage: (messageId: string, content: string, modelOptions?: ModelOptions) => unknown;
  onRegenerate: (messageId: string, modelOptions?: ModelOptions) => unknown;
  onRetry: (modelOptions?: ModelOptions) => unknown;
  onForkFromTurn?: Action<'onForkFromTurn'>;
  /** Send now for a steer of the main transcript, by its request key. */
  onSendSteerNow?: Action<'onSendNow'>;
  /** Send now for an instruction to the task the subagent transcript shows,
   *  by its input id. */
  onSendTaskMessageNow?: Action<'onSendNow'>;
  /** Created once by the session, so already identity-stable. */
  editDrafts: EditDraftStore;
  chatInputRef: RefObject<ChatInputHandle | null>;
}

/**
 * Stable handler identities for the memoized message tree. MessageBubble is
 * memo'd, but the handlers a host hands in are recreated upstream on every
 * streamed chunk (useChatMessages/useRightPanel re-render per chunk); passing
 * them straight through would re-render every settled bubble on every chunk.
 * Each identity is pinned while it always invokes the freshest closure, which
 * is exactly what edit/regenerate need: their turn-index math must read the
 * current messages array, never a memoized snapshot. Feedback handlers close
 * over the stored ratings, so their raw identity churns on every load/submit.
 */
export function useMessageActionBundles(src: MessageActionSources): {
  messageActions: MessageActions;
  subagentMessageActions: MessageActions;
} {
  const {
    reportThreadId, feedbackLoading, editDrafts, widgetPrompts, chatInputRef,
    onOpenArtifactComment, onEditMessage, onRegenerate, onRetry,
    onSendSteerNow, onSendTaskMessageNow, ...handlers
  } = src;
  const modelOptions = () => chatInputRef.current?.getModelOptions?.();
  const stable = useStableActions({
    ...handlers,
    onOpenArtifactComment: typeof onOpenArtifactComment === 'function' ? onOpenArtifactComment : undefined,
    onEditMessage: (id: string, content: string) => { onEditMessage(id, content, modelOptions()); },
    onRegenerate: (id: string) => { onRegenerate(id, modelOptions()); },
    onRetry: () => { onRetry(modelOptions()); },
    onSendNow: onSendSteerNow,
  });
  const task = useStableActions({ onSendNow: onSendTaskMessageNow });
  const openComment = stable.onOpenArtifactComment;
  const openArtifactComment = useMemo(
    () => (openComment ? { file: openComment, chart: openComment } : undefined),
    [openComment],
  );
  const commentOpeners = typeof onOpenArtifactComment === 'function' ? openArtifactComment : onOpenArtifactComment;

  // The main transcript's action surface. Every member is identity-stable, so
  // this object is built once and never re-renders the memoized message tree.
  const messageActions = useMemo<MessageActions>(() => ({
    ...stable,
    onOpenArtifactComment: commentOpeners,
    onFocusComposer: () => chatInputRef.current?.focus(),
    editDrafts,
    widgetPrompts,
    reportThreadId,
    feedbackLoading,
  }), [stable, commentOpeners, chatInputRef, editDrafts, widgetPrompts, reportThreadId, feedbackLoading]);

  // The subagent transcript is a DIFFERENT surface: its cards belong to a task,
  // not to the main thread's turn, so the main thread's approve/reject/edit
  // handlers must not be reachable from it. Navigation only, and Send now for
  // the reader's own instruction to that task.
  // No onRevealFiles: it moves the main transcript, and the settle-aware
  // observer that follows an unfolding deck is only attached there.
  const subagentMessageActions = useMemo<MessageActions>(() => ({
    onOpenFile: stable.onOpenFile,
    onDownloadFile: stable.onDownloadFile,
    onPublishFile: stable.onPublishFile,
    downloadKeyFor: stable.downloadKeyFor,
    onToolCallDetailClick: stable.onToolCallDetailClick,
    onOpenChart: stable.onOpenChart,
    onSendNow: task.onSendNow,
  }), [stable, task]);

  return { messageActions, subagentMessageActions };
}
