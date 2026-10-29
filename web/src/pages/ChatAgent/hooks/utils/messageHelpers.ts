/**
 * Message creation and manipulation utilities
 * Provides helper functions for creating and updating message objects
 */

import type {
  ChatMessage,
  AssistantMessage,
  UserMessage,
  NotificationMessage,
  NotificationVariant,
  WakeupArrival,
} from '@/types/chat';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import type { ChartSelectionSnapshot } from '@/pages/MarketView/stores/chartSelectionStore';

// Re-export types for consumers
export type { ChatMessage, AssistantMessage, UserMessage, NotificationMessage, NotificationVariant };

// Keeps ids made within the same millisecond apart: a message is updated by
// id, so two sharing one are both updated.
let _idSeq = 0;

export interface AttachmentMeta {
  file: File;
  dataUrl: string;
  type: string;
}

/**
 * `turnIndex` is the backend turn the bubble belongs to, or undefined for one
 * whose turn the transcript's count names (see projectTurns). Required, so
 * every bubble the view creates decides its turn where it is made. `id` is a
 * bubble already on screen that the message takes over.
 */
export function createUserMessage(
  message: string,
  turnIndex: number | undefined,
  { attachments, widgetSnapshots, chartSelections, id }: {
    attachments?: AttachmentMeta[] | null;
    widgetSnapshots?: WidgetContextSnapshot[] | null;
    chartSelections?: ChartSelectionSnapshot[] | null;
    id?: string;
  } = {},
): UserMessage {
  const msg: UserMessage = {
    id: id ?? `user-${Date.now()}-${_idSeq++}`,
    role: 'user',
    content: message,
    contentType: 'text',
    timestamp: new Date(),
    isStreaming: false,
    turnIndex,
    // Every caller is a send from this view; history builds its own rows.
    sentHere: true,
  };
  if (attachments && attachments.length > 0) {
    // AttachmentMeta is the upload-time shape (file, dataUrl, type).
    // Attachment from sse.ts has a different shape (name, size, url).
    // At send time only AttachmentMeta fields are used, so store as-is.
    msg.attachments = attachments as any;
  }
  if (widgetSnapshots && widgetSnapshots.length > 0) {
    msg.widgetSnapshots = widgetSnapshots;
  }
  if (chartSelections && chartSelections.length > 0) {
    msg.chartSelections = chartSelections;
  }
  return msg;
}

/** Whether a user message carried more than its text: an attachment, a chart
 *  selection or a widget, which a send of its text alone would drop. */
export function carriesContext(message: UserMessage): boolean {
  return !!(message.attachments?.length || message.chartSelections?.length || message.widgetSnapshots?.length);
}

/** `turnIndex` as for createUserMessage. */
export function createAssistantMessage(id: string, turnIndex: number | undefined): AssistantMessage {
  return {
    id,
    role: 'assistant',
    content: '',
    contentType: 'text',
    timestamp: new Date(),
    isStreaming: true,
    contentSegments: [],
    reasoningProcesses: {},
    toolCallProcesses: {},
    todoListProcesses: {},
    turnIndex,
  };
}

export function updateMessage<T extends { id: string }>(
  messages: T[],
  messageId: string,
  updater: (msg: T) => T,
): T[] {
  return messages.map((msg) => {
    if (msg.id !== messageId) return msg;
    return updater(msg);
  });
}

export function insertMessage<T extends { id: string }>(
  messages: T[],
  insertIndex: number,
  newMessage: T,
): T[] {
  return [
    ...messages.slice(0, insertIndex),
    newMessage,
    ...messages.slice(insertIndex),
  ];
}

export function appendMessage<T extends { id: string }>(messages: T[], newMessage: T): T[] {
  return [...messages, newMessage];
}

/**
 * Creates a notification message for inline dividers (e.g. compaction, offload).
 * ``detail`` is the optional expandable text (e.g. the compaction summary).
 */
export function createNotificationMessage(
  text: string,
  variant: NotificationVariant = 'info',
  detail?: string,
): NotificationMessage {
  return {
    id: `notification-${Date.now()}-${_idSeq++}`,
    role: 'notification',
    content: text,
    variant,
    timestamp: new Date(),
    detail,
  };
}

/**
 * The row a wakeup, or a price alert, draws in the transcript. The id is the
 * caller's, so the same notice keeps one id across a live stream and every
 * replay.
 */
export function createWakeupMessage(
  id: string,
  wakeup: WakeupArrival,
  timestamp: Date,
): NotificationMessage {
  return {
    id,
    role: 'notification',
    content: '',
    variant: 'info',
    timestamp,
    wakeup,
  };
}
