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
} from '@/types/chat';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

// Re-export types for consumers
export type { ChatMessage, AssistantMessage, UserMessage, NotificationMessage, NotificationVariant };

// Module-level sequence counter to avoid ID collisions when multiple
// notifications are created within the same millisecond.
let _notifSeq = 0;

export interface AttachmentMeta {
  file: File;
  dataUrl: string;
  type: string;
}

export function createUserMessage(
  message: string,
  attachments: AttachmentMeta[] | null = null,
  widgetSnapshots: WidgetContextSnapshot[] | null = null,
): UserMessage {
  const msg: UserMessage = {
    id: `user-${Date.now()}`,
    role: 'user',
    content: message,
    contentType: 'text',
    timestamp: new Date(),
    isStreaming: false,
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
  return msg;
}

export function createAssistantMessage(messageId: string | null = null): AssistantMessage {
  const id = messageId || `assistant-${Date.now()}`;
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
    id: `notification-${Date.now()}-${_notifSeq++}`,
    role: 'notification',
    content: text,
    variant,
    timestamp: new Date(),
    detail,
  };
}
