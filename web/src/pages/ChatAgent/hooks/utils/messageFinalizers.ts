/**
 * Pure message-array finalizers and segment builders shared by the live and
 * history paths of the chat engine. Extracted verbatim from useChatMessages
 * module scope (W1).
 */

import type { ChatMessage, AssistantMessage, NotificationSegment } from '@/types/chat';
import type { TodoItem } from '@/types/sse';
import { INTERRUPT_CARD_BUCKETS } from '../../session/interrupts/buckets';
import type { ToolCallAgentIndex } from '../../session/subagents/historyStore';

/** Collects the interrupt_ids of every HITL card rendered on the given messages. */
function collectRenderedInterruptIds(messages: ChatMessage[]): Set<string> {
  const ids = new Set<string>();
  for (const m of messages) {
    if (m.role !== 'assistant') continue;
    for (const bucket of INTERRUPT_CARD_BUCKETS) {
      const entries = m[bucket];
      if (!entries) continue;
      for (const entry of Object.values(entries)) {
        if (entry?.interruptId) ids.add(entry.interruptId);
      }
    }
  }
  return ids;
}

/**
 * Build the transcript notification segment for a model_fallback event.
 * Shared by the live-stream and history-replay branches. The expandable
 * detail carries the failed model's error + HTTP status / attempt count;
 * `suggestedModel` lets the renderer offer switching to the model that
 * actually answered.
 */
function buildModelFallbackSegment(
  event: Record<string, unknown>,
  t: (key: string, opts?: Record<string, unknown>) => string,
  order: number,
): NotificationSegment {
  const fromModel = (event.from_model as string) || '';
  const toModel = (event.to_model as string) || '';
  const errText = typeof event.error === 'string' ? event.error.trim() : '';
  const statusCode = typeof event.status_code === 'number' ? event.status_code : null;
  const attempts = typeof event.attempts_on_from === 'number' ? event.attempts_on_from : null;
  const metaBits: string[] = [];
  if (statusCode != null) metaBits.push(`HTTP ${statusCode}`);
  if (attempts != null && attempts > 0) metaBits.push(t('chat.modelFallbackAttempts', { count: attempts }));
  const detail = [metaBits.join(' · '), errText].filter(Boolean).join('\n') || undefined;
  const segment: NotificationSegment = {
    type: 'notification',
    content: t('chat.modelFallbackNotification', { from: fromModel, to: toModel }),
    order,
  };
  if (detail) {
    segment.detail = detail;
    segment.detailKind = 'error';
  }
  return segment;
}

/**
 * Append a notification segment unless one with the same order already exists.
 * Redelivery guard: a reconnect or replay that re-sends a persisted event
 * (same _eventId-derived order) must not create a duplicate divider.
 */
function appendNotificationSegmentOnce(
  aMsg: AssistantMessage,
  segment: NotificationSegment,
): AssistantMessage {
  const existing = aMsg.contentSegments || [];
  if (existing.some((s) => s.type === 'notification' && s.order === segment.order)) {
    return aMsg;
  }
  return { ...aMsg, contentSegments: [...existing, segment] };
}


/**
 * Whether a tool result changed the user's workspaces: an agent created one,
 * or handed work to one, which may create it and starts its computer. Both
 * answer with the workspace's id. The user's own changes refresh the cached
 * lists in their mutation hooks; this is the agent's equivalent.
 */
function isWorkspaceChangingToolSuccess(resultContent: unknown): boolean {
  if (typeof resultContent !== 'string' || !resultContent.startsWith('{')) return false;
  try {
    const parsed = JSON.parse(resultContent);
    return parsed?.success === true && typeof parsed.workspace_id === 'string';
  } catch {
    return false;
  }
}


/**
 * Marks incomplete todos as 'stale' in todoListProcesses of assistant messages.
 * Used when the agent stream ends without completing all todos.
 * @param messages - Current messages array
 * @param targetMessageId - If provided, only finalize the specific message; otherwise finalize all
 */
export function finalizeTodoListProcessesInMessages(
  messages: ChatMessage[],
  targetMessageId?: string
): ChatMessage[] {
  let anyChanged = false;
  const updated = messages.map((m) => {
    if (m.role !== 'assistant') return m;
    if (targetMessageId && m.id !== targetMessageId) return m;
    const am = m as AssistantMessage;
    if (!am.todoListProcesses || Object.keys(am.todoListProcesses).length === 0) return m;
    const entries = Object.entries(am.todoListProcesses);
    const lastEntry = entries.reduce((a, b) => ((a[1].order || 0) >= (b[1].order || 0) ? a : b));
    const [lastKey, lastVal] = lastEntry;
    if (!Array.isArray(lastVal.todos)) return m;
    const hasIncomplete = lastVal.todos.some(
      (todo: TodoItem) => todo.status !== 'completed' && todo.status !== 'stale'
    );
    if (!hasIncomplete) return m;
    anyChanged = true;
    const finalizedTodos: TodoItem[] = lastVal.todos.map((todo: TodoItem) =>
      todo.status === 'completed' || todo.status === 'stale'
        ? todo
        : { ...todo, status: 'stale' as const }
    );
    return {
      ...am,
      todoListProcesses: {
        ...am.todoListProcesses,
        [lastKey]: { ...lastVal, todos: finalizedTodos, in_progress: 0, pending: 0 },
      },
    };
  });
  return anyChanged ? updated : messages;
}

/**
 * Map a task artifact event's tool_call_id to its agentId and drain the pending queue.
 *
 * When multiple Task tool calls are in a single tool_calls event, the pending queue
 * holds their IDs in array order. Because LangGraph processes tool calls in parallel,
 * artifact events may arrive in a different order. This function uses a direct mapping
 * (by value) when tool_call_id is available, falling back to FIFO for legacy events.
 *
 * @returns Updated pending queue after draining.
 */
export function mapToolCallIdToAgentId(
  eventToolCallId: string | undefined,
  agentId: string,
  action: string,
  pendingToolCallIds: string[],
  toolCallIdMap: Pick<ToolCallAgentIndex, 'has' | 'set'>,
): string[] {
  if (eventToolCallId) {
    toolCallIdMap.set(eventToolCallId, agentId);
  }
  if (action !== 'init') {
    return pendingToolCallIds;
  }
  if (pendingToolCallIds.length === 0) {
    return pendingToolCallIds;
  }
  if (eventToolCallId) {
    // Direct mapping — remove by value (not FIFO) since parallel tool calls
    // may complete in different order than the tool_calls array.
    return pendingToolCallIds.filter(id => id !== eventToolCallId);
  }
  // Legacy fallback: FIFO drain for events without tool_call_id.
  const [firstId, ...rest] = pendingToolCallIds;
  if (!toolCallIdMap.has(firstId)) {
    toolCallIdMap.set(firstId, agentId);
  }
  return rest;
}

export { collectRenderedInterruptIds, buildModelFallbackSegment, appendNotificationSegmentOnce, isWorkspaceChangingToolSuccess };
