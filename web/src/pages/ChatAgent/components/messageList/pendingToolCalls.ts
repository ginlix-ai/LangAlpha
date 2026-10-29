/**
 * The transcript's read of the tool calls whose arguments are still streaming,
 * as the assistant message keeps them (`pendingToolCallChunks`). The stream
 * handlers write them (`session/stream/toolCallChunks.ts`), which takes type
 * imports only so they stay clear of the render module graph.
 */
import type { PendingToolCallChunk } from '@/types/chat';
import { byArrival } from '../../session/stream/toolCallChunks';
import type { PreparingCall, PreparingToolCallData } from './activityTypes';

/** The calls still preparing, in the order they were written, minus any the
 *  transcript never draws and any that already landed. Null when none is. */
export function preparingCallsOf(
  pending: Record<string, PendingToolCallChunk> | undefined,
  landed: Readonly<Record<string, unknown>>,
  hidden: ReadonlySet<string>,
): PreparingToolCallData | null {
  if (!pending) return null;
  const calls: PreparingCall[] = [];
  for (const key of byArrival(pending)) {
    const { toolName, toolCallId, argsLength, firstSeenAt } = pending[key];
    if (toolName && hidden.has(toolName)) continue;
    if (toolCallId && landed[toolCallId]) continue;
    calls.push({
      pendingKey: `${key}@${firstSeenAt}`,
      toolCallId: toolCallId ?? undefined,
      toolName: toolName ?? undefined,
      argsLength,
      firstSeenAt,
    });
  }
  return calls.length ? { calls } : null;
}

export function samePreparing(a: PreparingToolCallData, b: PreparingToolCallData): boolean {
  return a.calls.length === b.calls.length && a.calls.every((call, i) => {
    const other = b.calls[i];
    return call.pendingKey === other.pendingKey && call.toolCallId === other.toolCallId
      && call.toolName === other.toolName && call.argsLength === other.argsLength;
  });
}
