/**
 * The write side of the tool calls whose arguments are still streaming, as
 * the assistant message keeps them (`pendingToolCallChunks`): one entry per
 * call, under the index the model gave it. The main stream and a subagent's
 * both write it here, and the transcript reads it back as preparing rows
 * (`components/messageList/pendingToolCalls.ts`). Type imports only, so the
 * stream handlers can use it without the render module graph.
 */
import type { PendingToolCallChunk } from '@/types/chat';

type PendingCalls = Record<string, PendingToolCallChunk>;

interface ToolCallChunkInput {
  index?: number;
  name?: string | null;
  args?: string | null;
  id?: unknown;
}

const idOf = (value: unknown): string | null => (typeof value === 'string' && value ? value : null);

/** `pending` with one streamed chunk folded into its call's entry. */
export function addToolCallChunk(pending: PendingCalls, chunk: ToolCallChunkInput, seenAt: number): PendingCalls {
  const key = `${chunk.index ?? 0}`;
  const id = idOf(chunk.id);
  const prior = pending[key];
  // A new id under an index an earlier call left behind is a new call there.
  const existing = prior && !(id && prior.toolCallId && prior.toolCallId !== id) ? prior : undefined;
  return {
    ...pending,
    [key]: {
      toolName: chunk.name || existing?.toolName || null,
      toolCallId: id ?? existing?.toolCallId ?? null,
      chunkCount: (existing?.chunkCount ?? 0) + 1,
      argsLength: (existing?.argsLength ?? 0) + (chunk.args?.length || 0),
      firstSeenAt: existing?.firstSeenAt ?? seenAt,
    },
  };
}

/** Earliest written first: calls land in the order the model wrote them. */
export function byArrival(pending: PendingCalls): string[] {
  return Object.keys(pending).sort((a, b) => pending[a].firstSeenAt - pending[b].firstSeenAt || Number(a) - Number(b));
}

/**
 * `pending` once the call `toolCallId` has landed: its own entry goes, and so
 * does every entry written before it, whose call has landed or never will (a
 * call the producer dropped). Each later call keeps its entry, and with it its
 * row, until it lands itself. A producer that streams arguments with no id is
 * matched by order instead: a call landing for the first time takes the
 * earliest entry nobody has named. Clearing everything, as this once did, took
 * every parallel call's row away when the first landed.
 */
export function removeLandedToolCall(pending: PendingCalls, toolCallId: string, firstLanding: boolean): PendingCalls {
  const order = byArrival(pending);
  let at = order.findIndex((key) => pending[key].toolCallId === toolCallId);
  if (at === -1 && firstLanding) at = order.findIndex((key) => !pending[key].toolCallId);
  if (at === -1) return pending;
  const next = { ...pending };
  for (const key of order.slice(0, at + 1)) delete next[key];
  return next;
}

/** Whether `message` has a call still preparing. */
export function hasPreparing(message: { pendingToolCallChunks?: unknown }): boolean {
  const pending = message.pendingToolCallChunks;
  return !!pending && typeof pending === 'object' && Object.keys(pending).length > 0;
}

/**
 * `message` once nothing on it is preparing, and the message itself when
 * nothing was, so clearing a bubble with nothing pending costs no re-render.
 * The producer sends a model call's `tool_calls`, one event per call, ahead
 * of its finish, so a call still preparing at the finish was dropped (an
 * empty name, a repeated id, no arguments) and never lands, as one still open
 * when its message stops streaming never does. Nothing else would take its
 * row away; a landing cannot, since the calls after it land in the events
 * that follow.
 */
export function withoutPending<T extends object>(message: T): T {
  return 'pendingToolCallChunks' in message && hasPreparing(message)
    ? { ...message, pendingToolCallChunks: {} }
    : message;
}
