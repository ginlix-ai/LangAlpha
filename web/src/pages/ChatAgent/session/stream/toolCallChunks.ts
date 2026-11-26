/**
 * The write side of the tool calls whose arguments are still streaming, as
 * the assistant message keeps them (`pendingToolCallChunks`): one entry per
 * call, under the index the model gave it. The main stream and a subagent's
 * both write it here, and the transcript reads it back as preparing rows
 * (`components/messageList/pendingToolCalls.ts`). Type imports only, so the
 * stream handlers can use it without the render module graph.
 */
import type { PendingToolCallChunk, WidgetDraft } from '@/types/chat';
import { EMPTY_WIDGET_ARGS, feedWidgetArgs } from './widgetArgs';

/** The tool whose arguments are read as they stream, to draw its widget. */
export const WIDGET_TOOL = 'ShowWidget';

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

type Drafts = Record<string, WidgetDraft>;

/** Stands in for the id of a call whose producer has not sent one yet. */
const UNNAMED = 'chunk:';

/**
 * The key a streaming call's draft is kept under: its id, or until the id
 * arrives its index and start, so a producer that names the call late loses
 * none of what was drawn.
 */
export function widgetDraftKey(call: PendingToolCallChunk, index: string): string {
  return call.toolCallId ?? `${UNNAMED}${index}@${call.firstSeenAt}`;
}

/**
 * `drafts` without a draft whose arguments are still streaming and whose call
 * has no entry in `pending` any more: one that was dropped, or replaced at its
 * index. A landed call's draft has no scan, so it stays.
 */
function pruneDrafts(drafts: Drafts, pending: PendingCalls): Drafts {
  let next: Drafts | null = null;
  for (const [key, draft] of Object.entries(drafts)) {
    if (!draft.scan) continue;
    if (Object.entries(pending).some(([index, call]) => widgetDraftKey(call, index) === key)) continue;
    next ??= { ...drafts };
    delete next[key];
  }
  return next ?? drafts;
}

/**
 * `drafts` with `chunk`, already folded into `pending`, read into its call's
 * draft when the call is a ShowWidget: only the main agent draws widgets, and
 * no other call's arguments are kept, since they can run to megabytes. A call
 * named only after its first chunk has lost its start, so it gets no draft.
 */
export function feedWidgetDraft(drafts: Drafts, pending: PendingCalls, chunk: ToolCallChunkInput): Drafts {
  const index = `${chunk.index ?? 0}`;
  const call = pending[index];
  if (!call) return drafts;
  const current = call.chunkCount === 1 ? pruneDrafts(drafts, pending) : drafts;
  if (call.toolName !== WIDGET_TOOL) return current;
  const key = widgetDraftKey(call, index);
  const unnamed = `${UNNAMED}${index}@${call.firstSeenAt}`;
  const prior = current[key] ?? current[unnamed];
  if (prior ? !prior.scan : call.chunkCount > 1) return current;
  const scan = feedWidgetArgs(prior?.scan ?? EMPTY_WIDGET_ARGS, chunk.args ?? '');
  const next = { ...current };
  delete next[unnamed];
  next[key] = { html: scan.html, title: scan.title, argsLength: call.argsLength, scan };
  return next;
}

/**
 * `drafts` once `call` has landed and `pending` dropped what that landing
 * took. A ShowWidget call is drawn from its complete arguments while the tool
 * runs, unless its widget is already `drawn`; one whose arguments carry no
 * markup draws nothing.
 */
export function landWidgetDraft(
  drafts: Drafts,
  pending: PendingCalls,
  call: { id: string; name?: string; args?: unknown },
  drawn: boolean,
): Drafts {
  const next = pruneDrafts(drafts, pending);
  if (call.name !== WIDGET_TOOL) return next;
  const args = call.args as Record<string, unknown> | undefined;
  const html = typeof args?.html === 'string' && !drawn ? args.html : null;
  const prior = next[call.id];
  if (html === null) return prior ? withoutWidgetDraft(next, call.id) : next;
  const title = typeof args?.title === 'string' ? args.title : null;
  if (prior && !prior.scan && prior.html === html && prior.title === title) return next;
  return { ...next, [call.id]: { html, title, argsLength: html.length } };
}

/** `drafts` without the draft of `toolCallId`, and itself when it has none. */
export function withoutWidgetDraft(drafts: Drafts, toolCallId: string): Drafts {
  if (!drafts[toolCallId]) return drafts;
  const { [toolCallId]: _dropped, ...rest } = drafts;
  return rest;
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

/**
 * `message` at the end of its turn: nothing preparing, and no landed widget
 * still waiting on its artifact, since none can arrive once the turn is over.
 * Not for the end of one model call (`withoutPending`), after which a landed
 * call's widget is still on its way, though a draft still streaming goes.
 */
export function withoutStreamDrafts<T extends object>(message: T): T {
  const cleared = withoutPending(message);
  const drafts = (cleared as { widgetDrafts?: object }).widgetDrafts;
  return drafts && Object.keys(drafts).length > 0 ? { ...cleared, widgetDrafts: {} } : cleared;
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
  if (!('pendingToolCallChunks' in message) || !hasPreparing(message)) return message;
  const drafts = (message as { widgetDrafts?: Drafts }).widgetDrafts;
  const kept = drafts && pruneDrafts(drafts, {});
  return { ...message, pendingToolCallChunks: {}, ...(kept !== drafts ? { widgetDrafts: kept } : {}) };
}
