/**
 * Message classification shared by the transcript renderer and the turn-index
 * math. Steering bubbles are mid-turn injections with NO backend turn boundary
 * of their own, so every consumer must classify them identically — the edit
 * gate, the regenerate tail, and the turn projection each carried their own
 * inline `role === 'assistant' && !isSteering` check before this module.
 */
import type { MessageRecord } from './types';

/** The subset of message fields the predicates read. Structural so both the
 *  loose `MessageRecord` wire shape and the typed `ChatMessage` union fit. */
export interface MessageLike {
  role?: unknown;
  steering?: unknown;
  steeringDelivered?: unknown;
  isSteering?: unknown;
}

/**
 * A user bubble that was injected into a RUNNING turn (queued or already
 * delivered as steering). It has no `/turns` checkpoint of its own, so an edit
 * fork would land on the NEXT turn and leave the original steering text in the
 * agent's context — the pencil is hidden for these and the hook refuses them.
 */
export function isSteeringUserMessage(message: MessageLike): boolean {
  if (message.role !== 'user') return false;
  return !!message.steeringDelivered || !!message.steering;
}

/**
 * An assistant bubble that CONTINUES the turn its predecessor opened (the
 * agent resumed after consuming steering). It is not a turn of its own: the
 * turn index folds back to the bubble it continues, and regenerate re-runs the
 * whole turn from the opener's checkpoint.
 */
export function isSteeringContinuation(message: MessageLike): boolean {
  return message.role === 'assistant' && !!message.isSteering;
}

/**
 * An assistant bubble that settled with nothing to show. Some turns legitimately
 * finalize empty in STATE — a HITL resume whose content landed on another bubble,
 * or a history turn whose only event was a re-raised interrupt deduped by
 * interrupt_id — and they must stay in state because edit/regenerate map UI
 * position → backend turn_index by counting assistant bubbles. But painting them
 * shows an orphan avatar + action row, so the list skips rendering them.
 * Anything renderable keeps the bubble: a streaming indicator, text, segments,
 * the Sources pill, the Stopped chip, or an error.
 *
 * INVARIANT: everything an assistant bubble can render must surface through
 * content / contentSegments / provenanceRecords / error / stopped / isStreaming.
 * A future assistant field that renders OUTSIDE those (e.g. assistant-side
 * attachments) must be added to this guard or its bubbles will be hidden.
 */
export function isOrphanAssistantMessage(message: MessageRecord): boolean {
  if (message.role !== 'assistant') return false;
  if (message.isStreaming) return false;
  const segments = message.contentSegments as unknown[] | undefined;
  if (segments && segments.length > 0) return false;
  if (message.content) return false;
  const provenance = message.provenanceRecords as Record<string, unknown> | undefined;
  if (provenance && Object.keys(provenance).length > 0) return false;
  return !message.error && !message.stopped;
}
