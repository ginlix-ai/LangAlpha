/**
 * The card state for a direct MCP tool call stopped for approval. Both
 * projections build it from the same action request so a reload renders the
 * card the live stream did.
 */

import type { ToolApprovalState } from '@/types/chat';
import type { ActionRequest } from '@/types/sse';
import { isDirectToolName, parseDirectToolName } from '../../utils/directTools';

/**
 * Whether an interrupt's first action request is a stopped tool call.
 *
 * A direct MCP name is the original signal and still the common one. An order
 * request is also one whatever it is named: the order gate raises its own
 * interrupt, and a keyed request that fell through to the plan-approval branch
 * would render a plan card over a live order.
 */
export function isToolApprovalRequest(request: ActionRequest | undefined): boolean {
  if (!request) return false;
  return isDirectToolName(request.name) || !!request.attempt_id || !!request.order;
}

/** The same question asked of a whole interrupt. The gate says what it raised,
 *  which settles the routing even for an order whose every request went out
 *  under a name and a shape this build has never seen. */
export function isToolApprovalInterrupt(
  kind: string | undefined,
  request: ActionRequest | undefined,
): boolean {
  return kind === 'order_approval' || isToolApprovalRequest(request);
}

export function buildToolApprovalState(
  request: ActionRequest,
  interruptId: string | undefined,
  actionIndex: number,
  actionCount: number,
): ToolApprovalState {
  const name = request.name || '';
  const parsed = parseDirectToolName(name) || { server: '', tool: name };
  return {
    toolName: name,
    server: parsed.server,
    tool: parsed.tool,
    args: request.args && typeof request.args === 'object' ? request.args : {},
    interruptId,
    actionIndex,
    actionCount,
    toolCallId: request.tool_call_id,
    attemptId: request.attempt_id,
    order: request.order ?? null,
    status: 'pending',
    reason: null,
  };
}

/**
 * Where one stopped call's verdict is looked up, decided once when the
 * interrupt is read rather than rebuilt from a card id's `#N` suffix.
 *
 * A keyed request is answered by the attempt it names, which is true however
 * the interrupt was re-raised or re-ordered. Position is the fallback for a
 * request that carried no attempt, and for a keyed one whose resume predates
 * the map.
 */
export type DecisionTarget =
  | { kind: 'attempt'; attemptId: string; index: number }
  | { kind: 'position'; index: number };

/**
 * One card per stopped call, keyed so the two projections agree.
 *
 * A turn that places two orders arrives as ONE interrupt carrying both action
 * requests, and the resume must answer every one of them. Rendering only the
 * first left the second call invisible and the resume short a decision, which
 * the server rejects outright.
 */
export function toolApprovalCards(
  actionRequests: ActionRequest[],
  interruptId: string | undefined,
  fallbackId: string,
): Array<{ proposalId: string; target: DecisionTarget; state: ToolApprovalState }> {
  const base = interruptId || fallbackId;
  return actionRequests.map((request, index) => ({
    proposalId: actionRequests.length > 1 ? `${base}#${index}` : base,
    target: request.attempt_id
      ? { kind: 'attempt', attemptId: request.attempt_id, index }
      : { kind: 'position', index },
    state: buildToolApprovalState(request, interruptId, index, actionRequests.length),
  }));
}

/**
 * The verdict to write on the cards of an interrupt that stopped several calls,
 * or null for an interrupt that stopped one, whose own verdict is knowable.
 *
 * A resume persists one answer per interrupt, never one per call: approving
 * AAPL and rejecting MSFT records exactly what rejecting both records (see
 * `hitl_answers` in the server's `process_hitl_response`). No reject in a batch
 * is attributable to a particular call, so these cards read approved instead of
 * taking the interrupt-level verdict onto every one of them. The other floor
 * puts a rejected badge on an order that is executing, and a user who reads
 * that places it a second time. It is a floor, not evidence: only a
 * per-decision record on the resume turn can replace it with the truth.
 *
 * That record is `hitl_decisions`, read by `resolveApprovalDecision`. This
 * floor is what a thread persisted before the server kept it still gets.
 */
export function batchToolApprovalFields(
  cardCount: number,
): { status: 'approved'; reason: null } | null {
  return cardCount > 1 ? { status: 'approved', reason: null } : null;
}

/** One entry of the resume's `hitl_decisions`, in the shape the server records
 *  a `HITLDecision` in: the verdict, plus the user's own message if they gave
 *  one. */
export interface HitlDecision {
  type?: string;
  message?: string | null;
}

/**
 * The per-interrupt decision lists a resume turn recorded, or undefined for a
 * thread persisted before the server kept them.
 */
export function readHitlDecisions(
  metadata: Record<string, unknown> | undefined,
): Record<string, HitlDecision[]> | undefined {
  const raw = metadata?.hitl_decisions;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  return raw as Record<string, HitlDecision[]>;
}

/**
 * The verdicts a resume turn recorded per order attempt, keyed by attempt id.
 *
 * This is the record that survives everything position cannot: a batch whose
 * cards were re-raised onto another bubble, a turn whose interrupt stopped
 * more calls than the one the user answered, an order retried under a fresh
 * attempt. Undefined for a thread whose resume recorded none, which is every
 * thread from before the ledger and every resume that answered no order.
 */
export function readOrderDecisions(
  metadata: Record<string, unknown> | undefined,
): Record<string, HitlDecision> | undefined {
  const raw = metadata?.order_decisions;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  return raw as Record<string, HitlDecision>;
}

/**
 * What one resume recorded, as the two shapes the wire carries. Either the
 * whole replay's accumulated evidence or a single resume's metadata can answer
 * these, which is why both settlement paths share one resolver.
 */
export interface DecisionLookup {
  /** `hitl_decisions[interruptId]`: slot i answers action request i. */
  positional: HitlDecision[] | undefined;
  /** `order_decisions`: the verdict for one attempt, whatever slot it sat in. */
  attempt: (attemptId: string) => HitlDecision | undefined;
}

/**
 * One card's own verdict, or null when the resume recorded none.
 *
 * Keyed first, then positional: this is the evidence `batchToolApprovalFields`
 * says can replace its floor, so a mixed batch replays each call the way the
 * user answered it, reject reason included.
 */
export function resolveApprovalDecision(
  target: DecisionTarget,
  lookup: DecisionLookup,
): { status: 'approved' | 'rejected'; reason: string | null } | null {
  const keyed = target.kind === 'attempt' ? lookup.attempt(target.attemptId) : undefined;
  return orderDecisionFields(keyed) ?? orderDecisionFields(lookup.positional?.[target.index]);
}

/** One decision read as card fields, whichever record it came out of: the
 *  positional list, or the map keyed by attempt id. */
export function orderDecisionFields(
  decision: HitlDecision | undefined,
): { status: 'approved' | 'rejected'; reason: string | null } | null {
  if (!decision || (decision.type !== 'approve' && decision.type !== 'reject')) {
    return null;
  }
  const rejected = decision.type === 'reject';
  const message = typeof decision.message === 'string' ? decision.message.trim() : '';
  return { status: rejected ? 'rejected' : 'approved', reason: rejected && message ? message : null };
}
