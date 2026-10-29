/**
 * Shared interrupt descriptors — the one place the HITL card vocabulary is
 * declared. Both projections (live stream and history replay) key off these
 * tables; the dedup rebuild in messageFinalizers walks INTERRUPT_CARD_BUCKETS.
 * A new interrupt type registers here first, then adds its projection branches.
 */

import type { AssistantMessage, ChatMessage } from '@/types/chat';
import { updateMessage } from '../../hooks/utils/messageHelpers';
import { dispatchResultFields, type ToolResultLike } from '../../utils/dispatchResult';
import type { HistoryInterruptInfo, SetMessages } from '../types';

/** Interrupt types that map to proposal-based HITL cards (workspace, question, ptc, secretary). */
const PROPOSAL_INTERRUPT_TYPES = new Set([
  'create_workspace', 'start_question', 'ptc_agent',
  'delete_workspace', 'stop_workspace', 'delete_thread',
]);

/** Maps interrupt types to their proposal bucket key on AssistantMessage. */
const PROPOSAL_DATA_KEY_MAP: Record<string, keyof AssistantMessage> = {
  create_workspace: 'workspaceProposals',
  start_question: 'questionProposals',
  ptc_agent: 'ptcAgentProposals',
  delete_workspace: 'secretaryActionProposals',
  stop_workspace: 'secretaryActionProposals',
  delete_thread: 'secretaryActionProposals',
};

/**
 * The card bucket each pending interrupt settles into, keyed by the type its
 * pending entry carries — which, unlike the action request's `type`, names a
 * tool approval too, so there is no fallback case to get wrong. Typed against
 * `AssistantMessage` so a renamed bucket fails here rather than writing a card
 * under a name nothing renders.
 */
const CARD_BUCKET_FOR_TYPE: Record<string, keyof AssistantMessage> = {
  ask_user_question: 'userQuestions',
  credit_pause: 'creditPauses',
  tool_approval: 'toolApprovals',
  ...PROPOSAL_DATA_KEY_MAP,
};

/**
 * Message-map buckets whose entries carry an `interruptId` (rendered HITL
 * cards). `satisfies keyof AssistantMessage` makes a renamed/added bucket a
 * compile error here instead of a silently-wrong dedup rebuild.
 */
const INTERRUPT_CARD_BUCKETS = [
  'userQuestions', 'workspaceProposals', 'questionProposals',
  'ptcAgentProposals', 'secretaryActionProposals',
  'creditPauses', 'toolApprovals',
] as const satisfies readonly (keyof AssistantMessage)[];

/**
 * Merge fields into one card wherever it lives. A deduped re-raise can put the
 * card on a bubble other than the one the answer came from, so every assistant
 * message is searched rather than the one that was clicked.
 */
function setCardFields(
  messages: ChatMessage[],
  bucket: string,
  cardId: string,
  fields: Record<string, unknown>,
): ChatMessage[] {
  return messages.map((m) => {
    if (m.role !== 'assistant') return m;
    const cards = (m as unknown as Record<string, Record<string, Record<string, unknown>>>)[bucket];
    if (!cards?.[cardId]) return m;
    return { ...(m as AssistantMessage), [bucket]: { ...cards, [cardId]: { ...cards[cardId], ...fields } } };
  });
}

/** Flip one card's status wherever it lives. */
function setCardStatus(
  messages: ChatMessage[],
  bucket: string,
  cardId: string,
  status: string,
): ChatMessage[] {
  return setCardFields(messages, bucket, cardId, { status });
}

/**
 * Drop the history copies of these cards from every bubble, so a reconnect
 * stream's redelivery of the same interrupt is the only copy on screen.
 *
 * By card id, never by the bubble a pending entry names: a re-raised interrupt
 * restores its cards where they were first rendered while its entries are
 * queued against the turn that re-raised them, and a bubble-scoped strip would
 * leave those cards standing next to the redelivered pair.
 */
function stripHistoryInterruptCards(
  messages: ChatMessage[],
  strips: HistoryInterruptInfo[],
): ChatMessage[] {
  // Question and proposal ids come from distinct interrupts, so one set can
  // hold both: only card segments carry either id.
  const ids = new Set(strips.map(historyCardKey).filter((id): id is string => !!id));
  if (ids.size === 0) return messages;
  return messages.map((m) => {
    if (m.role !== 'assistant') return m;
    const msg = m as AssistantMessage;
    const next: AssistantMessage = {
      ...msg,
      contentSegments: (msg.contentSegments || []).filter((seg) => {
        const { questionId, proposalId } = seg as { questionId?: string; proposalId?: string };
        const id = questionId ?? proposalId;
        return !id || !ids.has(id);
      }),
    };
    for (const key of INTERRUPT_CARD_BUCKETS) {
      const bucket = msg[key];
      if (!bucket) continue;
      const map = { ...(bucket as Record<string, unknown>) };
      for (const id of ids) delete map[id];
      (next as unknown as Record<string, unknown>)[key] = map;
    }
    return next;
  });
}

/**
 * The card id a pending history interrupt settles under. Each family fills
 * exactly one of these at queue time, so the first one present is its key.
 */
function historyCardKey(info: HistoryInterruptInfo): string | undefined {
  return info.proposalId ?? info.questionId;
}

/** Which card a resolved history interrupt writes to, and what it writes. */
interface HistoryCardPatch {
  bucket: string;
  key: string;
  fields: Record<string, unknown>;
}

/**
 * Settle the first pending history interrupt `match` accepts: merge its patch
 * into the card and drop it from the pending list, so what remains at the end
 * of replay is exactly what the user still owes an answer.
 */
function resolvePendingHistoryInterrupt(
  pending: HistoryInterruptInfo[],
  match: (p: HistoryInterruptInfo) => boolean,
  toPatch: (matched: HistoryInterruptInfo) => HistoryCardPatch | null,
  setMessages: SetMessages,
): boolean {
  const idx = pending.findIndex(match);
  if (idx === -1) return false;
  const matched = pending[idx];
  // A caller that matches an entry but declines to patch it leaves the card
  // pending and the entry queued, so replay still ends holding what is owed.
  const patch = toPatch(matched);
  if (!patch) return false;
  const { bucket, key, fields } = patch;
  setMessages((prev) =>
    updateMessage(prev, matched.assistantMessageId, (m) => {
      if (m.role !== 'assistant') return m;
      const msg = m as AssistantMessage;
      const cards = ((msg as unknown as Record<string, unknown>)[bucket] || {}) as Record<string, Record<string, unknown>>;
      return { ...msg, [bucket]: { ...cards, [key]: { ...(cards[key] || {}), ...fields } } };
    })
  );
  pending.splice(idx, 1);
  return true;
}

/**
 * Settle the pending proposal a tool result answers. A decline rejects it, and
 * so does a result with `success: false` on any card but a hand-off's (see
 * `dispatchResultFields`); anything else approves it. One resume can answer
 * several interrupts, and their results need not arrive in the order the cards
 * did, so an interrupt that named its tool call (a dispatch does) is settled
 * only by that call's result; the rest take the next result in order.
 */
function settleProposalFromResult(
  pending: HistoryInterruptInfo[],
  toolCallId: string | undefined,
  result: ToolResultLike,
  setMessages: SetMessages,
): boolean {
  const { content } = result;
  if (typeof content !== 'string') return false;
  const named = !!toolCallId && pending.some((p) => p.toolCallId === toolCallId);
  return resolvePendingHistoryInterrupt(
    pending,
    (p) => PROPOSAL_INTERRUPT_TYPES.has(p.type) &&
      (named ? p.toolCallId === toolCallId : !toolCallId || !p.toolCallId),
    // Parsed here so a result pays for it only when it settles a proposal.
    (m) => {
      let status = 'approved';
      if (content.startsWith('User declined')) {
        status = 'rejected';
      } else if (m.type !== 'ptc_agent') {
        try {
          if (JSON.parse(content)?.success === false) status = 'rejected';
        } catch { /* non-JSON → treat as approved */ }
      }
      const extraFields = m.type === 'ptc_agent' && status === 'approved'
        ? dispatchResultFields(result)
        : {};
      return {
        bucket: PROPOSAL_DATA_KEY_MAP[m.type] || 'questionProposals',
        key: m.proposalId!,
        fields: { status, ...extraFields },
      };
    },
    setMessages,
  );
}

export {
  PROPOSAL_INTERRUPT_TYPES, PROPOSAL_DATA_KEY_MAP,
  INTERRUPT_CARD_BUCKETS, CARD_BUCKET_FOR_TYPE,
  setCardStatus, setCardFields, resolvePendingHistoryInterrupt, historyCardKey,
  settleProposalFromResult,
  stripHistoryInterruptCards,
};
