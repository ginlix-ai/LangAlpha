/**
 * What a replayed turn settles on the cards before it: a resume answers the
 * interrupts it resumed, a steer marks the task's resume card, and a tool
 * result fills its call and settles the card it answers. The replay applies
 * these on its own page, and an older page applies the ones a newer page
 * carried back to it.
 */
import type { AssistantMessage, ChatMessage } from '@/types/chat';
import { handleHistoryToolCallResult } from './historyHandlers';
import type { SSEEvent, HistoryInterruptInfo, PairState } from '../types';
import { resolvePendingHistoryInterrupt, setCardStatus, setCardFields, settleProposalFromResult } from '../interrupts/buckets';
import {
  batchToolApprovalFields,
  readHitlDecisions,
  readOrderDecisions,
  resolveApprovalDecision,
} from '../interrupts/toolApprovalCard';
import type { HistoryRuntime } from '../runtime';

/**
 * Settle the interrupt cards a resume turn answered. Shared by the replay,
 * where the cards are on the same page, and by an older page, where they are
 * on that page and the resume is in its carry.
 */
export function settleResumedInterrupts(
  rt: Pick<HistoryRuntime, 'setMessages'>,
  pendingHistoryInterrupts: HistoryInterruptInfo[],
  event: SSEEvent,
): void {
  // Resolve tool_approval interrupts from the resume's content (empty =
  // approved, non-empty = rejected), with one more signal: a reject that
  // carried no reason leaves the content empty, so it is told apart from
  // an approve by its null `hitl_answers` entry (the only reject shape the
  // server records there).
  {
    const hitlAnswers = event.metadata?.hitl_answers as Record<string, unknown> | undefined;
    const hitlDecisions = readHitlDecisions(event.metadata);
    const orderDecisions = readOrderDecisions(event.metadata);
    const content = typeof event.content === 'string' ? event.content.trim() : '';
    const resumedIds = event.metadata?.hitl_interrupt_ids as string[] | undefined;
    // One call per resumed id, and one per card behind it: an interrupt
    // that stopped several calls raised several cards, and a single
    // resolve would leave the rest pending with live controls on a batch
    // already answered. Every HITL resume stamps the ids, so an ordinary
    // message settles nothing here.
    for (const interruptId of Array.isArray(resumedIds) ? resumedIds : []) {
      const answer = hitlAnswers ? hitlAnswers[interruptId] : undefined;
      const rejected = answer === null || (answer === undefined && !!content);
      // Content is attributable only when this resume answered one card;
      // in a batch it is the joined text of every reject in it.
      const batched = (resumedIds?.length || 0) > 1;
      const batchFields = batchToolApprovalFields(
        pendingHistoryInterrupts.filter(
          (p) => p.type === 'tool_approval' && p.interruptId === interruptId,
        ).length,
      );
      const lookup = {
        positional: hitlDecisions?.[interruptId],
        attempt: (attemptId: string) => orderDecisions?.[attemptId],
      };
      for (;;) {
        const idx = pendingHistoryInterrupts.findIndex(
          (p) => p.type === 'tool_approval' && p.interruptId === interruptId,
        );
        if (idx === -1) break;
        const [matched] = pendingHistoryInterrupts.splice(idx, 1);
        // By card id, not by bubble: a batch re-raised on a resume that
        // never consumed it is re-queued against that resume's bubble, so
        // a later attempt patching through updateMessage would flip an
        // invisible copy and leave the visible cards pending, with the
        // pending set already dropped so nothing could answer them. The
        // credit pause settles by id for the same reason.
        const decided = matched.target
          ? resolveApprovalDecision(matched.target, lookup)
          : null;
        rt.setMessages((prev) =>
          setCardFields(
            prev,
            'toolApprovals',
            matched.proposalId!,
            decided ??
              batchFields ?? {
                status: rejected ? 'rejected' : 'approved',
                reason: rejected && content && !batched ? content : null,
              },
          ),
        );
      }
    }
  }

  // Resolve ask_user_question interrupts from resume query metadata (hitl_answers).
  // Persisted immediately by persist_query_start(), keyed by interrupt_id.
  {
    const hitlAnswers = event.metadata?.hitl_answers as Record<string, unknown> | undefined;
    if (hitlAnswers && pendingHistoryInterrupts.length > 0) {
      for (const [interruptId, answerValue] of Object.entries(hitlAnswers)) {
        resolvePendingHistoryInterrupt(
          pendingHistoryInterrupts,
          (p) => p.type === 'ask_user_question' && p.interruptId === interruptId,
          (m) => ({
            bucket: 'userQuestions',
            key: m.questionId!,
            fields: {
              status: answerValue !== null ? 'answered' : 'skipped',
              answer: answerValue as string | null,
            },
          }),
          rt.setMessages,
        );
      }
    }
  }

  // Resolve credit_pause interrupts from the resume query's metadata.
  // A credit resume carries no answer (approve with no message), so it
  // never lands in `hitl_answers` the way a question does — but every
  // HITL resume stamps `hitl_interrupt_ids` with the ids it answered,
  // and that is the signal here. Without it the card replays pending
  // forever and re-arms a live Resume button on a turn that already
  // resumed and completed.
  {
    const resumedIds = event.metadata?.hitl_interrupt_ids as string[] | undefined;
    if (Array.isArray(resumedIds) && pendingHistoryInterrupts.length > 0) {
      for (const interruptId of resumedIds) {
        const idx = pendingHistoryInterrupts.findIndex(
          (p) => p.type === 'credit_pause' && p.interruptId === interruptId,
        );
        if (idx === -1) continue;
        pendingHistoryInterrupts.splice(idx, 1);
        // By card id, not by bubble: a pause re-raised on a refused
        // resume is re-queued against that resume's bubble, so a later
        // attempt resolving through updateMessage would flip an invisible
        // copy and leave the visible card pending — a Resume button on a
        // finished thread, and the pending set is gone, so it would not
        // even answer. The live path settles by id for the same reason.
        rt.setMessages((prev) => setCardStatus(prev, 'creditPauses', interruptId, 'resumed'));
      }
    }
  }
}

/** Mark the resume cards of steered tasks "Updated". */
export function markSteeredResumeCards(messages: ChatMessage[], steeredAgentIds: Set<string>): ChatMessage[] {
  if (steeredAgentIds.size === 0) return messages;
  let changedAny = false;
  const next = messages.map((msg) => {
    if (msg.role !== 'assistant') return msg;
    const aMsg = msg as AssistantMessage;
    if (!aMsg.subagentTasks) return msg;
    let changed = false;
    const newTasks = { ...aMsg.subagentTasks };
    for (const [tcId, task] of Object.entries(newTasks)) {
      if (task.resumeTargetId && steeredAgentIds.has(task.resumeTargetId) && task.action === 'resume') {
        newTasks[tcId] = { ...task, action: 'update' };
        changed = true;
      }
    }
    if (!changed) return msg;
    changedAny = true;
    return { ...aMsg, subagentTasks: newTasks };
  });
  return changedAny ? next : messages;
}

/**
 * Fill a call with its replayed result and settle the card the result answers.
 * Shared by the replay and by an older page, whose calls can get their results
 * from the newer page's turn after them.
 */
export function applyHistoryToolResult({ event, assistantMessageId, pairState, pending, setMessages }: {
  event: SSEEvent;
  assistantMessageId: string;
  pairState: PairState;
  pending: HistoryInterruptInfo[];
  setMessages: HistoryRuntime['setMessages'];
}): void {
  handleHistoryToolCallResult({
    assistantMessageId,
    toolCallId: event.tool_call_id as string,
    result: {
      content: event.content,
      content_type: event.content_type,
      tool_call_id: event.tool_call_id,
      artifact: event.artifact,
      status: event.status,
    },
    pairState,
    setMessages: setMessages as unknown as (
      updater: (prev: Record<string, unknown>[]) => Record<string, unknown>[]
    ) => void,
  });
  if (typeof event.content !== 'string') return;

  // Resolve pending ask_user_question interrupt from tool_call_result
  // (fallback for conversations where hitl_answers wasn't persisted)
  if (event.content.startsWith('User answered:') || event.content.startsWith('User skipped')) {
    const isAnswered = event.content.startsWith('User answered:');
    const answerText = isAnswered ? event.content.replace('User answered: ', '') : null;
    resolvePendingHistoryInterrupt(
      pending,
      (p) => p.type === 'ask_user_question',
      (m) => ({
        bucket: 'userQuestions',
        key: m.questionId!,
        fields: { status: isAnswered ? 'answered' : 'skipped', answer: answerText },
      }),
      setMessages,
    );
  }

  // Resolve pending create_workspace, start_question, ptc_agent, or secretary action interrupt from tool_call_result
  settleProposalFromResult(pending, event.tool_call_id as string | undefined, event.content, setMessages);
}

