/**
 * History-replay interrupt projection: re-renders the HITL card for a replayed
 * `interrupt` event onto its turn's assistant bubble and queues the pending
 * entry that answer-replay later resolves by interrupt id. Pair bookkeeping
 * stays in replayHistory and is passed in as replay context; the live stream
 * has its own projection in fromLiveEvent.
 */

import type { AssistantMessage } from '@/types/chat';
import { updateMessage } from '../../hooks/utils/messageHelpers';
import {
  setCardStatus,
  resolvePendingHistoryInterrupt,
  CARD_BUCKET_FOR_TYPE,
} from './buckets';
import { claimedCardFields, type HistoryInterruptClaim } from './claims';
import { buildCreditPauseState } from './creditPauseCard';
import type { SSEEvent, PairState, HistoryInterruptInfo } from '../types';
import type { HistoryRuntime } from '../runtime';

export interface HistoryInterruptContext {
  currentActivePairIndex: number | null;
  assistantMessagesByPair: Map<number, string>;
  pairStateByPair: Map<number, PairState>;
  pendingHistoryInterrupts: HistoryInterruptInfo[];
  /** What each resume turn recorded about the interrupts it answered, keyed by
   *  interrupt id — including claims that replayed BEFORE the interrupt. */
  claimedInterrupts: Map<string, HistoryInterruptClaim>;
}

export function projectHistoryInterrupt(
  rt: HistoryRuntime,
  event: SSEEvent,
  ctx: HistoryInterruptContext,
): void {
  const actionRequests = event.action_requests || [];
  const actionType = actionRequests[0]?.type as string | undefined;
  const pairIndex = event.turn_index ?? ctx.currentActivePairIndex;
  const interruptAssistantId = pairIndex != null ? ctx.assistantMessagesByPair.get(pairIndex) : null;
  const pairState = pairIndex != null ? ctx.pairStateByPair.get(pairIndex) : null;

  // Skip a re-raised interrupt: LangGraph re-emits an unanswered
  // interrupt with the same interrupt_id on every resume, landing on a
  // later turn's bubble. The first occurrence owns the card (and its
  // ctx.pendingHistoryInterrupts entry, which answer-replay resolves by id);
  // re-emissions would append a duplicate card and a phantom pending
  // entry, so drop them wholesale.
  if (event.interrupt_id && rt.renderedInterruptIdsRef.current.has(event.interrupt_id)) {
    // A credit pause is the exception, because it is the one interrupt resolved
    // from the resume turn's `hitl_interrupt_ids` — stamped when the resume is
    // requested, not when the graph consumes it. A re-raise is proof it never
    // was, and it replays after that stamp, so it is the later truth: put the
    // card back and re-queue the entry that arms the Resume button. Otherwise
    // the pause replays answered with nothing left to answer it.
    // ...unless this is the branch tip replayed a second time, for an answer a
    // resume that is STILL RUNNING already gave. A resume's `__interrupt__` and
    // `__resume__` writes ride one checkpoint, and the tip the reader is handed
    // only advances at finalize, so for the length of the run that one
    // checkpoint is both the boundary ending the turn and the tip: the same
    // interrupt replays inline and again at the tail. Re-arming from that
    // second copy undoes the settle the resume turn just earned.
    //
    // Two things have to hold together, because either alone is a real
    // re-raise. A tip copy carries no turn_index (only a boundary's interrupts
    // are enriched with one), so an occurrence that HAS one rode a genuine
    // later boundary and still re-arms. And a claim is recorded only for a
    // resume with no run_id, so a tip trailing a terminal turn -- the resume
    // that failed to consume its Command -- finds none and re-arms too.
    if (
      event.interrupt_id
      && event.turn_index == null
      && ctx.claimedInterrupts.has(event.interrupt_id)
    ) {
      return;
    }
    if (actionType === 'credit_pause' && interruptAssistantId) {
      const proposalId = event.interrupt_id;
      rt.setMessages((prev) => setCardStatus(prev, 'creditPauses', proposalId, 'pending'));
      if (!ctx.pendingHistoryInterrupts.some((p) => p.interruptId === proposalId)) {
        ctx.pendingHistoryInterrupts.push({
          type: 'credit_pause',
          assistantMessageId: interruptAssistantId,
          proposalId,
          interruptId: proposalId,
        });
      }
    }
    return;
  }

  if (interruptAssistantId && pairState) {
    // Mark rendered only once a card will actually attach — a pair-less
    // event must not poison the set and drop a later valid re-raise.
    if (event.interrupt_id) rt.renderedInterruptIdsRef.current.add(event.interrupt_id);

    if (actionType === 'ask_user_question') {
      // --- User question interrupt (history) ---
      const questionId = event.interrupt_id || `question-history-${Date.now()}`;
      const questionData = actionRequests[0];
      const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

      rt.setMessages((prev) =>
        updateMessage(prev,interruptAssistantId, (m) => {
          if (m.role !== 'assistant') return m;
          const msg = m as AssistantMessage;
          return {
            ...msg,
            contentSegments: [...(msg.contentSegments || []), { type: 'user_question' as const, questionId, order }],
            userQuestions: {
              ...(msg.userQuestions || {}),
              [questionId]: {
                question: questionData.question,
                options: questionData.options || [],
                allow_multiple: questionData.allow_multiple || false,
                interruptId: event.interrupt_id,
                status: 'pending',
                answer: null,
              },
            },
          };
        })
      );

      ctx.pendingHistoryInterrupts.push({
        type: 'ask_user_question',
        assistantMessageId: interruptAssistantId,
        questionId,
        interruptId: event.interrupt_id,
        answer: null,
      });
    } else if (actionType === 'create_workspace') {
      // --- Create workspace interrupt (history) ---
      const proposalId = event.interrupt_id || `workspace-history-${Date.now()}`;
      const proposalData = actionRequests[0];
      const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

      rt.setMessages((prev) =>
        updateMessage(prev,interruptAssistantId, (m) => {
          if (m.role !== 'assistant') return m;
          const msg = m as AssistantMessage;
          return {
            ...msg,
            contentSegments: [...(msg.contentSegments || []), { type: 'create_workspace' as const, proposalId, order }],
            workspaceProposals: {
              ...(msg.workspaceProposals || {}),
              [proposalId]: {
                workspace_name: proposalData.workspace_name,
                workspace_description: proposalData.workspace_description,
                interruptId: event.interrupt_id,
                status: 'pending',
              },
            },
          };
        })
      );

      ctx.pendingHistoryInterrupts.push({
        type: 'create_workspace',
        assistantMessageId: interruptAssistantId,
        proposalId,
        interruptId: event.interrupt_id,
      });
    } else if (actionType === 'start_question') {
      // --- Start question interrupt (history) ---
      const proposalId = event.interrupt_id || `question-start-history-${Date.now()}`;
      const proposalData = actionRequests[0];
      const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

      rt.setMessages((prev) =>
        updateMessage(prev,interruptAssistantId, (m) => {
          if (m.role !== 'assistant') return m;
          const msg = m as AssistantMessage;
          return {
            ...msg,
            contentSegments: [...(msg.contentSegments || []), { type: 'start_question' as const, proposalId, order }],
            questionProposals: {
              ...(msg.questionProposals || {}),
              [proposalId]: {
                workspace_id: proposalData.workspace_id,
                question: proposalData.question,
                interruptId: event.interrupt_id,
                status: 'pending',
              },
            },
          };
        })
      );

      ctx.pendingHistoryInterrupts.push({
        type: 'start_question',
        assistantMessageId: interruptAssistantId,
        proposalId,
        interruptId: event.interrupt_id,
      });
    } else if (actionType === 'ptc_agent') {
      // --- PTC agent interrupt (history) ---
      const proposalId = event.interrupt_id || `ptc-agent-history-${Date.now()}`;
      const proposalData = actionRequests[0];
      const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

      rt.setMessages((prev) =>
        updateMessage(prev,interruptAssistantId, (m) => {
          if (m.role !== 'assistant') return m;
          const msg = m as AssistantMessage;
          return {
            ...msg,
            contentSegments: [...(msg.contentSegments || []), { type: 'ptc_agent' as const, proposalId, order }],
            ptcAgentProposals: {
              ...(msg.ptcAgentProposals || {}),
              [proposalId]: {
                workspace_id: proposalData.workspace_id,
                workspace_name: proposalData.workspace_name,
                question: proposalData.question,
                report_back: proposalData.report_back ?? true,
                interruptId: event.interrupt_id,
                tool_call_id: proposalData.tool_call_id,
                status: 'pending',
              },
            },
          };
        })
      );

      ctx.pendingHistoryInterrupts.push({
        type: 'ptc_agent',
        assistantMessageId: interruptAssistantId,
        proposalId,
        interruptId: event.interrupt_id,
      });
    } else if (actionType === 'delete_workspace' || actionType === 'stop_workspace' || actionType === 'delete_thread') {
      // --- Secretary action interrupt (history) ---
      const proposalId = event.interrupt_id || `secretary-${actionType}-history-${Date.now()}`;
      const proposalData = actionRequests[0];
      const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

      rt.setMessages((prev) =>
        updateMessage(prev,interruptAssistantId, (m) => {
          if (m.role !== 'assistant') return m;
          const msg = m as AssistantMessage;
          return {
            ...msg,
            contentSegments: [...(msg.contentSegments || []), { type: actionType as 'delete_workspace' | 'stop_workspace' | 'delete_thread', proposalId, order }],
            secretaryActionProposals: {
              ...(msg.secretaryActionProposals || {}),
              [proposalId]: {
                actionType: actionType as 'delete_workspace' | 'stop_workspace' | 'delete_thread',
                workspace_id: proposalData.workspace_id,
                thread_id: proposalData.thread_id,
                interruptId: event.interrupt_id,
                status: 'pending',
              },
            },
          };
        })
      );

      ctx.pendingHistoryInterrupts.push({
        type: actionType,
        assistantMessageId: interruptAssistantId,
        proposalId,
        interruptId: event.interrupt_id,
      });
    } else if (actionType === 'credit_pause') {
      // --- Credit pause interrupt (history) ---
      const proposalId = event.interrupt_id!;
      const pauseState = buildCreditPauseState(actionRequests[0], event.interrupt_id!);
      const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

      rt.setMessages((prev) =>
        updateMessage(prev,interruptAssistantId, (m) => {
          if (m.role !== 'assistant') return m;
          const msg = m as AssistantMessage;
          return {
            ...msg,
            contentSegments: [...(msg.contentSegments || []), { type: 'credit_pause' as const, proposalId, order }],
            creditPauses: { ...(msg.creditPauses || {}), [proposalId]: pauseState },
          };
        })
      );

      ctx.pendingHistoryInterrupts.push({
        type: 'credit_pause',
        assistantMessageId: interruptAssistantId,
        proposalId,
        interruptId: event.interrupt_id,
      });
    } else {
      // --- Plan approval interrupt (existing) ---
      const planApprovalId = event.interrupt_id || `plan-history-${Date.now()}`;
      const description =
        (actionRequests[0]?.description as string) ||
        (actionRequests[0]?.args?.plan as string) ||
        'No plan description provided.';
      const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

      rt.setMessages((prev) =>
        updateMessage(prev,interruptAssistantId, (m) => {
          if (m.role !== 'assistant') return m;
          const msg = m as AssistantMessage;
          return {
            ...msg,
            contentSegments: [...(msg.contentSegments || []), { type: 'plan_approval' as const, planApprovalId, order }],
            planApprovals: {
              ...(msg.planApprovals || {}),
              [planApprovalId]: {
                description,
                interruptId: event.interrupt_id,
                status: 'pending',
              },
            },
          };
        })
      );

      ctx.pendingHistoryInterrupts.push({
        type: 'plan_approval',
        assistantMessageId: interruptAssistantId,
        planApprovalId,
        interruptId: event.interrupt_id,
      });
    }

    // The card just rendered, and a still-running resume turn earlier in this
    // replay already said it was answered. Position is not evidence: the tip
    // interrupt is appended once at the end of a checkpoint replay with no
    // turn_index, and a resume commits the boundary that would place it only
    // when the graph consumes the Command — so on a reload taken inside that
    // window the interrupt replays AFTER the claim that settles it, and the
    // resolvers above found nothing to settle. Left pending, the card offers
    // live Approve/Reject/Resume on a turn that is already running again.
    // Claims are recorded only for a live resume, so an interrupt trailing a
    // terminal turn — a genuine re-raise, or one a failed resume never took —
    // finds none here and keeps its controls.
    const cardId = event.interrupt_id;
    const claim = cardId ? ctx.claimedInterrupts.get(cardId) : undefined;
    if (claim && cardId) {
      // The branch above queued this entry against the same bubble it rendered
      // the card on, so the ordinary resolver settles it: patch the card, drop
      // the entry, and what survives replay stays exactly what the user still
      // owes an answer. A claim that does not prove an outcome patches nothing
      // and leaves the entry queued.
      const settled = resolvePendingHistoryInterrupt(
        ctx.pendingHistoryInterrupts,
        (p) => p.interruptId === cardId,
        (m) => {
          const bucket = CARD_BUCKET_FOR_TYPE[m.type];
          const fields = claimedCardFields(m.type, claim);
          return bucket && fields ? { bucket, key: cardId, fields } : null;
        },
        rt.setMessages,
      );
      // A settled card is a record, not a control, so it no longer satisfies
      // what the rendered set means to the live path: "a card for this id is
      // already on screen and can still be answered". Release the id, or a
      // re-raise arriving on the reconnect stream is suppressed as a duplicate
      // and lands nowhere. Dropping the pending entry is what took this card
      // out of the reconnect strip list that would otherwise have released it.
      if (settled) rt.renderedInterruptIdsRef.current.delete(cardId);
    }
  }
}
