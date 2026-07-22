/**
 * History-replay interrupt projection: re-renders the HITL card for a replayed
 * `interrupt` event onto its turn's assistant bubble and queues the pending
 * entry that answer-replay later resolves by interrupt id. Pair bookkeeping
 * stays in replayHistory and is passed in as replay context; the live stream
 * has its own projection in fromLiveEvent.
 */

import type { AssistantMessage } from '@/types/chat';
import { updateMessage } from '../../hooks/utils/messageHelpers';
import type { SSEEvent, PairState, HistoryInterruptInfo } from '../types';
import type { HistoryRuntime } from '../runtime';

export interface HistoryInterruptContext {
  currentActivePairIndex: number | null;
  assistantMessagesByPair: Map<number, string>;
  pairStateByPair: Map<number, PairState>;
  pendingHistoryInterrupts: HistoryInterruptInfo[];
}

export function projectHistoryInterrupt(
  rt: HistoryRuntime,
  event: SSEEvent,
  ctx: HistoryInterruptContext,
): void {
  // Skip a re-raised interrupt: LangGraph re-emits an unanswered
  // interrupt with the same interrupt_id on every resume, landing on a
  // later turn's bubble. The first occurrence owns the card (and its
  // ctx.pendingHistoryInterrupts entry, which answer-replay resolves by id);
  // re-emissions would append a duplicate card and a phantom pending
  // entry, so drop them wholesale.
  if (event.interrupt_id && rt.renderedInterruptIdsRef.current.has(event.interrupt_id)) return;
  const pairIndex = event.turn_index ?? ctx.currentActivePairIndex;
  const interruptAssistantId = pairIndex != null ? ctx.assistantMessagesByPair.get(pairIndex) : null;
  const pairState = pairIndex != null ? ctx.pairStateByPair.get(pairIndex) : null;

  if (interruptAssistantId && pairState) {
    // Mark rendered only once a card will actually attach — a pair-less
    // event must not poison the set and drop a later valid re-raise.
    if (event.interrupt_id) rt.renderedInterruptIdsRef.current.add(event.interrupt_id);
    const actionRequests = event.action_requests || [];
    const actionType = actionRequests[0]?.type as string | undefined;

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
  }
}
