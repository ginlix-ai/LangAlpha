/**
 * Live-stream interrupt projection: renders the HITL card for an `interrupt`
 * SSE event onto the streaming assistant bubble and arms the pending-interrupt
 * state. Stream-lifecycle effects (loading flag, ownership release) stay with
 * the router; history replay has its own projection in fromHistoryEvent.
 */

import type { AssistantMessage } from '@/types/chat';
import { updateMessage } from '../../hooks/utils/messageHelpers';
import type { SSEEvent, StreamProcessorRefs } from '../types';
import type { StreamRuntime } from '../runtime';

export function projectLiveInterrupt(
  rt: StreamRuntime,
  event: SSEEvent,
  assistantMessageId: string,
  refs: StreamProcessorRefs,
): void {
  const actionRequests = event.action_requests || [];
  const actionType = actionRequests[0]?.type as string | undefined;

  // A still-pending interrupt re-raised after a HITL resume streams into a
  // fresh `assistant-hitl-*` bubble with the same interrupt_id. Suppress the
  // duplicate CARD (the segment push) while keeping the map write + pending
  // tracking below, so the original card stays answerable and the resume's
  // pending set (cleared at resume start) still re-tracks it.
  const interruptAlreadyRendered = event.interrupt_id
    ? rt.renderedInterruptIdsRef.current.has(event.interrupt_id)
    : false;
  if (event.interrupt_id) rt.renderedInterruptIdsRef.current.add(event.interrupt_id);
  // Every branch below MUST push its card segment through this helper so
  // the re-raise suppression can't be forgotten on a future interrupt type.
  const appendCardSegment = (
    segs: AssistantMessage['contentSegments'] | undefined,
    seg: AssistantMessage['contentSegments'][number],
  ): AssistantMessage['contentSegments'] =>
    interruptAlreadyRendered ? (segs || []) : [...(segs || []), seg];

  if (actionType === 'ask_user_question') {
    // --- User question interrupt ---
    const questionId = event.interrupt_id || `question-${Date.now()}`;
    const questionData = actionRequests[0];
    const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

    rt.setMessages((prev) =>
      updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        contentSegments: appendCardSegment(msg.contentSegments, { type: 'user_question', questionId, order }),
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
        isStreaming: false,
      }; })
    );

    rt.pendingInterruptIdsRef.current.add(event.interrupt_id!);
    rt.setPendingInterrupt({
      type: 'ask_user_question',
      interruptId: event.interrupt_id,
      assistantMessageId,
      questionId,
    });
  } else if (actionType === 'create_workspace') {
    // --- Create workspace interrupt ---
    const proposalId = event.interrupt_id || `workspace-${Date.now()}`;
    const proposalData = actionRequests[0];
    const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

    rt.setMessages((prev) =>
      updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        contentSegments: appendCardSegment(msg.contentSegments, { type: 'create_workspace', proposalId, order }),
        workspaceProposals: {
          ...(msg.workspaceProposals || {}),
          [proposalId]: {
            workspace_name: proposalData.workspace_name,
            workspace_description: proposalData.workspace_description,
            interruptId: event.interrupt_id,
            status: 'pending',
          },
        },
        isStreaming: false,
      }; })
    );

    rt.pendingInterruptIdsRef.current.add(event.interrupt_id!);
    rt.setPendingInterrupt({
      type: 'create_workspace',
      interruptId: event.interrupt_id,
      assistantMessageId,
      proposalId,
    });
  } else if (actionType === 'start_question') {
    // --- Start question interrupt ---
    const proposalId = event.interrupt_id || `question-start-${Date.now()}`;
    const proposalData = actionRequests[0];
    const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

    rt.setMessages((prev) =>
      updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        contentSegments: appendCardSegment(msg.contentSegments, { type: 'start_question', proposalId, order }),
        questionProposals: {
          ...(msg.questionProposals || {}),
          [proposalId]: {
            workspace_id: proposalData.workspace_id,
            question: proposalData.question,
            interruptId: event.interrupt_id,
            status: 'pending',
          },
        },
        isStreaming: false,
      }; })
    );

    rt.pendingInterruptIdsRef.current.add(event.interrupt_id!);
    rt.setPendingInterrupt({
      type: 'start_question',
      interruptId: event.interrupt_id,
      assistantMessageId,
      proposalId,
    });
  } else if (actionType === 'ptc_agent') {
    // --- PTC agent interrupt ---
    const proposalId = event.interrupt_id || `ptc-agent-${Date.now()}`;
    const proposalData = actionRequests[0];
    const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

    rt.setMessages((prev) =>
      updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        contentSegments: appendCardSegment(msg.contentSegments, { type: 'ptc_agent' as const, proposalId, order }),
        ptcAgentProposals: {
          ...(msg.ptcAgentProposals || {}),
          [proposalId]: {
            workspace_id: proposalData.workspace_id,
            workspace_name: proposalData.workspace_name,
            question: proposalData.question,
            report_back: proposalData.report_back ?? true,
            interruptId: event.interrupt_id,
            // Persist tool_call_id ON the proposal so the clicked card
            // self-identifies for backfill — never read from
            // `pendingInterrupt`, which N parallel dispatches overwrite.
            tool_call_id: proposalData.tool_call_id,
            status: 'pending',
          },
        },
        isStreaming: false,
      }; })
    );

    rt.pendingInterruptIdsRef.current.add(event.interrupt_id!);
    rt.setPendingInterrupt({
      type: 'ptc_agent',
      interruptId: event.interrupt_id,
      assistantMessageId,
      proposalId,
      toolCallId: proposalData.tool_call_id,
    });
  } else if (actionType === 'delete_workspace' || actionType === 'stop_workspace' || actionType === 'delete_thread') {
    // --- Secretary action interrupt ---
    const proposalId = event.interrupt_id || `secretary-${actionType}-${Date.now()}`;
    const proposalData = actionRequests[0];
    const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

    rt.setMessages((prev) =>
      updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        contentSegments: appendCardSegment(msg.contentSegments, { type: actionType as 'delete_workspace' | 'stop_workspace' | 'delete_thread', proposalId, order }),
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
        isStreaming: false,
      }; })
    );

    rt.pendingInterruptIdsRef.current.add(event.interrupt_id!);
    rt.setPendingInterrupt({
      type: actionType,
      interruptId: event.interrupt_id,
      assistantMessageId,
      proposalId,
    });
  } else {
    // --- Plan approval interrupt (existing) ---
    const planApprovalId = event.interrupt_id || `plan-${Date.now()}`;
    const description =
      actionRequests[0]?.description ||
      (actionRequests[0]?.args?.plan as string) ||
      'No plan description provided.';

    const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

    rt.setMessages((prev) =>
      updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        contentSegments: appendCardSegment(msg.contentSegments, { type: 'plan_approval', planApprovalId, order }),
        planApprovals: {
          ...(msg.planApprovals || {}),
          [planApprovalId]: {
            description,
            interruptId: event.interrupt_id,
            status: 'pending',
          },
        },
        isStreaming: false,
      }; })
    );

    rt.pendingInterruptIdsRef.current.add(event.interrupt_id!);
    rt.setPendingInterrupt({
      interruptId: event.interrupt_id,
      actionRequests: actionRequests,
      threadId: event.thread_id,
      assistantMessageId,
      planApprovalId,
      planMode: actionRequests.some((r) => r.name === 'SubmitPlan') || rt.currentPlanModeRef.current,
    });
  }
}
