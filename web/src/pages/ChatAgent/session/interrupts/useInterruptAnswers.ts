/**
 * The handlers a HITL card calls to answer its interrupt. Most cards carry
 * their own interrupt id and answer through the board, because parallel
 * interrupts must resume together in one batch. The workspace, start-question
 * and secretary cards still answer the single pending slot directly.
 */

import { useCallback, useMemo } from 'react';
import type { HitlDecisionBody, HitlResponseBody, HitlResumeEntry } from '@/types/api';
import type { AssistantMessage, PTCAgentProposalState, ToolApprovalPosition } from '@/types/chat';
import type { ReportBackWatch } from '../../hooks/useReportBackWatch';
import type { PendingInterrupt, SetMessages } from '../types';
import type { AnswerBoard } from './answerBoard';
import { PROPOSAL_DATA_KEY_MAP, setCardFields, setCardStatus } from './buckets';

type ProposalBucket = keyof AssistantMessage & ('workspaceProposals' | 'questionProposals' | 'secretaryActionProposals');

interface InterruptAnswerDeps {
  pendingInterrupt: PendingInterrupt | null;
  setMessages: SetMessages;
  resumeWithHitlResponse: (hitlResponse: HitlResponseBody) => unknown;
  answerBoard: AnswerBoard;
  /** tool_call_id to proposal id, read by the resumed stream to backfill the
   *  dispatched thread onto the approved PTC card. */
  pendingPTCBackfillRef: { current: Map<string, string> };
  armReportBackWatch: ReportBackWatch['arm'];
  threadIdRef: { current: string };
}

export function useInterruptAnswers({
  pendingInterrupt,
  setMessages,
  resumeWithHitlResponse,
  answerBoard,
  pendingPTCBackfillRef,
  armReportBackWatch,
  threadIdRef,
}: InterruptAnswerDeps) {
  // Each handler records its own interrupt's decision on the board, and the
  // resume goes out only once every armed interrupt has one. Reading
  // pendingInterrupt instead (a single slot that N dispatches overwrite) would
  // answer the wrong interrupt and leave the others to re-interrupt. Returns
  // whether a resume went out, since a caller that flipped its card
  // optimistically must otherwise put it back.
  const collectHitlResponseAndMaybeResume = useCallback((
    interruptId: string,
    response: HitlResumeEntry,
  ) => {
    const batch = answerBoard.answer(interruptId, response);
    if (batch) resumeWithHitlResponse(batch);
    return !!batch;
  }, [resumeWithHitlResponse, answerBoard]);

  const handleAnswerQuestion = useCallback((answer: string, questionId: string, interruptId: string) => {
    if (!questionId || !interruptId) return;
    setMessages((prev) => setCardFields(prev, 'userQuestions', questionId, { status: 'answered', answer }));
    collectHitlResponseAndMaybeResume(interruptId, { decisions: [{ type: 'approve', message: answer }] });
  }, [collectHitlResponseAndMaybeResume, setMessages]);

  const handleSkipQuestion = useCallback((questionId: string, interruptId: string) => {
    if (!questionId || !interruptId) return;
    setMessages((prev) => setCardStatus(prev, 'userQuestions', questionId, 'skipped'));
    collectHitlResponseAndMaybeResume(interruptId, { decisions: [{ type: 'reject' }] });
  }, [collectHitlResponseAndMaybeResume, setMessages]);

  // The card names its own bucket because the slot may hold another family's
  // interrupt when several are pending, and a click on one card must never
  // settle a different one (a delete_thread, say).
  const decidePendingProposal = useCallback((bucket: ProposalBucket, verdict: 'approve' | 'reject') => {
    if (!pendingInterrupt || PROPOSAL_DATA_KEY_MAP[pendingInterrupt.type] !== bucket) return;
    const { proposalId, interruptId } = pendingInterrupt;
    setMessages((prev) => setCardStatus(prev, bucket, proposalId!, verdict === 'approve' ? 'approved' : 'rejected'));
    resumeWithHitlResponse({ [interruptId!]: { decisions: [{ type: verdict }] } });
  }, [pendingInterrupt, setMessages, resumeWithHitlResponse]);

  const proposalHandlers = useMemo(() => ({
    handleApproveCreateWorkspace: () => decidePendingProposal('workspaceProposals', 'approve'),
    handleRejectCreateWorkspace: () => decidePendingProposal('workspaceProposals', 'reject'),
    handleApproveStartQuestion: () => decidePendingProposal('questionProposals', 'approve'),
    handleRejectStartQuestion: () => decidePendingProposal('questionProposals', 'reject'),
    handleApproveSecretaryAction: () => decidePendingProposal('secretaryActionProposals', 'approve'),
    handleRejectSecretaryAction: () => decidePendingProposal('secretaryActionProposals', 'reject'),
  }), [decidePendingProposal]);

  // The clicked PTC card supplies its own proposal and interrupt ids, for the
  // same reason the board exists.
  const handleApprovePTCAgent = useCallback((
    proposal?: PTCAgentProposalState,
    overrides?: { report_back?: boolean },
    proposalId?: string,
    interruptId?: string,
  ) => {
    if (!proposalId || !interruptId) return;

    // The resumed stream's tool_call_result backfills the dispatched thread
    // onto this card. The id comes from the card's own proposal data, so it
    // stays right under parallel dispatches.
    const toolCallId = proposal?.tool_call_id;
    if (toolCallId) {
      pendingPTCBackfillRef.current.set(toolCallId, proposalId);
    }

    // Arm the report-back watch at dispatch, not at the dispatch turn's stream
    // end: the wake is pub/sub with no replay, so a fast PTC finishing mid-turn
    // would hit zero subscribers and lose the report-back. Subscribing now
    // latches such a wake (enqueued before the reconcile's isStreamingRef bail)
    // to attach at stream end. No named run exists yet (approval is what
    // dispatches), so no seed and no poke.
    if (overrides?.report_back !== false) {
      armReportBackWatch(threadIdRef.current, null, null);
    }

    setMessages((prev) => setCardStatus(prev, 'ptcAgentProposals', proposalId, 'approved'));
    const decision: HitlDecisionBody & { overrides?: { report_back?: boolean } } = { type: 'approve' };
    if (overrides) decision.overrides = overrides;
    collectHitlResponseAndMaybeResume(interruptId, { decisions: [decision] });
  }, [collectHitlResponseAndMaybeResume, setMessages, pendingPTCBackfillRef, armReportBackWatch, threadIdRef]);

  const handleRejectPTCAgent = useCallback((
    _proposal?: PTCAgentProposalState,
    proposalId?: string,
    interruptId?: string,
  ) => {
    if (!proposalId || !interruptId) return;
    setMessages((prev) => setCardStatus(prev, 'ptcAgentProposals', proposalId, 'rejected'));
    collectHitlResponseAndMaybeResume(interruptId, { decisions: [{ type: 'reject' }] });
  }, [collectHitlResponseAndMaybeResume, setMessages]);

  // Approve is the only decision: the gate's interrupt() discards the resume
  // value and re-checks the verdict itself, so resuming is the answer.
  // Admission re-runs the quota check and can refuse with a 429 that opens no
  // turn at all, so the click only moves the card to `resuming` and the stream
  // settles it either way.
  const handleResumeCreditPause = useCallback((pauseId: string, interruptId: string) => {
    if (!pauseId || !interruptId) return;
    answerBoard.armCreditPause(pauseId);
    const resumed = collectHitlResponseAndMaybeResume(interruptId, { decisions: [{ type: 'approve' }] });
    // Another interrupt in this turn is still unanswered, so nothing was sent
    // and no stream will settle this card. Put it back rather than leave a
    // disabled spinner the user can only clear by reloading.
    if (!resumed) answerBoard.restoreCreditPause();
  }, [collectHitlResponseAndMaybeResume, answerBoard]);

  const settleToolApproval = useCallback((
    approvalId: string,
    interruptId: string,
    position: ToolApprovalPosition,
    click: { approved: boolean; message?: string },
    attemptId?: string,
  ) => {
    if (!approvalId || !interruptId) return;
    const entry = answerBoard.decideToolCall(approvalId, interruptId, position, click, attemptId);
    if (entry) collectHitlResponseAndMaybeResume(interruptId, entry);
  }, [collectHitlResponseAndMaybeResume, answerBoard]);

  const handleApproveToolCall = useCallback((
    approvalId: string, interruptId: string, position: ToolApprovalPosition, attemptId?: string,
  ) => {
    settleToolApproval(approvalId, interruptId, position, { approved: true }, attemptId);
  }, [settleToolApproval]);

  const handleRejectToolCall = useCallback((
    approvalId: string, interruptId: string, position: ToolApprovalPosition, message?: string, attemptId?: string,
  ) => {
    settleToolApproval(approvalId, interruptId, position, { approved: false, message }, attemptId);
  }, [settleToolApproval]);

  return {
    handleAnswerQuestion,
    handleSkipQuestion,
    ...proposalHandlers,
    handleApprovePTCAgent,
    handleRejectPTCAgent,
    handleResumeCreditPause,
    handleApproveToolCall,
    handleRejectToolCall,
  };
}
