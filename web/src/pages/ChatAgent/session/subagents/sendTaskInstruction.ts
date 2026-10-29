/**
 * Sends an instruction the user typed to a running subagent. It shows at once
 * as a pending bubble in the transcript the task's stream writes, and the
 * stream settles it: delivered before the subagent's next model call, or
 * returned when the run ends first. A send that fails takes the bubble back
 * itself, since nothing on the stream may settle it; one that failed only on
 * the way back and is delivered anyway replaces its notice.
 *
 * The bubble is named before the send goes out, and the frames that settle it
 * carry that name: a delivery can land before the POST answers, and the main
 * agent's own follow-ups share the queue, so its text alone cannot say which
 * bubble a frame is about.
 */
import { randomUUID } from '@/lib/randomUUID';
import { apiErrorStatus, sendSubagentMessage } from '../../utils/api';
import { taskIdFromAgentId } from '../../utils/agentId';
import type { SubagentRuntime } from '../runtime';
import { addPendingTaskInstruction, hasPendingInstruction, returnTaskInstruction } from './liveEventHandlers';

export async function sendTaskInstruction(
  rt: Pick<SubagentRuntime, 't' | 'updateSubagentCard' | 'subagentStateRefsRef'>,
  threadId: string,
  agentId: string,
  content: string,
): Promise<void> {
  const taskId = taskIdFromAgentId(agentId);
  if (!taskId) return;
  const inputId = randomUUID();
  addPendingTaskInstruction({
    taskId: agentId,
    inputId,
    content,
    subagentStateRefs: rt.subagentStateRefsRef.current,
    updateSubagentCard: rt.updateSubagentCard,
  });
  try {
    await sendSubagentMessage(threadId, taskId, content, inputId);
  } catch (err) {
    console.error('[sendTaskInstruction] Failed to send subagent instruction:', err);
    // Read after the await, and only while the bubble is still pending: a
    // thread opened since holds other transcripts, and the stream may have
    // returned the instruction first.
    const taskRefs = rt.subagentStateRefsRef.current[agentId];
    if (!rt.updateSubagentCard || !taskRefs || !hasPendingInstruction(taskRefs, inputId)) return;
    // A 409 is the task settling before the instruction got in, so it reads as
    // a return; any other failure says only that it was not sent.
    const notice = apiErrorStatus(err) === 409
      ? 'chat.taskSteeringReturnedNotification'
      : 'chat.taskSteeringNotSentNotification';
    returnTaskInstruction(taskRefs, { inputId, content }, rt.t(notice));
    rt.updateSubagentCard(agentId, { messages: taskRefs.messages });
  }
}
