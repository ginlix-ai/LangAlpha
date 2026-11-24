/**
 * The status read, apart from the rest of the messaging API so a thread link's
 * boot-time prefetch (primedStatus) asks for it without bringing the stream
 * code into the first load.
 */
import { api } from '@/api/client';
import type { WorkflowStatusResponse } from './messages';

/**
 * Get the current status of a workflow for a thread
 * @param {string} threadId - The thread ID to check
 * @param {number} [options.timeout] - Bound the read; the shared API client sets none
 * @returns {Promise<WorkflowStatusResponse>} Workflow status with can_reconnect, status, etc.
 */
export async function getWorkflowStatus(
  threadId: string,
  options: { timeout?: number } = {},
): Promise<WorkflowStatusResponse> {
  if (!threadId) throw new Error('Thread ID is required');
  const { data } = await api.get(`/api/v1/threads/${threadId}/status`, { timeout: options.timeout });
  return data;
}
