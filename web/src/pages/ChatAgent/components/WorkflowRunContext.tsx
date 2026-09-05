import { createContext, useContext } from 'react';
import type { WorkflowRunState } from '../session/subagents/workflowRunState';

export type WorkflowRunResolver = (subagentId: string) => WorkflowRunState | undefined;

// Consumed at the leaf (WorkflowRunCard) so live workflow_lifecycle ticks
// re-render the card without busting the memoized MessageBubble /
// MessageContentSegments trees. Same pattern as SubagentTelemetryContext.
export const WorkflowRunContext = createContext<WorkflowRunResolver | null>(null);

export function useWorkflowRun(subagentId: string | undefined): WorkflowRunState | undefined {
  const resolver = useContext(WorkflowRunContext);
  if (!resolver || !subagentId) return undefined;
  return resolver(subagentId);
}
