import { isTaskTool } from '../toolDisplayConfig';
import type { ToolCallProcessRecord } from '../ToolCallDetailView';

/**
 * Whether a tool call reads as failed wherever it is named. A Task's own
 * status chip already reports its outcome, so only a plain tool call carries
 * the failure marker, in the tab strip and DetailPanel's mobile header alike.
 */
export function isToolCallFailed(proc: ToolCallProcessRecord): boolean {
  return !isTaskTool(proc.toolName) && proc.isFailed === true;
}
