import React from 'react';
import type { SubagentTaskRecord } from '@/types/chat';
import SubagentTaskMessageContent, { type ToolCallProcess } from '../SubagentTaskMessageContent';
import WorkflowRunCard from '../WorkflowRunCard';
import { WORKFLOW_TASK_TYPE } from '../../session/subagents/workflowRunState';
import type { SubagentInfo, ToolCallProcessRecord } from './types';

/**
 * The card a `subagent_task` segment renders. A workflow run and a subagent
 * spawn share the segment type and are told apart only here, so the two render
 * paths (block-based and segment-based) enter the choice once instead of each
 * carrying its own copy.
 */
export function TaskSegmentCard({
  subagentId,
  task,
  toolCallProcess,
  onOpen,
  onDetailOpen,
}: {
  subagentId: string;
  /** The message's record for this segment; absent while a card is still
   *  being derived, in which case nothing renders. */
  task: SubagentTaskRecord | undefined;
  toolCallProcess?: ToolCallProcessRecord;
  onOpen?: (info: SubagentInfo) => void;
  /** Opens the raw tool result; only the transcript path offers it. */
  onDetailOpen?: (proc: ToolCallProcessRecord) => void;
}): React.ReactElement | null {
  if (!task) return null;

  if (task.type === WORKFLOW_TASK_TYPE) {
    return (
      <WorkflowRunCard
        subagentId={subagentId}
        description={task.description}
        status={task.status}
        launchReply={task.result}
        onOpen={onOpen}
      />
    );
  }

  // The detail panel shows the task's own outcome, which lives on the card
  // record rather than the tool call — the call settles as soon as the spawn
  // is dispatched, long before the task it started does.
  const enrichedProcess = toolCallProcess
    ? ({ ...toolCallProcess, _subagentStatus: task.status || null } as ToolCallProcess)
    : undefined;

  return (
    <SubagentTaskMessageContent
      subagentId={subagentId}
      description={task.description}
      type={task.type}
      status={task.status}
      action={task.action}
      resumeTargetId={task.resumeTargetId}
      onOpen={onOpen}
      onDetailOpen={onDetailOpen}
      toolCallProcess={enrichedProcess}
    />
  );
}

export default TaskSegmentCard;
