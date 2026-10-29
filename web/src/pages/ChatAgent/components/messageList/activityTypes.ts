export type LiveState = 'active' | 'completing' | 'completed' | 'failed';

export type ToolCallData = {
  args?: Record<string, unknown>;
};

export type ToolCallResultData = {
  content?: unknown;
  artifact?: Record<string, unknown>;
};

interface ActivityIdentity {
  id: string;
  _liveState: LiveState;
}

export interface ReasoningActivityItem extends ActivityIdentity {
  type: 'reasoning';
  content: string;
  reasoningStartedAt?: number;
  reasoningElapsedMs?: number;
}

export interface ToolActivityItem extends ActivityIdentity {
  type: 'tool_call';
  toolCallId: string;
  toolName: string;
  toolCall?: ToolCallData;
  toolCallResult?: ToolCallResultData;
  isComplete?: boolean;
  isFailed?: boolean;
  /** When the call arrived on this page; 1 when a reconnect replayed it, and
   *  absent from history. */
  _createdAt?: number;
  _recentlyCompleted?: boolean;
  _annotationStep?: boolean;
  /** The computer's root, which the row's title, pill and icon read its
   *  paths under. Stamped by the projection that built the row, so every
   *  reader of the row agrees with the filter that kept it. */
  root?: string | null;
}

export type ActivityItem = ReasoningActivityItem | ToolActivityItem;

/** A tool call whose arguments are still streaming. */
export interface PreparingCall {
  /** Which streaming call this is, for as long as it streams: the index the
   *  model gave it plus when it began, since a later call may reuse the index. */
  pendingKey: string;
  /** From the call's first chunk, when the producer sends it there. The call
   *  lands under the same id, so its row carries on rather than replacing. */
  toolCallId?: string;
  toolName?: string;
  argsLength: number;
  /** When its first chunk arrived on this page. */
  firstSeenAt?: number;
}

/** Every call still streaming its arguments, in the order the model wrote them. */
export interface PreparingToolCallData {
  calls: PreparingCall[];
}

/** When a step arrived on this page; undefined for one that came with the
 *  transcript, and for a thought once it has finished. */
export function arrivedAt(item: ActivityItem): number | undefined {
  return item.type === 'reasoning' ? item.reasoningStartedAt : item._createdAt;
}

export function isRunning(item: ActivityItem): boolean {
  return item.type === 'reasoning'
    ? item._liveState === 'active'
    : item._liveState === 'active' && !item.isComplete && !item._recentlyCompleted;
}
