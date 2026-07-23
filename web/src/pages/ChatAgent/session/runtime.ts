/**
 * ChatSessionRuntime — the dependency contract between useChatMessages (the
 * permanent composition root) and the session/ lane modules carved from it.
 *
 * Composition rules:
 * - The runtime is never built once and frozen: `workspaceId`, `t`, and the
 *   card-updater callbacks change across renders. The composition root
 *   rebuilds it per render (a cheap object literal) or keeps dynamic values
 *   behind latest-refs; a memoized-empty-deps runtime is a stale-closure bug.
 * - Lane modules accept their narrow lane port (SubagentRuntime,
 *   HistoryRuntime, StreamRuntime) — a full-ChatSessionRuntime parameter is
 *   allowed only in composition code. Hiding the hook's whole dependency
 *   graph behind one parameter is not decoupling.
 * - session/* never imports hooks/useChatMessages, and never imports back
 *   through the hooks/utils compatibility barrels.
 * - Ports carry the data plane (state, setters, refs, render-current props).
 *   Cross-lane orchestration callbacks (insertNotification, attachSubagentMux,
 *   releaseStreamOwnership, …) join a port only when the carve that settles
 *   their post-move signature lands; RecoveryRuntime joins with the
 *   recovery/ownership seam.
 *
 * Freshness classes (every field is exactly one; keep the labels current):
 * - stable:         identity fixed for the hook's lifetime — setState setters
 *                   and the ref containers themselves.
 * - render-current: captured from the render that built the runtime; stale
 *                   inside long-lived async closures — reread via a ref after
 *                   any await when the current value matters.
 * - ref-current:    read `.current` at use time; always fresh, never renders.
 */

import type React from 'react';
import type {
  MessageRecord, SetMessages, TokenUsage, PendingInterrupt, OffloadBatch, SSEEvent,
  SubagentHistoryEntry, TaskRefs, HistoryInterruptInfo, FallbackSuggestion,
} from './types';
import type { UpdateSubagentCard } from './streamRefs';
import type { SubagentTokenUsage } from '../utils/tokenUsage';
import type { RecentlySentTracker } from '../hooks/utils/recentlySentTracker';
import type { PreviewData } from '../hooks/utils/types';
import type { StructuredError } from '@/utils/rateLimitError';

/** Mutable ref container (matches both useRef cells and hand-built refs). */
type Ref<T> = { current: T };

type Translate = (key: string, opts?: Record<string, unknown>) => string;

/**
 * Subagent lane (carve A): history projection, mux sink/settlement, terminal
 * task outcomes. Owns the subagent-keyed ref containers; consumes the
 * thread/stream identity refs read-only.
 */
export interface SubagentRuntime {
  // render-current
  workspaceId: string;
  t: Translate;
  updateSubagentCard: UpdateSubagentCard | null;
  // stable (setters)
  setMessages: SetMessages;
  setHasActiveSubagents: React.Dispatch<React.SetStateAction<boolean>>;
  setReloadTrigger: React.Dispatch<React.SetStateAction<number>>;
  // stable containers, ref-current reads — owned by this lane
  subagentStateRefsRef: Ref<Record<string, TaskRefs>>;
  subagentHistoryRef: Ref<Record<string, SubagentHistoryEntry>>;
  subagentProcessEventRef: Ref<((event: SSEEvent) => void) | null>;
  subagentTokenUsageRef: Ref<Record<string, SubagentTokenUsage>>;
  terminalTaskOutcomesRef: Ref<Map<string, 'completed' | 'cancelled' | 'error'>>;
  toolCallIdToTaskIdMapRef: Ref<Map<string, string>>;
  historyPendingTaskToolCallIdsRef: Ref<string[]>;
  pendingMuxResyncRef: Ref<boolean>;
  // stable containers, ref-current reads — consumed read-only
  threadIdRef: Ref<string>;
  isStreamingRef: Ref<boolean>;
}

/**
 * History lane (carve B): loadConversationHistory replay. Owns the replay
 * guards (historyLoadingRef / historyLoadedKeyRef move together with the
 * StrictMode guard) and the replay-scoped cursors.
 */
export interface HistoryRuntime {
  // render-current
  workspaceId: string;
  threadId: string;
  messages: MessageRecord[];
  t: Translate;
  updateTodoListCard: ((todoData: Record<string, unknown>, isNew?: boolean) => void) | null;
  // stable (setters)
  setMessages: SetMessages;
  setIsLoadingHistory: React.Dispatch<React.SetStateAction<boolean>>;
  setIsCompacting: React.Dispatch<React.SetStateAction<string | false>>;
  setMessageError: React.Dispatch<React.SetStateAction<string | StructuredError | null>>;
  setFallbackSuggestion: React.Dispatch<React.SetStateAction<FallbackSuggestion | null>>;
  setThreadModels: React.Dispatch<React.SetStateAction<string[]>>;
  setLastThreadModel: React.Dispatch<React.SetStateAction<string | null>>;
  setTokenUsage: React.Dispatch<React.SetStateAction<TokenUsage | null>>;
  setReloadTrigger: React.Dispatch<React.SetStateAction<number>>;
  setThreadId: React.Dispatch<React.SetStateAction<string>>;
  // stable containers, ref-current reads — owned by this lane
  historyLoadingRef: Ref<boolean>;
  replayedRunIdsRef: Ref<string[]>;
  historyLoadedKeyRef: Ref<string | null>;
  historyHasUnresolvedInterruptRef: Ref<boolean>;
  unresolvedHistoryInterruptRef: Ref<HistoryInterruptInfo[]>;
  lastRenderedTurnIndexRef: Ref<number | null>;
  newMessagesStartIndexRef: Ref<number>;
  historyPendingTaskToolCallIdsRef: Ref<string[]>;
  // stable containers, ref-current reads — shared with other lanes
  currentMessageRef: Ref<string | null>;
  lastEventIdRef: Ref<number | string | null>;
  renderedInterruptIdsRef: Ref<Set<string>>;
  toolCallIdToTaskIdMapRef: Ref<Map<string, string>>;
  recentlySentTrackerRef: Ref<RecentlySentTracker>;
  offloadBatchRef: Ref<OffloadBatch>;
}

/**
 * Stream lane (carve C): the live event router (processStreamEvent). Owns the
 * per-turn stream cursors; consumes interrupt bookkeeping and subagent
 * outcome refs shared with the other lanes.
 */
export interface StreamRuntime {
  // render-current
  workspaceId: string;
  threadId: string;
  t: Translate;
  updateSubagentCard: UpdateSubagentCard | null;
  onWorkspaceCreated: ((info: { workspaceId: string; question: string }) => void) | null;
  onFileArtifact: ((event: SSEEvent) => void) | null;
  onPreviewUrl: ((data: PreviewData) => void) | null;
  onOnboardingRelatedToolComplete: (() => void) | null;
  // stable (setters)
  setMessages: SetMessages;
  setIsLoading: React.Dispatch<React.SetStateAction<boolean>>;
  setPendingInterrupt: React.Dispatch<React.SetStateAction<PendingInterrupt | null>>;
  setTokenUsage: React.Dispatch<React.SetStateAction<TokenUsage | null>>;
  setMessageError: React.Dispatch<React.SetStateAction<string | StructuredError | null>>;
  setIsCompacting: React.Dispatch<React.SetStateAction<string | false>>;
  setFallbackSuggestion: React.Dispatch<React.SetStateAction<FallbackSuggestion | null>>;
  setReturnedSteering: React.Dispatch<React.SetStateAction<string | null>>;
  setThreadId: React.Dispatch<React.SetStateAction<string>>;
  setWorkspaceStarting: React.Dispatch<React.SetStateAction<false | 'starting' | 'archived'>>;
  // stable containers, ref-current reads — owned by this lane (per-turn cursors)
  contentOrderCounterRef: Ref<number>;
  currentReasoningIdRef: Ref<string | null>;
  currentToolCallIdRef: Ref<string | null>;
  currentMessageRef: Ref<string | null>;
  currentRunIdRef: Ref<string | null>;
  currentPlanModeRef: Ref<boolean>;
  steeringAtOrderRef: Ref<number | null>;
  pendingPTCBackfillRef: Ref<Map<string, string>>;
  // stable containers, ref-current reads — shared with other lanes
  threadIdRef: Ref<string>;
  lastEventIdRef: Ref<number | string | null>;
  pendingInterruptIdsRef: Ref<Set<string>>;
  renderedInterruptIdsRef: Ref<Set<string>>;
  unresolvedHistoryInterruptRef: Ref<HistoryInterruptInfo[]>;
  terminalTaskOutcomesRef: Ref<Map<string, 'completed' | 'cancelled' | 'error'>>;
  toolCallIdToTaskIdMapRef: Ref<Map<string, string>>;
  subagentHistoryRef: Ref<Record<string, SubagentHistoryEntry>>;
  subagentTokenUsageRef: Ref<Record<string, SubagentTokenUsage>>;
  offloadBatchRef: Ref<OffloadBatch>;
}

/**
 * Recovery/ownership lane (the last carve): stream-ownership contract
 * (isStreamingRef + streamingThreadIdRef mutate only together), reconnect
 * machinery, and shared stream-end cleanup. Owns the ownership and
 * reconnect-bookkeeping refs; consumes the stream cursors and subagent
 * projection refs shared with the other lanes.
 */
export interface RecoveryRuntime {
  // render-current
  updateSubagentCard: UpdateSubagentCard | null;
  // stable (setters)
  setMessages: SetMessages;
  setIsLoading: React.Dispatch<React.SetStateAction<boolean>>;
  setIsReconnecting: React.Dispatch<React.SetStateAction<boolean>>;
  setMessageError: React.Dispatch<React.SetStateAction<string | StructuredError | null>>;
  setWorkspaceStarting: React.Dispatch<React.SetStateAction<false | 'starting' | 'archived'>>;
  setIsCompacting: React.Dispatch<React.SetStateAction<string | false>>;
  setHasActiveSubagents: React.Dispatch<React.SetStateAction<boolean>>;
  setReloadTrigger: React.Dispatch<React.SetStateAction<number>>;
  // stable containers, ref-current reads — owned by this lane
  isStreamingRef: Ref<boolean>;
  streamingThreadIdRef: Ref<string | null>;
  mainStreamAbortRef: Ref<AbortController | null>;
  isReconnectingOwnerRef: Ref<AbortController | null>;
  wasStoppedRef: Ref<boolean>;
  backgroundReconnectRef: Ref<boolean>;
  pendingMuxResyncRef: Ref<boolean>;
  // stable containers, ref-current reads — shared with other lanes
  threadIdRef: Ref<string>;
  currentRunIdRef: Ref<string | null>;
  lastEventIdRef: Ref<number | string | null>;
  currentMessageRef: Ref<string | null>;
  contentOrderCounterRef: Ref<number>;
  currentReasoningIdRef: Ref<string | null>;
  currentToolCallIdRef: Ref<string | null>;
  terminalTaskOutcomesRef: Ref<Map<string, 'completed' | 'cancelled' | 'error'>>;
  unresolvedHistoryInterruptRef: Ref<HistoryInterruptInfo[]>;
  renderedInterruptIdsRef: Ref<Set<string>>;
  subagentHistoryRef: Ref<Record<string, SubagentHistoryEntry>>;
  subagentTokenUsageRef: Ref<Record<string, SubagentTokenUsage>>;
}

/**
 * The full composition-root view. Only useChatMessages builds or holds this;
 * everything under session/ takes one lane port.
 */
export type ChatSessionRuntime = SubagentRuntime & HistoryRuntime & StreamRuntime & RecoveryRuntime;
