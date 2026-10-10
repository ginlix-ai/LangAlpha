/**
 * Older pages of a long thread, put above the transcript as the reader scrolls
 * up. Each replays into a transcript of its own (loadOlderHistoryPage) and
 * joins the one on screen only when committed (commitOlderHistoryPage), so a
 * page that something overtook on its way changes nothing.
 */
import type { AssistantMessage, ChatMessage } from '@/types/chat';
import { createRecentlySentTracker } from '../../hooks/utils/recentlySentTracker';
import { flushOffloadBatch } from '../../hooks/utils/contextWindowEvent';
import type { SSEEvent, SubagentHistoryData, TokenUsage } from '../types';
import { ownerOfToolCall } from '../toolCallOwner';
import type { SubagentTerminalStatus } from '../subagents/subagentStatus';
import type { HistoryRuntime } from '../runtime';
import { loadConversationHistory } from './replayHistory';
import { applyHistoryToolResult, markSteeredResumeCards, settleResumedInterrupts } from './resumeSettlement';

/** Where a replayed page starts, and whether older turns remain. A page
 *  that holds no turn is no page (null): there is nothing to page back from. */
export interface HistoryPageInfo {
  firstTurnIndex: number;
  hasMore: boolean;
}

/**
 * What a page leaves for the page before it. Replay settles an interrupt from
 * the resume turn after it, and a gated call gets its result in that same
 * turn, so when that turn opens a page, the card and the call are on the page
 * before and only it can apply them.
 */
export interface HistoryCarry {
  /** Resume user_messages and results whose call the page did not hold, in
   *  replay order. */
  events: SSEEvent[];
  /** Each task's replayed events across every loaded page, oldest first, so
   *  a task spanning a page boundary is projected whole. */
  subagents: Map<string, SubagentHistoryData>;
  /** Tasks a loaded page saw steered (their resume cards read "Updated"). */
  steeredAgentIds: Set<string>;
}

export interface LoadedHistoryPage extends HistoryPageInfo {
  /** The newest turn the page holds. */
  lastTurnIndex: number;
  carry: HistoryCarry;
}

/** An older page, replayed but not yet on screen. */
export interface OlderHistoryPage {
  /** The page's bubbles, oldest first, to go above the transcript. */
  messages: ChatMessage[];
  page: LoadedHistoryPage;
  /** Interrupt ids the page's cards claimed, for the thread-scoped dedup set. */
  renderedInterruptIds: Set<string>;
  /** Models the page's turns ran on. */
  threadModels: string[];
  /** Tasks the page touched, merged with what newer pages held of them. */
  subagents: Map<string, SubagentHistoryData>;
  /** The runs the page alone holds of each task it touched. */
  ownSubagents: Map<string, SubagentHistoryData>;
  /** Tokens the page's model calls read and wrote, for the thread's totals. */
  tokens: { input: number; output: number };
}

/** Newer page's knowledge of a task folded over an older page's. */
function mergeSubagentHistory(older: SubagentHistoryData, newer: SubagentHistoryData): SubagentHistoryData {
  // The newest status is the task's status, with the failure it arrived with.
  const settled = newer.status ? newer : older;
  return {
    ...older,
    ...newer,
    messages: [],
    events: [...older.events, ...newer.events],
    description: newer.description || older.description,
    prompt: newer.prompt || older.prompt,
    type: newer.type || older.type,
    status: settled.status,
    error: settled.error,
    errorType: settled.errorType,
    projectedRunStartedMs:
      newer.projectedRunStartedMs != null && older.projectedRunStartedMs != null
        ? Math.max(newer.projectedRunStartedMs, older.projectedRunStartedMs)
        : (newer.projectedRunStartedMs ?? older.projectedRunStartedMs),
  };
}

/**
 * Replay the page of turns before `beforeTurn` into a private transcript.
 * Nothing on screen changes until commitOlderHistoryPage: the caller checks
 * the page still belongs (no reload, thread switch or fork since it started)
 * and drops it otherwise. Resolves null when the fetch was aborted.
 *
 * Only the transcript and the page's share of the token totals come from an
 * older page. The rest of the thread-level state a replay sets (the context
 * window and last output, the fallback suggestion, the todo card, the
 * rendered-turn watermark, the replayed run ids, a paused interrupt) belongs
 * to the newest turns, which the newest page already set.
 */
export async function loadOlderHistoryPage(
  rt: HistoryRuntime,
  request: { threadId: string; beforeTurn: number; limit: number; signal: AbortSignal },
  carry: HistoryCarry,
): Promise<OlderHistoryPage | null> {
  let local: ChatMessage[] = [];
  let models: string[] = [];
  const usage: { current: TokenUsage | null } = { current: null };
  const noop = () => {};
  const renderedInterruptIds = new Set(rt.renderedInterruptIdsRef.current);
  // Every field named, so a field HistoryRuntime gains is a decision here
  // rather than a write to the live session.
  const pageRt: HistoryRuntime = {
    workspaceId: rt.workspaceId,
    threadId: request.threadId,
    t: rt.t,
    updateTodoListCard: null,
    setMessages: (next) => {
      local = typeof next === 'function' ? next(local) : next;
    },
    setIsLoadingHistory: noop,
    setHistoryLoadFailed: noop,
    setIsCompacting: noop,
    setMessageError: noop,
    setFallbackSuggestion: noop,
    setThreadModels: (next) => {
      models = typeof next === 'function' ? next(models) : next;
    },
    setTokenUsage: (next) => {
      usage.current = typeof next === 'function' ? next(usage.current) : next;
    },
    setReloadTrigger: noop,
    setThreadId: noop,
    historyLoadingRef: { current: false },
    replayedRunIdsRef: { current: [] },
    historyLoadedKeyRef: { current: null },
    historyHasUnresolvedInterruptRef: { current: false },
    unresolvedHistoryInterruptRef: { current: [] },
    lastRenderedTurnIndexRef: { current: null },
    newMessagesStartIndexRef: { current: 0 },
    historyPendingTaskToolCallIdsRef: { current: [] },
    // No turn on an older page is one this view just sent or is streaming.
    currentMessageRef: { current: null },
    recentlySentTrackerRef: { current: createRecentlySentTracker() },
    lastEventIdRef: { current: null },
    // A copy: a re-raise the newer pages already carded stays a re-raise, and
    // a discarded page must not mark ids it never put on screen.
    renderedInterruptIdsRef: { current: renderedInterruptIds },
    // Task ids by tool call, which are the same whichever page reads them.
    subagentHistory: rt.subagentHistory,
    offloadBatchRef: { current: { args: 0, reads: 0, timer: null } },
  };

  // The newest page's load, run against the runtime above. Undefined until it
  // reports, which an aborted fetch never does.
  const loaded: { page?: LoadedHistoryPage | null } = {};
  try {
    await loadConversationHistory(pageRt, {
      applyFallbackSuggestion: noop,
      loadFeedback: async () => {},
      projectSubagentHistory: noop,
      beforeTurn: request.beforeTurn,
      limit: request.limit,
      signal: request.signal,
      onPage: (page) => {
        loaded.page = page;
      },
    });
  } finally {
    // The one write a replay defers (an offload notice, debounced), made now
    // so the page is whole when it is returned.
    flushOffloadBatch(pageRt.offloadBatchRef);
  }
  if (loaded.page === undefined || request.signal.aborted) return null;
  const own = loaded.page;
  // A page with no turn has nothing to hand on.
  const ownCarry: HistoryCarry = own?.carry ?? { events: [], subagents: new Map(), steeredAgentIds: new Set() };

  // The resumes and results the newer pages left for this one, in replay
  // order: a resume settles this page's cards, a result fills its call.
  const leftover: SSEEvent[] = [];
  // The cards the page left pending.
  const pending = pageRt.unresolvedHistoryInterruptRef.current;
  for (const event of carry.events) {
    if (event.event === 'user_message') {
      settleResumedInterrupts(pageRt, pending, event);
      continue;
    }
    const toolCallId = event.tool_call_id as string | undefined;
    if (!toolCallId || !ownerOfToolCall(local, toolCallId)) {
      leftover.push(event);
      continue;
    }
    applyHistoryToolResult({
      event,
      assistantMessageId: '',
      pairState: { contentOrderCounter: 0, reasoningId: null, toolCallId: null, steeringBatches: 0 },
      pending,
      setMessages: pageRt.setMessages,
    });
  }
  // Whatever is still pending was never answered, and only the newest page
  // can hold the interrupt the thread is paused on: these stay as replayed.

  const steeredAgentIds = new Set([...ownCarry.steeredAgentIds, ...carry.steeredAgentIds]);

  const subagents = new Map<string, SubagentHistoryData>();
  for (const [taskId, older] of ownCarry.subagents) {
    const newer = carry.subagents.get(taskId);
    subagents.set(taskId, newer ? mergeSubagentHistory(older, newer) : older);
  }
  const allSubagents = new Map(carry.subagents);
  for (const [taskId, merged] of subagents) allSubagents.set(taskId, merged);

  // A page that does not start before the one it extends would be asked for
  // again forever: it is the start of the thread.
  const advances = own !== null && own.firstTurnIndex < request.beforeTurn;
  return {
    messages: local,
    page: {
      firstTurnIndex: advances ? own.firstTurnIndex : request.beforeTurn,
      lastTurnIndex: advances ? own.lastTurnIndex : request.beforeTurn - 1,
      hasMore: advances && own.hasMore,
      carry: {
        events: [...ownCarry.events, ...leftover],
        subagents: allSubagents,
        steeredAgentIds,
      },
    },
    renderedInterruptIds,
    threadModels: models,
    subagents,
    ownSubagents: ownCarry.subagents,
    tokens: { input: usage.current?.totalInput ?? 0, output: usage.current?.totalOutput ?? 0 },
  };
}

/** The page's chips still reading running for a task whose live stream has
 *  since closed, stamped with the outcome it closed with. */
function settleClosedTaskChips(
  rt: HistoryRuntime,
  messages: ChatMessage[],
  liveOutcome: (agentId: string) => SubagentTerminalStatus | undefined,
): ChatMessage[] {
  return messages.map((msg) => {
    const tasks = msg.role === 'assistant' ? (msg as AssistantMessage).subagentTasks : undefined;
    if (!tasks) return msg;
    let settled: typeof tasks | null = null;
    for (const [toolCallId, task] of Object.entries(tasks)) {
      const agentId = rt.subagentHistory.toolCalls.get(toolCallId);
      const outcome = agentId ? liveOutcome(agentId) : undefined;
      if (outcome && task.status === 'running') {
        settled ??= { ...tasks };
        settled[toolCallId] = { ...task, status: outcome };
      }
    }
    return settled ? { ...msg, subagentTasks: settled } : msg;
  });
}

/**
 * Put an older page above the transcript. Everything on screen keeps its
 * place in the array relative to everything else, and the splice point for
 * history moves down with it.
 */
export function commitOlderHistoryPage(
  rt: HistoryRuntime,
  deps: {
    projectSubagentHistory: (byTaskId: Map<string, SubagentHistoryData>) => void;
    /** Puts a task's runs ahead of what its live stream wrote. */
    prependSubagentRuns: (agentId: string, runs: SubagentHistoryData) => void;
    /** Tasks a live stream is still writing, whose state is the stream's. */
    isTaskLive: (agentId: string) => boolean;
    /** The outcome a task's live stream closed with since the history was last
     *  reset. The page and its carry may hold the task from before it. */
    liveOutcome: (agentId: string) => SubagentTerminalStatus | undefined;
  },
  older: OlderHistoryPage,
): void {
  const steered = older.page.carry.steeredAgentIds;
  const messages = settleClosedTaskChips(rt, older.messages, deps.liveOutcome);
  rt.setMessages((prev) => {
    rt.newMessagesStartIndexRef.current += messages.length;
    return markSteeredResumeCards([...messages, ...prev], steered);
  });
  for (const id of older.renderedInterruptIds) rt.renderedInterruptIdsRef.current.add(id);
  const { input, output } = older.tokens;
  if (input > 0 || output > 0) {
    // The totals are the thread's, so the page's calls join them. The window
    // and the last output stay the newest call's; with no newest call there is
    // no window to show them against.
    rt.setTokenUsage((prev) =>
      prev && {
        ...prev,
        totalInput: prev.totalInput + input,
        totalOutput: prev.totalOutput + output,
      },
    );
  }
  if (older.threadModels.length > 0) {
    // Older turns first, so the list stays in the order the thread used them.
    rt.setThreadModels((prev) => [...new Set([...older.threadModels, ...prev])]);
  }
  // A task a live stream wrote to since the history loaded keeps what it
  // wrote, with the page's runs ahead of it; every other task is projected
  // whole again.
  const projectable = new Map<string, SubagentHistoryData>();
  for (const [agentId, merged] of older.subagents) {
    const own = older.ownSubagents.get(agentId);
    if (own && (deps.isTaskLive(agentId) || deps.liveOutcome(agentId))) deps.prependSubagentRuns(agentId, own);
    else projectable.set(agentId, merged);
  }
  if (projectable.size > 0) deps.projectSubagentHistory(projectable);
}
