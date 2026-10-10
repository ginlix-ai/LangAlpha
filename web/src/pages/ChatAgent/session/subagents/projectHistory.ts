/**
 * Build per-task transcripts from replayed subagent events and seed the
 * persistent per-task refs so reconnect/resume can append in place.
 *
 * During history replay we deliberately do NOT open floating cards — this
 * only builds per-task message history (cards are created lazily when the
 * user opens subagent details), so the card updater passed to the shared
 * live handlers is a no-op.
 */

import type { AssistantMessage } from '@/types/chat';
import {
  handleSubagentMessageChunk,
  handleSubagentToolCalls,
  handleSubagentToolCallResult,
  handleTaskSteeringAccepted,
  type DeliveredInstruction,
} from './liveEventHandlers';
import { countToolCalls } from './subagentMetrics';
import { createSubagentHistoryStore } from './historyStore';
import {
  type SubagentTokenUsage, ZERO_USAGE, extractTokenUsageDelta, accumulateTokenUsage,
} from '../../utils/tokenUsage';
import {
  DEFAULT_SUBAGENT_TYPE,
  applyWorkflowLifecycle,
  deriveChildIdentity,
  isWorkflowRunTerminal,
  type WorkflowLifecycleFrame,
  type WorkflowRunState,
} from './workflowRunState';
import type { SubagentHistoryData, SubagentHistoryEntry, StreamProcessorRefs, TaskRefs } from '../types';
import type { SubagentRuntime } from '../runtime';

export function projectSubagentHistory(
  rt: Pick<SubagentRuntime, 't' | 'subagentHistory' | 'subagentStateRefsRef'>,
  subagentHistoryByTaskId: Map<string, SubagentHistoryData>,
): void {
  // Built here and published as one snapshot at the end, so the backfill below
  // edits entries nobody else holds yet.
  const projected: Record<string, SubagentHistoryEntry> = {};
  // Workflow runs whose reduced state names their children (label/type/owner);
  // backfilled onto the child entries after the loop — a child's own lane
  // carries anonymous content only, and map iteration order is not chronology.
  const workflowRunsByTaskId = new Map<string, WorkflowRunState>();
  for (const [taskId, subagentHistory] of subagentHistoryByTaskId.entries()) {
    // Create temporary refs structure for processing
    let currentRunIndex = 0;
    // Per-task workflow_lifecycle reducer state (workflow run tasks only).
    let workflowRun: WorkflowRunState | undefined;
    // Per-task token-usage accumulator: backend emits per-call deltas
    // and we sum them into a running total before storing on the
    // SubagentHistoryEntry below.
    let tempTokenUsage: SubagentTokenUsage = ZERO_USAGE;
    const tempSubagentStateRefs: Record<string, TaskRefs> = {
      [taskId]: {
        contentOrderCounterRef: { current: 0 },
        currentReasoningIdRef: { current: null },
        currentToolCallIdRef: { current: null },
        messages: [] as Record<string, unknown>[],
        runIndex: 0,
      },
    };

    // tempRefs matches StreamProcessorRefs; tempSubagentStateRefs is already Record<string, TaskRefs>
    const tempRefs: StreamProcessorRefs = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      subagentStateRefs: tempSubagentStateRefs,
      isReconnect: true, // Suppress Date.now() timestamps so items go straight to accordion zone
    };

    // History-specific no-op updater: prevents floating cards from being
    // created during history load while still letting handlers build
    // the in-memory message structures in tempSubagentStateRefs.
    const historyUpdateSubagentCard = () => {};

    // Process each event in chronological order
    for (let i = 0; i < subagentHistory.events.length; i++) {
      const event = subagentHistory.events[i];
      const eventType = event.event;
      const contentType = event.content_type;

      // Side channel for compaction-middleware LLM output; drop so
      // it does not mingle with the subagent's own messages.
      if (eventType === 'compaction_chunk') {
        continue;
      }

      // Use per-run assistant message ID
      const assistantMessageId = `subagent-${taskId}-assistant-${currentRunIndex}`;

      if (eventType === 'message_chunk' && event.role === 'assistant') {
        handleSubagentMessageChunk({
          taskId,
          assistantMessageId,
          contentType: contentType as string,
          content: event.content as string,
          finishReason: event.finish_reason,
          elapsedMs: typeof event.elapsed_ms === 'number' ? event.elapsed_ms : undefined,
          refs: tempRefs,
          updateSubagentCard: historyUpdateSubagentCard,
        });
      } else if (eventType === 'tool_calls' && event.tool_calls) {
        handleSubagentToolCalls({
          taskId,
          assistantMessageId,
          toolCalls: event.tool_calls as unknown as Record<string, unknown>[],
          refs: tempRefs,
          updateSubagentCard: historyUpdateSubagentCard,
        });
      } else if (eventType === 'tool_call_result') {
        handleSubagentToolCallResult({
          taskId,
          assistantMessageId,
          toolCallId: event.tool_call_id as string,
          result: {
            content: event.content,
            content_type: event.content_type,
            tool_call_id: event.tool_call_id,
            artifact: event.artifact,
            status: event.status,
          },
          refs: tempRefs,
          updateSubagentCard: historyUpdateSubagentCard,
        });
      } else if (eventType === 'subagent_followup_injected' || eventType === 'turn_start') {
        // Legacy subagent_followup_injected had content (steering user message).
        // turn_start was an inter-model-call boundary — no longer emitted,
        // but old persisted data may still contain it. Just extract content.
        if (event.content) {
          handleTaskSteeringAccepted({
            taskId,
            content: event.content as string,
            refs: tempRefs,
            updateSubagentCard: historyUpdateSubagentCard,
          });
          // Sync local run index — handleTaskSteeringAccepted bumps runIndex
          currentRunIndex = tempSubagentStateRefs[taskId].runIndex;
        }
      } else if (eventType === 'steering_delivered') {
        if (event.content) {
          handleTaskSteeringAccepted({
            taskId,
            content: event.content as string,
            entries: event.entries as DeliveredInstruction[] | undefined,
            refs: tempRefs,
            updateSubagentCard: historyUpdateSubagentCard,
          });
          // Sync local run index — handleTaskSteeringAccepted bumps runIndex
          currentRunIndex = tempSubagentStateRefs[taskId].runIndex;
        }
      } else if (eventType === 'user_message') {
        // Run boundary from the wire: the spawn/resume instruction the
        // backend materializes from the task namespace. Same mechanics
        // as a steering follow-up — finalize the previous run's
        // message, render the instruction bubble, open a new run.
        if (event.content) {
          handleTaskSteeringAccepted({
            taskId,
            content: event.content as string,
            refs: tempRefs,
            updateSubagentCard: historyUpdateSubagentCard,
          });
          currentRunIndex = tempSubagentStateRefs[taskId].runIndex;
        }
      } else if (eventType === 'context_window') {
        // Embed notification as content segment in the assistant message
        const action = event.action;
        if (action === 'token_usage') {
          tempTokenUsage = accumulateTokenUsage(tempTokenUsage, extractTokenUsageDelta(event));
        } else {
          let text;
          let detail: string | undefined;
          if (action === 'summarize' && event.signal === 'complete') {
            text = rt.t('chat.compactedNotification', { from: event.original_message_count });
            detail = (event.summary_text as string | undefined) || undefined;
          } else if (action === 'offload' && event.signal === 'complete') {
            const args = event.offloaded_args || 0;
            const reads = event.offloaded_reads || 0;
            if (args > 0 && reads > 0) text = rt.t('chat.offloadedNotification', { args, reads });
            else if (reads > 0) text = rt.t('chat.offloadedReadsNotification', { count: reads });
            else if (args > 0) text = rt.t('chat.offloadedArgsNotification', { count: args });
          }
          if (text) {
            const taskRefsLocal = tempSubagentStateRefs[taskId];
            const order = ++taskRefsLocal.contentOrderCounterRef.current;
            // Find the last assistant message and append notification segment
            const msgIdx = taskRefsLocal.messages.findLastIndex((m) => m.role === 'assistant');
            if (msgIdx !== -1) {
              const taskMsg = taskRefsLocal.messages[msgIdx];
              if (taskMsg.role === 'assistant') {
                const aMsg = taskMsg as AssistantMessage;
                taskRefsLocal.messages[msgIdx] = { ...aMsg, contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order, detail }] };
              }
            }
          }
        }
      } else if (eventType === 'workflow_lifecycle') {
        workflowRun = applyWorkflowLifecycle(
          workflowRun,
          event as unknown as WorkflowLifecycleFrame,
        );
      } else if (eventType === 'provenance') {
        // Table-sourced citation metadata; not part of the transcript.
      } else {
        console.warn('[History] Unhandled subagent event type:', eventType);
      }
    }
    if (workflowRun) workflowRunsByTaskId.set(taskId, workflowRun);
    
    // Get final messages from temp refs
    const rawMessages = tempSubagentStateRefs[taskId]?.messages || [];

    // Finalize messages: set isStreaming=false and close open reasoning/tool
    // processes on the last assistant message so SubagentStatusBar shows 'completed'.
    const finalMessages = rawMessages.map((msg) => {
      if (msg.role !== 'assistant') return msg;
      const aMsg = msg as AssistantMessage;
      // Only finalize the last assistant message (or all, to be safe)
      const m = { ...aMsg, isStreaming: false as const };
      if (m.toolCallProcesses) {
        const procs = { ...m.toolCallProcesses };
        for (const [id, proc] of Object.entries(procs)) {
          if (proc.isInProgress) {
            procs[id] = { ...proc, isInProgress: false, isComplete: true };
          }
        }
        m.toolCallProcesses = procs;
      }
      if (m.reasoningProcesses) {
        const rps = { ...m.reasoningProcesses };
        for (const [id, rp] of Object.entries(rps)) {
          if (rp.isReasoning) {
            rps[id] = { ...rp, isReasoning: false, reasoningComplete: true };
          }
        }
        m.reasoningProcesses = rps;
      }
      return m;
    });

    // Get task metadata from stored history
    const taskMetadata = subagentHistoryByTaskId.get(taskId);

    // Stored so it can be used when the user explicitly opens the subagent
    // card from the main chat view. We do NOT create the floating card here.
    projected[taskId] = {
      taskId,
      description: taskMetadata?.description || '',
      prompt: taskMetadata?.prompt || taskMetadata?.description || '',
      type: taskMetadata?.type || DEFAULT_SUBAGENT_TYPE,
      messages: finalMessages,
      // Prefer the backend-stamped real status. Absent metadata must
      // NOT read as settled — closure is positive-only, so fall back
      // to 'running' and let /status reconciliation settle it.
      status: taskMetadata?.status || 'running',
      error: taskMetadata?.error,
      errorType: taskMetadata?.errorType,
      toolCalls: countToolCalls(finalMessages),
      tokenUsage: tempTokenUsage,
      currentTool: '',
      projectedRunStartedMs: taskMetadata?.projectedRunStartedMs,
      ...(workflowRun ? { workflowRun } : {}),
      ...(taskMetadata?.ownerTaskId ? { ownerTaskId: taskMetadata.ownerTaskId } : {}),
    };

    // Seed persistent subagent state refs from history so that
    // reconnect or future resume can append to the existing messages.
    rt.subagentStateRefsRef.current[taskId] = {
      contentOrderCounterRef: { current: tempSubagentStateRefs[taskId].contentOrderCounterRef.current },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      messages: finalMessages,
      runIndex: currentRunIndex,
      ...(workflowRun ? { workflowRun } : {}),
    };
  }

  // Backfill child identity from the reduced workflow runs: label as the
  // description, the dispatched subagent type, and the owning run's id (the
  // sidebar hides owner-children; the detail drill-in shows the label).
  // Also settle the child's status — a child's own lane carries no lifecycle
  // events, so without this a replayed child reads 'running' forever.
  for (const [wfTaskId, run] of workflowRunsByTaskId.entries()) {
    for (const child of run.children) {
      if (!child.childTaskId) continue;
      const childKey = `task:${child.childTaskId}`;
      const prior = projected[childKey] ?? rt.subagentHistory.get().entries[childKey];
      if (!prior) continue;
      const identity = deriveChildIdentity(child, {
        description: prior.description,
        type: prior.type,
      });
      const childEntry = {
        ...prior,
        description: identity.description,
        type: identity.type,
        ownerTaskId: wfTaskId,
      };
      if (childEntry.status === 'running') {
        const settled = identity.status
          // A settled run with a child never marked done means the child was
          // torn down with the run (cancel/failure).
          || (isWorkflowRunTerminal(run.status) ? 'cancelled' : undefined);
        if (settled) childEntry.status = settled;
      }
      projected[childKey] = childEntry;
    }
  }

  rt.subagentHistory.putEntries(projected);
}

/**
 * Put the runs an older page holds of a task ahead of what its live stream
 * wrote. Replay leaves a running run to its stream and projects each finished
 * run whole at the turn that launched it, so every run on an older page came
 * before anything held for the task. Each run opens an assistant message
 * numbered by run, so the held ones move up by the runs put ahead of them, and
 * the stream goes on writing to the message it was writing to.
 */
export function prependSubagentRuns(
  rt: Pick<
    SubagentRuntime,
    't' | 'subagentHistory' | 'subagentStateRefsRef' | 'subagentTokenUsageRef' | 'updateSubagentCard'
  >,
  taskId: string,
  runs: SubagentHistoryData,
): void {
  const held = rt.subagentStateRefsRef.current[taskId];
  if (!held) {
    projectSubagentHistory(rt, new Map([[taskId, runs]]));
    return;
  }
  // The page's runs alone, projected as replay projects any runs.
  const page = {
    t: rt.t,
    subagentHistory: createSubagentHistoryStore(),
    subagentStateRefsRef: { current: {} as Record<string, TaskRefs> },
  };
  projectSubagentHistory(page, new Map([[taskId, runs]]));
  const front = page.subagentHistory.get().entries[taskId];
  const shift = page.subagentStateRefsRef.current[taskId].runIndex;
  const prefix = `subagent-${taskId}-assistant-`;
  const moved = held.messages.map((m) => {
    const id = String(m.id);
    const run = id.startsWith(prefix) ? Number(id.slice(prefix.length)) : NaN;
    return Number.isInteger(run) ? { ...m, id: `${prefix}${run + shift}` } : m;
  });
  held.messages = [...front.messages, ...moved];
  held.runIndex += shift;

  const prior = rt.subagentHistory.get().entries[taskId];
  const tokenUsage = accumulateTokenUsage(prior?.tokenUsage ?? ZERO_USAGE, front.tokenUsage);
  rt.subagentHistory.putEntries({
    [taskId]: { ...(prior ?? front), messages: held.messages, toolCalls: countToolCalls(held.messages), tokenUsage },
  });
  // The live total was seeded from the entry and counts on from there.
  const live = rt.subagentTokenUsageRef.current[taskId];
  if (live) rt.subagentTokenUsageRef.current[taskId] = accumulateTokenUsage(live, front.tokenUsage);
  rt.updateSubagentCard?.(taskId, {
    messages: held.messages,
    tokenUsage: rt.subagentTokenUsageRef.current[taskId] ?? tokenUsage,
  });
}
