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
} from './liveEventHandlers';
import { countToolCalls } from './subagentMetrics';
import {
  type SubagentTokenUsage, ZERO_USAGE, extractTokenUsageDelta, accumulateTokenUsage,
} from '../../utils/tokenUsage';
import type { SubagentHistoryData, StreamProcessorRefs, TaskRefs } from '../types';
import type { SubagentRuntime } from '../runtime';

export function projectSubagentHistory(
  rt: SubagentRuntime,
  subagentHistoryByTaskId: Map<string, SubagentHistoryData>,
): void {
  for (const [taskId, subagentHistory] of subagentHistoryByTaskId.entries()) {
    // Create temporary refs structure for processing
    let currentRunIndex = 0;
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
                const aMsg = taskMsg as unknown as AssistantMessage;
                taskRefsLocal.messages[msgIdx] = { ...aMsg, contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order, detail }] } as unknown as Record<string, unknown>;
              }
            }
          }
        }
      } else {
        console.warn('[History] Unhandled subagent event type:', eventType);
      }
    }
    
    // Get final messages from temp refs
    const rawMessages = tempSubagentStateRefs[taskId]?.messages || [];

    // Finalize messages: set isStreaming=false and close open reasoning/tool
    // processes on the last assistant message so SubagentStatusBar shows 'completed'.
    const finalMessages = rawMessages.map((msg) => {
      if (msg.role !== 'assistant') return msg;
      const aMsg = msg as unknown as AssistantMessage;
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

    // Store history in ref so it can be used when the user explicitly
    // opens the subagent card from the main chat view. We do NOT
    // create the floating card here.
    if (!rt.subagentHistoryRef.current) {
      rt.subagentHistoryRef.current = {};
    }
    rt.subagentHistoryRef.current[taskId] = {
      taskId,
      description: taskMetadata?.description || '',
      prompt: taskMetadata?.prompt || taskMetadata?.description || '',
      type: taskMetadata?.type || 'general-purpose',
      messages: finalMessages,
      // Prefer the backend-stamped real status. Absent metadata must
      // NOT read as settled — closure is positive-only, so fall back
      // to 'running' and let /status reconciliation settle it.
      status: taskMetadata?.status || 'running',
      error: taskMetadata?.error,
      toolCalls: countToolCalls(finalMessages),
      tokenUsage: tempTokenUsage,
      currentTool: '',
      projectedRunStartedMs: taskMetadata?.projectedRunStartedMs,
    };

    // Seed persistent subagent state refs from history so that
    // reconnect or future resume can append to the existing messages.
    rt.subagentStateRefsRef.current[taskId] = {
      contentOrderCounterRef: { current: tempSubagentStateRefs[taskId].contentOrderCounterRef.current },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      messages: finalMessages,
      runIndex: currentRunIndex,
    };
  }
}
