/**
 * Subagent mux settlement: the thread-level sink for the v2 mux and its
 * positive-closure contract. Chips and cards advance to terminal ONLY from
 * per-task closure (run_end / ledger truth) — never from channel absence,
 * and never via a sweep over siblings.
 */

import type { AssistantMessage, SubagentTask } from '@/types/chat';
import { getThreadMux, type ThreadMuxSink } from '../stream/threadStreamMux';
import { isTerminalStatus, normalizeWireStatus } from './subagentStatus';
import type { SSEEvent } from '../types';
import type { SubagentRuntime } from '../runtime';

/**
 * Helper to get taskId from event.
 * Routes subagent events to the correct task based on agent ID mapping.
 * Defined at hook level so it can be shared between handleSendMessage and reconnectToStream.
 */
export function getTaskIdFromEvent(event: SSEEvent): string | null {
  // With task:{task_id} format, the task ID is embedded in the agent field.
  // e.g., agent = "task:pkyRHQ" → taskId = "task:pkyRHQ"
  // This is the agent_id used as key throughout the frontend.
  const agent = event?.agent;
  if (!agent || typeof agent !== 'string' || !agent.startsWith('task:')) {
    if (import.meta.env.DEV) {
      console.warn('[Stream] Subagent event without task: agent field:', event);
    }
    return null;
  }
  return agent;
}

export interface SubagentMuxDeps {
  /** Short ids of tasks with an open channel on the thread's v2 mux. */
  muxOpenTaskIds: () => Set<string>;
  /** useReportBackWatch's high-level arm (mark awaiting + keyed watch + optional poke). */
  armReportBackWatch: (
    flashThreadId: string | null | undefined,
    reportBackRunId: string | null | undefined,
    pokeSource: string | null,
  ) => void;
}

/**
 * Bind the mux sink to the current render's runtime. Rebuilt per render like
 * the runtime itself; the mux holds whichever sink last attached.
 */
export function createSubagentMuxController(rt: SubagentRuntime, deps: SubagentMuxDeps) {
  /** Stamp one task's still-'running' inline chips (by mux agent id) with its
   * terminal outcome. The ONLY live path that moves a chip to terminal —
   * positive per-task closure (run_end / ledger-row truth via chan_close),
   * never inference from channel absence. Advancing only from 'running'
   * keeps an earlier cancelled/error stamp from being painted over. */
  const setInlineSubagentTaskStatus = (
    agentId: string,
    status: SubagentTask['status'],
  ) => {
    rt.setMessages((prev) => {
      let anyChanged = false;
      const updated = prev.map((msg) => {
        if (msg.role !== 'assistant') return msg;
        const aMsg = msg as AssistantMessage;
        if (!aMsg.subagentTasks || Object.keys(aMsg.subagentTasks).length === 0) return msg;
        let changed = false;
        const tasks = { ...aMsg.subagentTasks };
        Object.keys(tasks).forEach((toolCallId) => {
          if (rt.toolCallIdToTaskIdMapRef.current.get(toolCallId) !== agentId) return;
          if (tasks[toolCallId].status === 'running') {
            tasks[toolCallId] = { ...tasks[toolCallId], status };
            changed = true;
          }
        });
        if (changed) anyChanged = true;
        return changed ? { ...aMsg, subagentTasks: tasks } : msg;
      });
      return anyChanged ? updated : prev;
    });
  };

  /** A task is settled — and must never be re-activated by a stale /status
   * snapshot — if the ledger shows it terminal OR the client already saw its
   * per-task stream close live this session. Either way its run settled before
   * the mux's window, so no closure would ever arrive for a re-activated card.
   * The second half is what history alone misses: a task that closed live but
   * whose ledger the local history hasn't refreshed to terminal yet. */
  const isSettledTask = (shortTaskId: string): boolean => {
    const historyStatus = rt.subagentHistoryRef.current?.[`task:${shortTaskId}`]?.status;
    return (
      isTerminalStatus(normalizeWireStatus(historyStatus)) ||
      rt.terminalTaskOutcomesRef.current.has(shortTaskId)
    );
  };

  /**
   * Thread-level sink for the v2 mux. Task frames route through the CURRENT
   * stream processor (last attach wins), and run closure — run_end or
   * ledger-row truth, never socket loss — carries the completion lifecycle
   * that per-task stream teardown used to signal.
   */
  const muxSink: ThreadMuxSink = {
    onTaskEvent: (ev) => {
      // A drain channel replays a settled run's backlog from 0. When the
      // task is already terminal on our side — stamped by the history
      // projection or a live closure — that content is on screen, and the
      // chunk handlers concatenate; delivering it would duplicate the
      // transcript. Live (non-drain) frames always flow.
      if (ev._drain === true) {
        const agentId = typeof ev.agent === 'string' ? ev.agent : '';
        const shortId = agentId.startsWith('task:') ? agentId.slice(5) : '';
        const hist = rt.subagentHistoryRef.current?.[agentId];
        if (isSettledTask(shortId)) {
          return;
        }
        // Run-level guard: a task can be live under a successor run while a
        // stale drain re-delivers a settled predecessor. The watermark is
        // stamped by the projection build from the runs it actually claimed,
        // so anything at or before it is on screen already; a run the
        // projection excluded (still executing at build) or never saw starts
        // after it and still flows.
        if (
          typeof ev._runStartedMs === 'number' &&
          typeof hist?.projectedRunStartedMs === 'number' &&
          ev._runStartedMs <= hist.projectedRunStartedMs
        ) {
          return;
        }
      }
      rt.subagentProcessEventRef.current?.(ev as SSEEvent);
    },
    onTaskRunClosed: (shortTaskId, outcome) => {
      // Positive closure from the run ledger: honor the real terminal
      // outcome — cancelled, error, and interrupted (a failure for tasks;
      // task HITL is descoped) must not render as success. A missing or
      // non-terminal outcome keeps the legacy 'completed' default.
      const normalized = normalizeWireStatus(outcome);
      const status =
        normalized && isTerminalStatus(normalized) ? normalized : 'completed';
      // Record the exact outcome so every downstream reactivation guard can seed
      // the RIGHT terminal status (a failed task must not resurface as completed).
      rt.terminalTaskOutcomesRef.current.set(shortTaskId, status);
      if (rt.updateSubagentCard) {
        rt.updateSubagentCard(`task:${shortTaskId}`, { status, isActive: false });
      }
      // Flip ONLY the just-closed task's inline chips, with its real outcome.
      // Positive closure per task — never a sweep over siblings: a sibling
      // with no open channel is not terminal (its chan_open may simply not
      // have landed yet), and its own closure/ledger stamp will flip it.
      setInlineSubagentTaskStatus(`task:${shortTaskId}`, status);
      // Workflow-level flag only when ALL run channels have closed. No status
      // sweep: each task's card/chip was already stamped terminal by its own
      // chan_close above (positive closure per task). A blanket completion
      // sweep here would falsely complete any sibling whose chan_close merely
      // lagged or dropped.
      if (deps.muxOpenTaskIds().size === 0) {
        rt.setHasActiveSubagents(false);
      }
      // The natural report-back discovery moment: a tail task that just
      // settled has its notification turn enqueued about now. Ensure the
      // watch is armed and poke a reconcile; an idle read on this source
      // never disarms (the enqueue may still be in flight — the `cleared`
      // or dispatch wake is authoritative). Skip mid-turn closes (inline
      // delivery, no report-back due).
      if (!rt.isStreamingRef.current) {
        deps.armReportBackWatch(rt.threadIdRef.current, null, 'taskStreamEnd');
      }
    },
    onResyncRequired: () => {
      // The knowledge horizon outran the server's catch-up window (tab
      // asleep / long outage): reload the projection from history, which
      // re-attaches the mux with a fresh snapshot when it finishes.
      console.log('[mux] horizon beyond catch-up window, reloading history');
      if (rt.isStreamingRef.current) {
        // Mid-stream the load effect would detach the mux in its cleanup
        // and then bail on the streaming guard — defer to stream end
        // (releaseStreamOwnership flushes this).
        rt.pendingMuxResyncRef.current = true;
        return;
      }
      rt.setReloadTrigger((n) => n + 1);
    },
  };

  /**
   * Keep the thread's v2 mux attached with the current stream processor.
   * Idempotent; the mux discovers runs itself (attach seeds + control lane),
   * so callers never name a task. `snapshotAtMs` — when the driving
   * status/history snapshot was taken — anchors the server's settled-run
   * catch-up window at the client's true knowledge horizon.
   */
  const attachSubagentMux = (
    tid: string,
    processEvent: (event: SSEEvent) => void,
    snapshotAtMs?: number,
  ) => {
    rt.subagentProcessEventRef.current = processEvent;
    getThreadMux(tid).attach(muxSink, snapshotAtMs);
  };

  return { isSettledTask, attachSubagentMux };
}
