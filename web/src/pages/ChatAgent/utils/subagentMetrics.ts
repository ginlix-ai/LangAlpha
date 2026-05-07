/**
 * Pure derivations of subagent telemetry from the existing message tree.
 *
 * Tool-call count is derived (not stored) so there's only one source of truth
 * (`toolCallProcesses` map keyed by tool_call_id) — re-emitted events on
 * reconnect can't inflate the count, and the live and history-replay paths
 * don't need separate writers.
 */
interface MessageLike {
  toolCallProcesses?: Record<string, unknown>;
  [key: string]: unknown;
}

export function countToolCalls(messages: MessageLike[] | undefined | null): number {
  if (!messages || messages.length === 0) return 0;
  let total = 0;
  for (const m of messages) {
    const procs = m?.toolCallProcesses;
    if (procs && typeof procs === 'object') {
      total += Object.keys(procs).length;
    }
  }
  return total;
}
