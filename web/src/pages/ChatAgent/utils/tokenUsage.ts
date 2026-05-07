/**
 * Per-subagent token-usage accumulation.
 *
 * Backend emits one `context_window` `token_usage` SSE event per LLM call,
 * carrying THIS-CALL deltas (input/output/total). The frontend accumulates
 * deltas into a per-subagent running total and renders it on the inline
 * subagent card (and any future telemetry surface).
 *
 * Splitting extract / accumulate into pure functions lets the live and
 * history-replay handlers share one code path, and keeps the addition rule
 * in one place if backend semantics ever flip from delta to cumulative.
 */

export type SubagentTokenUsage = {
  input: number;
  output: number;
  total: number;
};

// Frozen so the shared default can't be mutated by an accidental write at any
// of the `?? ZERO_USAGE` call sites (TS `as` casts bypass type-level guards).
export const ZERO_USAGE: SubagentTokenUsage = Object.freeze({ input: 0, output: 0, total: 0 });

// Defensive against missing fields, NaN, and string-typed numbers from older
// SSE shapes. `total` falls back to `input + output` if backend omitted it.
export function extractTokenUsageDelta(event: Record<string, unknown>): SubagentTokenUsage {
  const num = (v: unknown): number => {
    const n = typeof v === 'number' ? v : typeof v === 'string' ? Number(v) : 0;
    return Number.isFinite(n) && n > 0 ? n : 0;
  };
  const input = num(event.input_tokens);
  const output = num(event.output_tokens);
  const totalRaw = num(event.total_tokens);
  return { input, output, total: totalRaw || input + output };
}

// Commutative addition — order of incoming events doesn't matter, so live
// and history-replay produce the same total for the same set of events.
export function accumulateTokenUsage(prev: SubagentTokenUsage, delta: SubagentTokenUsage): SubagentTokenUsage {
  return {
    input: prev.input + delta.input,
    output: prev.output + delta.output,
    total: prev.total + delta.total,
  };
}
