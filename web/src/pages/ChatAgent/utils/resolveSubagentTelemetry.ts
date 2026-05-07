/**
 * Pure resolver for inline-card subagent telemetry.
 *
 * Two writers feed the inline subagent card: the live `cards[...]` state
 * (driven by SSE events) and the post-refresh `subagentHistoryRef`
 * (driven by history replay). Either can be present, both can be present,
 * or neither. The resolver picks the right source so the card renders
 * the same numbers in every reconnect/refresh permutation.
 *
 * Extracted as a pure function so the namespace-race fallback (history
 * fills in when the live card hasn't been hydrated yet) and the
 * post-refresh ZERO_USAGE seeding can be regression-tested without
 * mounting `ChatView`.
 */
import { countToolCalls } from './subagentMetrics';
import { ZERO_USAGE, type SubagentTokenUsage } from './tokenUsage';

export interface SubagentTelemetry {
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
}

interface MessageLike {
  toolCallProcesses?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface SubagentDataLike {
  messages?: MessageLike[];
  tokenUsage?: SubagentTokenUsage;
}

export interface SubagentHistoryLike {
  messages?: MessageLike[];
  tokenUsage?: SubagentTokenUsage;
  toolCalls?: number;
}

export function resolveSubagentTelemetry(
  subagentData: SubagentDataLike | undefined,
  history: SubagentHistoryLike | undefined,
): SubagentTelemetry | undefined {
  const sdMessages = subagentData?.messages;
  const sdTokenUsage = subagentData?.tokenUsage;

  // Card path: prefer live state, but only when the card has actually been
  // populated. A click-created card with empty messages and zero tokens
  // should still pull from history below — the bug we hit when post-refresh
  // resolution returned zero even though history had the real total.
  if (subagentData && (sdMessages?.length || (sdTokenUsage?.total ?? 0) > 0)) {
    return {
      toolCalls: countToolCalls(sdMessages),
      tokenUsage: sdTokenUsage ?? ZERO_USAGE,
    };
  }

  // History fallback: post-refresh path before the user opens the card,
  // and the namespace-race fallback when SSE hydration hasn't caught up.
  if (history) {
    return {
      toolCalls: history.toolCalls ?? countToolCalls(history.messages),
      tokenUsage: history.tokenUsage ?? ZERO_USAGE,
    };
  }

  if (!subagentData) return undefined;

  return {
    toolCalls: countToolCalls(sdMessages),
    tokenUsage: sdTokenUsage ?? ZERO_USAGE,
  };
}
