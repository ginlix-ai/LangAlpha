/**
 * Pure resolver for what an inline subagent card shows about a task it does
 * not own: its telemetry, and why it stopped.
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
import { ZERO_USAGE, type SubagentTokenUsage } from '../../utils/tokenUsage';

export interface SubagentTelemetry {
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
  /** Why a settled task ended, when it ended with a reason. */
  stopReason?: string;
  /** That reason's machine spelling, so a surface can offer the remedy for the
   *  one kind of stop that has one. */
  stopReasonType?: string;
}

interface MessageLike {
  toolCallProcesses?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface SubagentDataLike {
  messages?: MessageLike[];
  tokenUsage?: SubagentTokenUsage;
  error?: string;
  errorType?: string;
}

export interface SubagentHistoryLike {
  messages?: MessageLike[];
  tokenUsage?: SubagentTokenUsage;
  toolCalls?: number;
  error?: string;
  errorType?: string;
}

export function resolveSubagentTelemetry(
  subagentData: SubagentDataLike | undefined,
  history: SubagentHistoryLike | undefined,
): SubagentTelemetry | undefined {
  const sdMessages = subagentData?.messages;
  const sdTokenUsage = subagentData?.tokenUsage;

  // The reason has its own precedence, deliberately unlike the numbers: those
  // pick a source and read it whole, but only one of the two writers ever
  // holds a reason for a given task — the live error frame stamps the card,
  // replay stamps history from the ledger — so the reason takes whichever has
  // one rather than whichever won the count.
  const reason = ((): Pick<SubagentTelemetry, 'stopReason' | 'stopReasonType'> => {
    // One source for both fields, picked once. Read independently they can
    // pair a reason from the card with a type from history, and the type is
    // what decides whether a stop is offered back to the user with plan
    // links — so a mismatch does not garble the copy, it changes what the
    // surface claims happened.
    const source = subagentData?.error ? subagentData : history;
    const error = source?.error;
    if (!error) return {};
    const errorType = source?.errorType;
    return { stopReason: error, ...(errorType ? { stopReasonType: errorType } : {}) };
  })();

  // Card path: prefer live state, but only when the card has actually been
  // populated. A click-created card with empty messages and zero tokens
  // should still pull from history below — the bug we hit when post-refresh
  // resolution returned zero even though history had the real total.
  if (subagentData && (sdMessages?.length || (sdTokenUsage?.total ?? 0) > 0)) {
    return {
      toolCalls: countToolCalls(sdMessages),
      tokenUsage: sdTokenUsage ?? ZERO_USAGE,
      ...reason,
    };
  }

  // History fallback: post-refresh path before the user opens the card,
  // and the namespace-race fallback when SSE hydration hasn't caught up.
  if (history) {
    return {
      toolCalls: history.toolCalls ?? countToolCalls(history.messages),
      tokenUsage: history.tokenUsage ?? ZERO_USAGE,
      ...reason,
    };
  }

  if (!subagentData) return undefined;

  return {
    toolCalls: countToolCalls(sdMessages),
    tokenUsage: sdTokenUsage ?? ZERO_USAGE,
    ...reason,
  };
}
