import { describe, it, expect } from 'vitest';
import {
  resolveSubagentTelemetry,
  type SubagentDataLike,
  type SubagentHistoryLike,
} from '../resolveSubagentTelemetry';
import { ZERO_USAGE } from '../../../utils/tokenUsage';

const msgWith = (tools: number) => ({
  toolCallProcesses: Object.fromEntries(Array.from({ length: tools }, (_, i) => [`tc-${i}`, {}])),
});

describe('resolveSubagentTelemetry', () => {
  it('returns undefined when neither card nor history is present', () => {
    expect(resolveSubagentTelemetry(undefined, undefined)).toBeUndefined();
  });

  it('uses card state when populated with messages', () => {
    const card: SubagentDataLike = {
      messages: [msgWith(2), msgWith(3)],
      tokenUsage: { input: 100, output: 50, total: 150 },
    };
    expect(resolveSubagentTelemetry(card, undefined)).toEqual({
      toolCalls: 5,
      tokenUsage: { input: 100, output: 50, total: 150 },
    });
  });

  it('uses card state when messages are empty but tokenUsage.total > 0', () => {
    // Live token_usage event landed before any tool call — card has tokens
    // but no messages yet.
    const card: SubagentDataLike = {
      messages: [],
      tokenUsage: { input: 10, output: 5, total: 15 },
    };
    expect(resolveSubagentTelemetry(card, undefined)).toEqual({
      toolCalls: 0,
      tokenUsage: { input: 10, output: 5, total: 15 },
    });
  });

  it('falls back to history when card is present but empty (namespace-race fix)', () => {
    // The exact scenario the click-replay path used to hit: a freshly
    // created card with no live state should NOT shadow the history total.
    const emptyCard: SubagentDataLike = { messages: [], tokenUsage: ZERO_USAGE };
    const history: SubagentHistoryLike = {
      messages: [msgWith(4)],
      tokenUsage: { input: 200, output: 100, total: 300 },
    };
    expect(resolveSubagentTelemetry(emptyCard, history)).toEqual({
      toolCalls: 4,
      tokenUsage: { input: 200, output: 100, total: 300 },
    });
  });

  it('falls back to history when no card exists at all (post-refresh path)', () => {
    const history: SubagentHistoryLike = {
      messages: [msgWith(7)],
      tokenUsage: { input: 500, output: 250, total: 750 },
    };
    expect(resolveSubagentTelemetry(undefined, history)).toEqual({
      toolCalls: 7,
      tokenUsage: { input: 500, output: 250, total: 750 },
    });
  });

  it('prefers explicit history.toolCalls over derived count', () => {
    // History writers may stamp toolCalls explicitly; the resolver should
    // trust that over re-deriving (avoids double-counting reconnect events).
    const history: SubagentHistoryLike = {
      messages: [msgWith(3)],
      toolCalls: 99,
      tokenUsage: { input: 0, output: 0, total: 1000 },
    };
    expect(resolveSubagentTelemetry(undefined, history)).toEqual({
      toolCalls: 99,
      tokenUsage: { input: 0, output: 0, total: 1000 },
    });
  });

  it('history with missing tokenUsage falls through to ZERO_USAGE', () => {
    const history: SubagentHistoryLike = { messages: [msgWith(2)] };
    expect(resolveSubagentTelemetry(undefined, history)).toEqual({
      toolCalls: 2,
      tokenUsage: ZERO_USAGE,
    });
  });

  it('card present but unpopulated AND no history → returns ZERO defaults from card path', () => {
    // Edge case: the card was created (e.g. user clicked) but neither live
    // events nor history exist yet. We still return a value so callers can
    // render "0 tools · 0 tokens" without flickering through undefined.
    const emptyCard: SubagentDataLike = { messages: [], tokenUsage: ZERO_USAGE };
    expect(resolveSubagentTelemetry(emptyCard, undefined)).toEqual({
      toolCalls: 0,
      tokenUsage: ZERO_USAGE,
    });
  });

  it('does not mutate inputs', () => {
    const card: SubagentDataLike = { messages: [msgWith(1)], tokenUsage: { input: 1, output: 1, total: 2 } };
    const history: SubagentHistoryLike = { messages: [msgWith(9)], tokenUsage: { input: 9, output: 9, total: 18 } };
    const cardSnap = JSON.stringify(card);
    const historySnap = JSON.stringify(history);
    resolveSubagentTelemetry(card, history);
    expect(JSON.stringify(card)).toBe(cardSnap);
    expect(JSON.stringify(history)).toBe(historySnap);
  });
});

/**
 * The stop reason has its own precedence, deliberately unlike the numbers.
 * Only one of the two writers ever holds a reason for a given task — the live
 * error frame stamps the card, replay stamps history from the ledger — so
 * reading it from whichever source won the count would blank it on exactly
 * the permutation the other one covers.
 */
describe('resolveSubagentTelemetry — stop reason', () => {
  const REASON = 'Monthly credit limit reached (50/50 credits)';

  it('takes the reason from history while the numbers come from the card', () => {
    const card: SubagentDataLike = { messages: [msgWith(2)], tokenUsage: ZERO_USAGE };
    const history: SubagentHistoryLike = { error: REASON, errorType: 'credit_stop' };
    expect(resolveSubagentTelemetry(card, history)).toEqual({
      toolCalls: 2,
      tokenUsage: ZERO_USAGE,
      stopReason: REASON,
      stopReasonType: 'credit_stop',
    });
  });

  it('takes the reason from the card when history is the source of the numbers', () => {
    const card: SubagentDataLike = { error: REASON, errorType: 'credit_stop' };
    const history: SubagentHistoryLike = { toolCalls: 13, tokenUsage: ZERO_USAGE };
    expect(resolveSubagentTelemetry(card, history)).toEqual({
      toolCalls: 13,
      tokenUsage: ZERO_USAGE,
      stopReason: REASON,
      stopReasonType: 'credit_stop',
    });
  });

  it('omits both keys entirely when neither source has a reason', () => {
    const result = resolveSubagentTelemetry({ messages: [msgWith(1)] }, undefined);
    expect(result).not.toHaveProperty('stopReason');
    expect(result).not.toHaveProperty('stopReasonType');
  });

  it('keeps a reason that arrived without a type — it is still worth stating', () => {
    const result = resolveSubagentTelemetry(undefined, { error: 'run ended: cancelled' });
    expect(result?.stopReason).toBe('run ended: cancelled');
    expect(result).not.toHaveProperty('stopReasonType');
  });
});
