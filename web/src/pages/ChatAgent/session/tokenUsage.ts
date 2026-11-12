/**
 * The thread's token usage as the context ring and panel read it, and the
 * reducers that keep it: a model call, the server's count, a load without
 * one, an older page.
 */

import type { SSEEvent } from './types';
import type { ThreadTokenTotals } from '../utils/api/messages';

/** The thread's usage. The totals are the thread's: the server's count when
 *  it loaded, plus live calls since. The rest is the newest call's. */
export interface TokenUsage {
  totalInput: number;
  totalOutput: number;
  /** Cache-read input tokens, held in `totalInput` too. Null once any part of
   *  the sum went uncounted (a server or a frame from before the field), so
   *  the hit rate hides rather than reading low. */
  totalCached: number | null;
  /** Whether the totals started from the server's count. Without one they
   *  sum the calls the view has loaded, the closest it can come. */
  fromServer: boolean;
  lastInput: number;
  lastCached: number | null;
  total: number;
  threshold: number;
  /** The model's context window, when it declares one. */
  contextLimit: number | null;
}

/** The fields of a `token_usage` frame the usage reads. */
type CallUsage = Pick<SSEEvent, 'input_tokens' | 'output_tokens' | 'cached_tokens' | 'total_tokens' | 'threshold' | 'context_limit'>;

const count = (value: unknown): number | null =>
  typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;

/** A sum that stays unknown once either side is. */
const addKnown = (a: number | null, b: number | null): number | null => (a === null || b === null ? null : a + b);

/**
 * One model call's usage folded in. The newest call sets the window, its
 * limit included, so a call without one clears the last call's. A live call
 * adds to the totals. A replayed one adds only when the server sent no count,
 * since a count already holds every call a replay delivers.
 */
export function applyCallUsage(prev: TokenUsage | null, call: CallUsage, live: boolean): TokenUsage {
  const callInput = call.input_tokens || 0;
  const callOutput = call.output_tokens || 0;
  const callCached = count(call.cached_tokens);
  const fromServer = prev?.fromServer ?? false;
  const adds = live || !fromServer;
  const priorCached = prev ? prev.totalCached : 0;
  return {
    totalInput: (prev?.totalInput || 0) + (adds ? callInput : 0),
    totalOutput: (prev?.totalOutput || 0) + (adds ? callOutput : 0),
    totalCached: adds ? addKnown(priorCached, callCached) : priorCached,
    fromServer,
    lastInput: callInput,
    lastCached: callCached,
    total: call.total_tokens || 0,
    threshold: call.threshold || prev?.threshold || 0,
    contextLimit: count(call.context_limit) || null,
  };
}

/** The usage with the server's count as the totals; the window stays. */
export function withServerTotals(prev: TokenUsage | null, counted: ThreadTokenTotals): TokenUsage {
  return {
    lastInput: 0, lastCached: null, total: 0, threshold: 0, contextLimit: null, ...prev,
    totalInput: counted.input_tokens,
    totalOutput: counted.output_tokens,
    totalCached: count(counted.cached_tokens),
    fromServer: true,
  };
}

/**
 * The usage for a load the server sent no count for, because its read
 * failed or it predates the count. The totals restart from zero and sum the
 * calls the load replays, so a reload does not add them a second time.
 */
export function withoutServerTotals(prev: TokenUsage | null): TokenUsage | null {
  return prev && { ...prev, totalInput: 0, totalOutput: 0, totalCached: 0, fromServer: false };
}

/**
 * The totals with an older page's calls joined, for a view the server sent no
 * count for. A count already holds them. With no usage yet, as when nothing
 * newer reported any, the page's totals start them and the window stays
 * unknown.
 */
export function withPageTotals(prev: TokenUsage | null, page: { input: number; output: number; cached: number | null }): TokenUsage | null {
  if (prev?.fromServer) return prev;
  prev ??= {
    totalInput: 0, totalOutput: 0, totalCached: 0, fromServer: false,
    lastInput: 0, lastCached: null, total: 0, threshold: 0, contextLimit: null,
  };
  return {
    ...prev,
    totalInput: prev.totalInput + page.input,
    totalOutput: prev.totalOutput + page.output,
    totalCached: addKnown(prev.totalCached, page.cached),
  };
}

/**
 * How much of the input was read from cache: cache reads over all the input
 * sent, the uncached and what was written to the cache included. A write
 * counts as a miss, having been processed in full. This is the rate other
 * agents' usage panels show, so the numbers compare. Null where it cannot be
 * told.
 */
export const hitRate = (cached: number | null, input: number): number | null =>
  cached !== null && input > 0 ? Math.min(cached / input, 1) : null;
