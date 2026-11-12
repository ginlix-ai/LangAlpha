// @vitest-environment node
import { describe, it, expect } from 'vitest';

import { applyCallUsage, hitRate, withPageTotals, withServerTotals, withoutServerTotals, type TokenUsage } from '../tokenUsage';
import type { SSEEvent } from '../types';

const call = (input: number, cached?: number, extra: Partial<SSEEvent> = {}): SSEEvent => ({
  event: 'context_window', action: 'token_usage',
  input_tokens: input, output_tokens: 10, total_tokens: input + 10, threshold: 1000,
  ...(cached === undefined ? {} : { cached_tokens: cached }),
  ...extra,
});

const fold = (start: TokenUsage | null, frames: SSEEvent[], live: boolean) =>
  frames.reduce<TokenUsage | null>((prev, frame) => applyCallUsage(prev, frame, live), start);

describe('cached totals', () => {
  it('sum the calls a view loads when no server count came', () => {
    const usage = fold(null, [call(400, 300), call(500, 450)], false);
    expect(usage).toMatchObject({ totalInput: 900, totalCached: 750, lastInput: 500, lastCached: 450 });
  });

  it('take the server count, which replayed calls do not add to and live calls do', () => {
    const counted = withServerTotals(null, { input_tokens: 9000, output_tokens: 700, cached_tokens: 6000 });
    const replayed = fold(counted, [call(400, 300)], false);
    expect(replayed).toMatchObject({ totalInput: 9000, totalCached: 6000, lastCached: 300 });
    expect(fold(replayed, [call(500, 450)], true)).toMatchObject({ totalInput: 9500, totalCached: 6450 });
  });

  it('are unknown from a server count without them, and live calls leave them so', () => {
    const counted = withServerTotals(null, { input_tokens: 9000, output_tokens: 700 });
    expect(counted.totalCached).toBeNull();
    expect(fold(counted, [call(500, 450)], true)).toMatchObject({ totalCached: null, lastCached: 450 });
  });

  it('are unknown once a call that adds came without them', () => {
    const usage = fold(null, [call(400, 300), call(500)], true);
    expect(usage).toMatchObject({ totalInput: 900, totalCached: null, lastCached: null });
  });

  it('a replayed call without them leaves a server count that has them alone', () => {
    const counted = withServerTotals(null, { input_tokens: 9000, output_tokens: 700, cached_tokens: 6000 });
    expect(fold(counted, [call(400)], false)).toMatchObject({ totalCached: 6000, lastCached: null });
  });

  it('restart from zero for a load without a count, and an older page joins them', () => {
    const before = fold(null, [call(400)], true);
    expect(before?.totalCached).toBeNull();
    const restarted = fold(withoutServerTotals(before), [call(400, 300)], false);
    expect(restarted).toMatchObject({ totalInput: 400, totalCached: 300 });
    expect(withPageTotals(restarted, { input: 200, output: 20, cached: 100 })).toMatchObject({ totalInput: 600, totalCached: 400 });
    expect(withPageTotals(restarted, { input: 200, output: 20, cached: null })).toMatchObject({ totalInput: 600, totalCached: null });
  });

  it('an older page leaves a server count alone', () => {
    const counted = withServerTotals(null, { input_tokens: 9000, output_tokens: 700, cached_tokens: 6000 });
    expect(withPageTotals(counted, { input: 200, output: 20, cached: 100 })).toBe(counted);
    // Nothing newer reported usage: the page's calls start the totals.
    expect(withPageTotals(null, { input: 200, output: 20, cached: 100 })).toMatchObject({
      totalInput: 200, totalOutput: 20, totalCached: 100, fromServer: false, total: 0, contextLimit: null,
    });
  });
});

describe('the newest call', () => {
  it('carries its limit', () => {
    const usage = applyCallUsage(null, call(400, 0, { context_limit: 200_000 }), true);
    expect(usage).toMatchObject({ contextLimit: 200_000, lastInput: 400, total: 410 });
  });

  it('clears the previous limit when it sends none', () => {
    const first = applyCallUsage(null, call(400, 0, { context_limit: 200_000 }), true);
    expect(applyCallUsage(first, call(500, 0), true)).toMatchObject({ contextLimit: null });
  });

  it('keeps the window and limit when the server count lands', () => {
    const live = applyCallUsage(null, call(400, 0, { context_limit: 200_000 }), true);
    expect(withServerTotals(live, { input_tokens: 9000, output_tokens: 700 })).toMatchObject({
      total: 410, lastInput: 400, contextLimit: 200_000,
    });
  });
});

describe('hitRate', () => {
  it('is the share of all the input sent that was read from cache', () => {
    expect(hitRate(208_000, 400_000)).toBe(0.52);
    expect(hitRate(137_200, 140_000)).toBe(0.98);
  });

  it('is unknown with no input, or with reads uncounted', () => {
    expect(hitRate(208_000, 0)).toBeNull();
    expect(hitRate(null, 400_000)).toBeNull();
  });

  it('never reads above all of it', () => {
    expect(hitRate(150_000, 140_000)).toBe(1);
  });
});
