/**
 * Poll-cadence decisions for the dispatched-PTC status card. A flash turn can
 * show several cards, each polling the full /status, so the cadence must back
 * off once a run is clearly long-lived and must stop entirely for a run that
 * never registers (a continuation rejected by the per-user cap stays 'starting'
 * forever) — otherwise the cards hammer /status indefinitely.
 */
import { describe, it, expect } from 'vitest';
import { nextDispatchPollInterval } from '../usePTCDispatchStatus';

describe('nextDispatchPollInterval', () => {
  it('stops polling once the run is terminal', () => {
    expect(nextDispatchPollInterval('completed', 1)).toBe(false);
    expect(nextDispatchPollInterval('failed', 50)).toBe(false);
    expect(nextDispatchPollInterval('stopped', 0)).toBe(false);
  });

  it('polls fast early, then backs off for a long-lived run', () => {
    expect(nextDispatchPollInterval('running', 0)).toBe(4_000);
    expect(nextDispatchPollInterval('running', 4)).toBe(4_000);
    // 5th poll onward: a dispatched analysis runs for minutes, so ease off.
    expect(nextDispatchPollInterval('running', 5)).toBe(10_000);
    expect(nextDispatchPollInterval('running', 100)).toBe(10_000);
  });

  it('keeps polling a still-starting run within the cap, then gives up', () => {
    expect(nextDispatchPollInterval('starting', 0)).toBe(4_000);
    expect(nextDispatchPollInterval('starting', 29)).toBe(10_000);
    // 30th poll still 'starting' → the run never registered; stop hammering.
    expect(nextDispatchPollInterval('starting', 30)).toBe(false);
  });

  it('caps on CONSECUTIVE starting rounds, not total polls in the window', () => {
    // Plenty of polls this window, but the run only just regressed to
    // 'starting' — the cap doesn't trip, cadence stays steady.
    expect(nextDispatchPollInterval('starting', 40, 3)).toBe(10_000);
    // 30 straight 'starting' rounds → give up, regardless of total polls.
    expect(nextDispatchPollInterval('starting', 40, 30)).toBe(false);
    // A wake resets both counters: full budget and fast cadence again.
    expect(nextDispatchPollInterval('starting', 0, 0)).toBe(4_000);
  });

  it('never caps a run that actually registered, even past the starting cap', () => {
    // needs_input / running past the starting cap keep polling — only a stuck
    // 'starting' is abandoned.
    expect(nextDispatchPollInterval('running', 30)).toBe(10_000);
    expect(nextDispatchPollInterval('needs_input', 40)).toBe(10_000);
  });
});
