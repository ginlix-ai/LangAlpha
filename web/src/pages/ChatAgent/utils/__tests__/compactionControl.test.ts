/**
 * Pure decision helpers for the manual-compaction Stop control in ChatView.
 * These cover three regressions:
 *  - the single Stop button must route to compaction-teardown only for a manual
 *    compaction (no streaming turn), never for a running turn (T2).
 *  - a user-cancelled compaction request rejects with a structured 409
 *    {code:"request_cancelled"}; that must read as a clean stop, not an error —
 *    even after a rapid stop→retrigger already consumed the local flag (T2).
 *  - a late resolution/rejection from a superseded compaction must NOT flip the
 *    isCompacting flag and unmask the input mid-compaction (RT#2 generation
 *    guard).
 */
import { describe, it, expect } from 'vitest';
import {
  routeStopAction,
  compactionErrorCode,
  isUserStoppedCompaction,
  shouldClearCompactingFlag,
  isManualCompactionInFlight,
} from '../compactionControl';

describe('routeStopAction', () => {
  it('routes a manual compaction (no streaming turn) to compaction teardown', () => {
    expect(routeStopAction({ isCompacting: 'summarize', isLoading: false })).toBe('compaction');
    expect(routeStopAction({ isCompacting: 'offload', isLoading: false })).toBe('compaction');
  });

  it('routes a running turn to workflow teardown even while it auto-compacts', () => {
    // An auto Tier-2 summarize runs inside a streaming turn (isLoading=true);
    // the Stop button must tear down the turn, not the manual-compaction path.
    expect(routeStopAction({ isCompacting: 'summarize', isLoading: true })).toBe('workflow');
  });

  it('routes an idle thread to workflow teardown', () => {
    expect(routeStopAction({ isCompacting: false, isLoading: false })).toBe('workflow');
  });
});

describe('compactionErrorCode', () => {
  it('extracts a structured detail code from an axios-style rejection', () => {
    const err = { response: { data: { detail: { code: 'request_cancelled' } } } };
    expect(compactionErrorCode(err)).toBe('request_cancelled');
  });

  it('returns undefined when there is no structured code', () => {
    expect(compactionErrorCode(new Error('boom'))).toBeUndefined();
    expect(compactionErrorCode(undefined)).toBeUndefined();
    expect(compactionErrorCode({ response: { data: { detail: 'plain string' } } })).toBeUndefined();
  });
});

describe('isUserStoppedCompaction', () => {
  it('treats a locally-flagged stop as a stop', () => {
    expect(isUserStoppedCompaction({ userStopped: true })).toBe(true);
  });

  it('treats a request_cancelled code as a stop even after the flag was consumed', () => {
    // Rapid stop→retrigger resets userStoppedCompactionRef to false before the
    // first request's rejection lands; the wire code is the durable signal.
    expect(
      isUserStoppedCompaction({ userStopped: false, errorCode: 'request_cancelled' }),
    ).toBe(true);
  });

  it('treats a genuine failure as an error', () => {
    expect(isUserStoppedCompaction({ userStopped: false, errorCode: 'compaction_in_progress' })).toBe(
      false,
    );
    expect(isUserStoppedCompaction({ userStopped: false })).toBe(false);
  });
});

describe('isManualCompactionInFlight (duplicate-trigger guard, #1)', () => {
  it('reports a manual compaction in flight (no streaming turn)', () => {
    expect(isManualCompactionInFlight({ isCompacting: 'summarize', isLoading: false })).toBe(true);
    expect(isManualCompactionInFlight({ isCompacting: 'offload', isLoading: false })).toBe(true);
  });

  it('does NOT report in-flight during an auto Tier-2 summarize (running turn)', () => {
    // isLoading=true means a streaming turn owns the summarize; a /compact then
    // falls through to the backend's workflow_active gate, not the local guard.
    expect(isManualCompactionInFlight({ isCompacting: 'summarize', isLoading: true })).toBe(false);
  });

  it('does NOT report in-flight on an idle thread', () => {
    // Refusing only when a manual compaction is genuinely running keeps the
    // legit stop→retrigger path open (Stop clears isCompacting synchronously).
    expect(isManualCompactionInFlight({ isCompacting: false, isLoading: false })).toBe(false);
  });
});

describe('shouldClearCompactingFlag (RT#2 generation guard)', () => {
  it('clears the flag for the current compaction', () => {
    expect(shouldClearCompactingFlag(2, 2)).toBe(true);
  });

  it('does NOT clear the flag for a superseded compaction', () => {
    // A stop→retrigger bumps the generation; the first compaction's late
    // settlement must not flip isCompacting and unmask the input.
    expect(shouldClearCompactingFlag(1, 2)).toBe(false);
  });
});
