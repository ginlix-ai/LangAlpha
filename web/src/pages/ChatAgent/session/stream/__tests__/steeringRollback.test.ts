import { describe, it, expect } from 'vitest';
import { computeSteeringBoundary, shouldSkipSteeringRollback } from '../steeringRollback';

describe('computeSteeringBoundary', () => {
  it('returns numeric _eventId verbatim', () => {
    expect(computeSteeringBoundary({ _eventId: 12345 }, 99)).toBe(12345);
  });

  it('coerces string _eventId via Number()', () => {
    // Redis stream ids parse as numeric millisecond prefix in the SSE
    // injection layer, but a defensive string fallback may slip through.
    expect(computeSteeringBoundary({ _eventId: '67890' }, 99)).toBe(67890);
  });

  it('returns NaN for non-numeric string _eventId', () => {
    // Non-numeric strings should yield NaN — caller MUST then call
    // shouldSkipSteeringRollback to avoid wiping the visible turn.
    expect(computeSteeringBoundary({ _eventId: '1759-0' }, 99)).toBeNaN();
    expect(computeSteeringBoundary({ _eventId: 'abc' }, 99)).toBeNaN();
  });

  it('falls back to counter when _eventId is undefined', () => {
    expect(computeSteeringBoundary({}, 42)).toBe(42);
  });

  it('falls back to counter when _eventId is null', () => {
    expect(computeSteeringBoundary({ _eventId: null as unknown as undefined }, 7)).toBe(7);
  });

  it('treats _eventId === 0 as a real boundary, not a fallback trigger', () => {
    // A literal 0 _eventId is rare but valid; the guard catches it
    // separately. computeSteeringBoundary should not silently substitute
    // the counter for it.
    expect(computeSteeringBoundary({ _eventId: 0 }, 99)).toBe(0);
  });
});

describe('shouldSkipSteeringRollback', () => {
  it('skips when boundary is null', () => {
    expect(shouldSkipSteeringRollback(null)).toBe(true);
  });

  it('skips when boundary is NaN', () => {
    // The regression: a non-numeric string `_eventId` from a legacy or
    // mis-tagged event yields NaN. Without this guard, every comparison
    // `s.order <= NaN` is false and the filter wipes the entire turn.
    expect(shouldSkipSteeringRollback(NaN)).toBe(true);
  });

  it('skips when boundary is 0', () => {
    expect(shouldSkipSteeringRollback(0)).toBe(true);
  });

  it('skips when boundary is negative', () => {
    expect(shouldSkipSteeringRollback(-1)).toBe(true);
    expect(shouldSkipSteeringRollback(-12345)).toBe(true);
  });

  it('skips when boundary is Infinity / -Infinity', () => {
    // Number.isFinite catches both infinities — Infinity boundary
    // technically keeps everything but signals corrupt state.
    expect(shouldSkipSteeringRollback(Infinity)).toBe(true);
    expect(shouldSkipSteeringRollback(-Infinity)).toBe(true);
  });

  it('does NOT skip for any positive finite boundary', () => {
    expect(shouldSkipSteeringRollback(1)).toBe(false);
    expect(shouldSkipSteeringRollback(42)).toBe(false);
    expect(shouldSkipSteeringRollback(1759000000000)).toBe(false); // realistic Redis ms id
  });

  it('does NOT skip for fractional positive boundaries', () => {
    // Defensive: a non-integer boundary still keeps segments with
    // smaller orders. Production never produces this, but the guard
    // should not over-reject.
    expect(shouldSkipSteeringRollback(0.5)).toBe(false);
  });
});

describe('integration: compute → guard pipeline', () => {
  it('numeric _eventId → boundary preserved → filter applies', () => {
    const boundary = computeSteeringBoundary({ _eventId: 100 }, 0);
    expect(shouldSkipSteeringRollback(boundary)).toBe(false);
  });

  it('non-numeric string _eventId → NaN → filter SKIPPED (regression)', () => {
    // The exact regression this branch fixes: a non-numeric `_eventId`
    // would have produced NaN and wiped the visible turn before the
    // widened guard.
    const boundary = computeSteeringBoundary({ _eventId: 'bad-id' }, 0);
    expect(shouldSkipSteeringRollback(boundary)).toBe(true);
  });

  it('missing _eventId AND zero counter → 0 boundary → filter SKIPPED', () => {
    // Steering arrived before any ordered content was emitted: counter
    // is still 0, no segments to keep, finalize without a destructive
    // filter that would otherwise drop everything.
    const boundary = computeSteeringBoundary({}, 0);
    expect(shouldSkipSteeringRollback(boundary)).toBe(true);
  });

  it('missing _eventId AND positive counter → counter boundary → filter applies', () => {
    const boundary = computeSteeringBoundary({}, 17);
    expect(shouldSkipSteeringRollback(boundary)).toBe(false);
    expect(boundary).toBe(17);
  });
});
