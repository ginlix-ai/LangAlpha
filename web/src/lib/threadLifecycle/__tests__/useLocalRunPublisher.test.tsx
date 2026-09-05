/**
 * Settle semantics of the local run publisher: a run-id-less settle (send
 * failed before response headers) must DROP the local observation — no run row
 * was committed, so a seq-less terminal observation could never be reconciled
 * away by snapshots and would sit as a permanent phantom dot.
 */
import { renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';
import { useLocalRunPublisher } from '../useLocalRunPublisher';
import { getEffectiveObservation, resetThreadLifecycle } from '../store';

beforeEach(() => {
  resetThreadLifecycle();
});

describe('useLocalRunPublisher — settle', () => {
  it('a settle with an admitted run publishes completed', () => {
    const runIdRef = { current: 'r1' };
    const { rerender } = renderHook(
      ({ loading }) => useLocalRunPublisher('t1', loading, runIdRef),
      { initialProps: { loading: true } },
    );
    expect(getEffectiveObservation('t1')?.status).toBe('running');

    rerender({ loading: false });
    expect(getEffectiveObservation('t1')?.status).toBe('completed');
    expect(getEffectiveObservation('t1')?.runId).toBe('r1');
  });

  it('a settle with no admitted run drops the observation instead of minting a phantom dot', () => {
    const runIdRef = { current: null };
    const { rerender } = renderHook(
      ({ loading }) => useLocalRunPublisher('t1', loading, runIdRef),
      { initialProps: { loading: true } },
    );
    expect(getEffectiveObservation('t1')?.status).toBe('running');

    rerender({ loading: false });
    expect(getEffectiveObservation('t1')).toBeUndefined();
  });

  it('a threadId swap while running never settles the new thread', () => {
    const runIdRef = { current: 'r-old' };
    const { rerender } = renderHook(
      ({ tid, loading }) => useLocalRunPublisher(tid, loading, runIdRef),
      { initialProps: { tid: 'a', loading: true } },
    );
    expect(getEffectiveObservation('a')?.status).toBe('running');

    // Not reachable through today's keying (ChatView instances only ever flip
    // __default__ → real), but the invariant must hold on its own.
    rerender({ tid: 'b', loading: false });
    expect(getEffectiveObservation('b')).toBeUndefined();
  });
});
