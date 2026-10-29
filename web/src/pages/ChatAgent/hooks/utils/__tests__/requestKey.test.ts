// @vitest-environment node
import { describe, it, expect } from 'vitest';

import { createRequestKeyTracker } from '../requestKey';

describe('createRequestKeyTracker', () => {
  it('reuses the pending key for an identical fingerprint (retransmit)', () => {
    const tracker = createRequestKeyTracker();
    const first = tracker.take('send|t1|hello');
    expect(tracker.take('send|t1|hello')).toBe(first);
  });

  it('mints a fresh key when the fingerprint changes, replacing the pending one', () => {
    const tracker = createRequestKeyTracker();
    const first = tracker.take('send|t1|hello');
    const second = tracker.take('send|t1|goodbye');
    expect(second).not.toBe(first);
    // The original fingerprint no longer matches the pending slot either.
    expect(tracker.take('send|t1|hello')).not.toBe(first);
  });

  it('mints a fresh key for the same fingerprint once its send settles (send accepted)', () => {
    const tracker = createRequestKeyTracker();
    const first = tracker.take('send|t1|hello');
    tracker.settle(first);
    expect(tracker.take('send|t1|hello')).not.toBe(first);
  });

  it('settles the pending key only for the send a run delivered', () => {
    const tracker = createRequestKeyTracker();
    const earlier = tracker.take('send|t1|hello');
    tracker.settle(earlier);
    const pending = tracker.take('send|t1|hello');
    // An earlier send of the same text was delivered: this one may still be
    // retransmitted.
    tracker.settle(earlier);
    expect(tracker.take('send|t1|hello')).toBe(pending);
    tracker.settle(pending);
    expect(tracker.take('send|t1|hello')).not.toBe(pending);
  });

  it('keeps a steer\'s pending key while another steer goes out alongside it', () => {
    const tracker = createRequestKeyTracker();
    const first = tracker.take('send|t1|hello', { alongside: true });
    const other = tracker.take('send|t1|goodbye', { alongside: true });
    tracker.settle(other);
    expect(tracker.take('send|t1|hello', { alongside: true })).toBe(first);
    // A send that does not go out alongside the others drops their keys.
    tracker.take('retry|t1');
    expect(tracker.take('send|t1|hello', { alongside: true })).not.toBe(first);
  });

  it('returns UUID-shaped keys', () => {
    const tracker = createRequestKeyTracker();
    expect(tracker.take('x')).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
  });
});
