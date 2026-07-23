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

  it('mints a fresh key for the same fingerprint after clear() (send accepted)', () => {
    const tracker = createRequestKeyTracker();
    const first = tracker.take('send|t1|hello');
    tracker.clear();
    expect(tracker.take('send|t1|hello')).not.toBe(first);
  });

  it('returns UUID-shaped keys', () => {
    const tracker = createRequestKeyTracker();
    expect(tracker.take('x')).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
  });
});
