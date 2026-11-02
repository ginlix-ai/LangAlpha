// @vitest-environment node
import { describe, it, expect } from 'vitest';

import { createRequestKeyTracker, sendFingerprint } from '../requestKey';

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

  it('mints a fresh key for the same fingerprint once its send is spent (send accepted)', () => {
    const tracker = createRequestKeyTracker();
    const first = tracker.take('send|t1|hello');
    tracker.spend(first);
    expect(tracker.take('send|t1|hello')).not.toBe(first);
  });

  it('spends the pending key only for the send a run delivered', () => {
    const tracker = createRequestKeyTracker();
    const earlier = tracker.take('send|t1|hello');
    tracker.spend(earlier);
    const pending = tracker.take('send|t1|hello');
    // An earlier send of the same text was delivered: this one may still be
    // retransmitted.
    tracker.spend(earlier);
    expect(tracker.take('send|t1|hello')).toBe(pending);
    tracker.spend(pending);
    expect(tracker.take('send|t1|hello')).not.toBe(pending);
  });

  it('spends the key on an accepted settle, and keeps it on any other', () => {
    const tracker = createRequestKeyTracker();
    const refused = tracker.take('send|t1|hello');
    tracker.settle(refused, { refused: 'No credits left.' });
    expect(tracker.take('send|t1|hello')).toBe(refused);
    tracker.settle(refused, 'dropped');
    expect(tracker.take('send|t1|hello')).toBe(refused);
    tracker.settle(refused, 'accepted');
    expect(tracker.take('send|t1|hello')).not.toBe(refused);
  });

  it('keeps a steer\'s pending key while another steer goes out alongside it', () => {
    const tracker = createRequestKeyTracker();
    const first = tracker.take('send|t1|hello', { alongside: true });
    const other = tracker.take('send|t1|goodbye', { alongside: true });
    tracker.spend(other);
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

describe('the comments a request carries', () => {
  const ids = (...list: string[]) => ({ ids: list, reading: 'chat' as const });
  function watched() {
    const tracker = createRequestKeyTracker();
    const heard: [string[], unknown][] = [];
    tracker.watchComments((comments, outcome) => {
      heard.push(['ids' in comments ? [...comments.ids] : comments.comments.map((c) => c.comment_id), outcome]);
    });
    return { tracker, heard };
  }

  it('tells a take, then the first outcome only', () => {
    const { tracker, heard } = watched();
    const key = tracker.take('send|t1|note', { comments: ids('c-1', 'c-2') });
    tracker.settle(key, { refused: 'No credits left.' });
    tracker.settle(key, 'dropped');
    expect(heard).toEqual([
      [['c-1', 'c-2'], 'began'],
      [['c-1', 'c-2'], { refused: 'No credits left.' }],
    ]);
  });

  it('keeps comments held through a hand-off, until the request that took them on settles', () => {
    const { tracker, heard } = watched();
    const key = tracker.take('send|t1|note', { comments: ids('c-1') });
    tracker.settle(key, 'handedOff');
    expect(heard).toEqual([[['c-1'], 'began']]);
    // The scheduled retry goes out under the same key and settles for them.
    tracker.take('retry|t1', { key, comments: ids('c-1') });
    tracker.settle(key, 'accepted');
    expect(heard.map(([, outcome]) => outcome)).toEqual(['began', 'began', 'accepted']);
  });

  it('settles a comment once, for the latest request to carry it', () => {
    const { tracker, heard } = watched();
    const parked = tracker.take('send|t1|note', { alongside: true, comments: ids('c-1', 'c-2') });
    const resend = tracker.take('send|t1|again', { alongside: true, comments: ids('c-2') });
    tracker.settle(parked, 'dropped');
    tracker.settle(resend, 'accepted');
    expect(heard.slice(2)).toEqual([
      [['c-1'], 'dropped'],
      [['c-2'], 'accepted'],
    ]);
  });

  it('says nothing for a request that carries no comments', () => {
    const { tracker, heard } = watched();
    const key = tracker.take('send|t1|plain', { comments: ids() });
    tracker.settle(key, 'dropped');
    expect(heard).toEqual([]);
  });
});

describe('sendFingerprint', () => {
  const comments = (...ids: string[]) => ({ ids, reading: 'chat' as const });

  it('is the plain send fingerprint when no file comments ride along', () => {
    expect(sendFingerprint('t1', 'hello')).toBe(sendFingerprint('t1', 'hello', comments()));
  });

  it('tells the same note with other comments apart, whatever order they come in', () => {
    const one = sendFingerprint('t1', 'note', comments('c-2', 'c-1'));
    expect(sendFingerprint('t1', 'note', comments('c-1', 'c-2'))).toBe(one);
    expect(sendFingerprint('t1', 'note', { ids: ['c-1', 'c-2'], reading: 'file' })).toBe(one);
    expect(sendFingerprint('t1', 'note', comments('c-1'))).not.toBe(one);
    expect(sendFingerprint('t1', 'note')).not.toBe(one);
  });

  it('tells whoever shows the comments that a stop handed them back', () => {
    const tracker = createRequestKeyTracker();
    const heard: unknown[] = [];
    tracker.watchComments((_comments, outcome) => heard.push(outcome));
    tracker.returned({ ids: ['c-1'], reading: 'chat' });
    expect(heard).toEqual(['returned']);
  });
});
