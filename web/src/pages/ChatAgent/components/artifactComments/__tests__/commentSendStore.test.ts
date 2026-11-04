/**
 * The comment-send store's transitions, and what the deck and a card read
 * from a mark: which comments leave the composer's next send, which draw as
 * sent, and which say why they were refused.
 */
import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ArtifactComment } from '@/types/api';
import {
  TAKEN_MS, leftQueue, nextMarks, sendLook, useCommentSendStore, type CommentSendMark, type Marks,
} from '../commentSendStore';

type Row = Pick<ArtifactComment, 'updated_at' | 'delivery' | 'status'>;
const row = (over: Partial<Row> = {}): Row => ({ updated_at: 'v1', delivery: 'queued', status: 'open', ...over });
const marks = (entries: [string, CommentSendMark][] = []): Marks => new Map(entries);

describe('nextMarks', () => {
  it('marks comments on their way at the version of their row', () => {
    expect(nextMarks(marks(), { type: 'sending', comments: [{ comment_id: 'c', updated_at: 'v1' }] }).get('c'))
      .toEqual({ phase: 'sending', at: 'v1' });
  });

  it('keeps the version a mark on its way holds when the taker never saw the row', () => {
    const was = marks([['c', { phase: 'sending', at: 'v1' }]]);
    expect(nextMarks(was, { type: 'taken', comments: [{ comment_id: 'c' }] }).get('c')).toEqual({ phase: 'taken', at: 'v1' });
    expect(nextMarks(marks(), { type: 'taken', comments: [{ comment_id: 'c' }] }).get('c')).toEqual({ phase: 'taken', at: null });
  });

  it('keeps that version too when the chat opens the send by id', () => {
    const was = marks([['c', { phase: 'sending', at: 'v1' }]]);
    expect(nextMarks(was, { type: 'sending', comments: [{ comment_id: 'c' }] }).get('c')).toEqual({ phase: 'sending', at: 'v1' });
  });

  it('releases only marks still on their way when a request closes unanswered', () => {
    const was = marks([
      ['a', { phase: 'sending', at: 'v1' }],
      ['b', { phase: 'taken', at: 'v1' }],
      ['c', { phase: 'refused', reason: 'busy' }],
    ]);
    const next = nextMarks(was, { type: 'release', ids: ['a', 'b', 'c'] });
    expect([next.has('a'), next.get('b'), next.get('c')]).toEqual([false, { phase: 'taken', at: 'v1' }, { phase: 'refused', reason: 'busy' }]);
    expect(nextMarks(next, { type: 'release', ids: ['b', 'c', 'z'] })).toBe(next);
  });

  it('takes a refused comment sent again: the refusal no longer stands', () => {
    const was = marks([['c', { phase: 'refused', reason: 'busy' }]]);
    expect(nextMarks(was, { type: 'taken', comments: [{ comment_id: 'c' }] }).get('c')).toEqual({ phase: 'taken', at: null });
  });

  it('refuses over a send on its way or taken', () => {
    const was = marks([['a', { phase: 'sending', at: 'v1' }], ['b', { phase: 'taken', at: 'v1' }]]);
    const next = nextMarks(was, { type: 'refused', ids: ['a', 'b'], reason: 'busy' });
    expect([next.get('a'), next.get('b')]).toEqual([{ phase: 'refused', reason: 'busy' }, { phase: 'refused', reason: 'busy' }]);
  });

  it('clears comments kept or queued again, and leaves the map alone when none had a mark', () => {
    const was = marks([['a', { phase: 'refused', reason: 'busy' }]]);
    expect(nextMarks(was, { type: 'clear', ids: ['a'] }).has('a')).toBe(false);
    expect(nextMarks(was, { type: 'clear', ids: ['z'] })).toBe(was);
  });

  it('expires a taken mark, never one that moved on to a send or a refusal', () => {
    expect(nextMarks(marks([['c', { phase: 'taken', at: 'v1' }]]), { type: 'expire', id: 'c' }).has('c')).toBe(false);
    const refused = marks([['c', { phase: 'refused', reason: 'busy' }]]);
    expect(nextMarks(refused, { type: 'expire', id: 'c' })).toBe(refused);
    const sending = marks([['c', { phase: 'sending', at: 'v2' }]]);
    expect(nextMarks(sending, { type: 'expire', id: 'c' })).toBe(sending);
  });
});

describe('leftQueue: what the composer\'s next send leaves out', () => {
  it.each<[string, CommentSendMark | undefined, Row, boolean]>([
    ['a comment with no send stays', undefined, row(), false],
    ['one on its way leaves', { phase: 'sending', at: 'v1' }, row(), true],
    ['one taken leaves while its row is the one sent', { phase: 'taken', at: 'v1' }, row(), true],
    ['one taken comes back once a list shows its row moved on', { phase: 'taken', at: 'v1' }, row({ updated_at: 'v2' }), false],
    ['one taken by id holds whatever the row', { phase: 'taken', at: null }, row({ updated_at: 'v2' }), true],
    ['one refused stays, with its reason', { phase: 'refused', reason: 'busy' }, row(), false],
  ])('%s', (_rule, mark, comment, left) => {
    expect(leftQueue(mark, comment)).toBe(left);
  });
});

describe('sendLook: how a card draws its send', () => {
  it('shows a queued comment\'s send on its button, then as sent once taken', () => {
    expect(sendLook({ phase: 'sending', at: 'v1' }, row())).toEqual({ sending: false, pending: true, refusal: null });
    expect(sendLook({ phase: 'taken', at: 'v1' }, row())).toEqual({ sending: true, pending: false, refusal: null });
  });

  it('draws a note as sent from the click, since it has no button to show the send', () => {
    expect(sendLook({ phase: 'sending', at: 'v1' }, row({ delivery: 'note' }))).toEqual({ sending: true, pending: false, refusal: null });
  });

  it('lets the server\'s row speak once it has moved on or says sent', () => {
    expect(sendLook({ phase: 'taken', at: 'v1' }, row({ updated_at: 'v2' })).sending).toBe(false);
    expect(sendLook({ phase: 'taken', at: 'v1' }, row({ delivery: 'sent' })).sending).toBe(false);
  });

  it('says why a send was refused while the comment still waits, note or queued', () => {
    const refused: CommentSendMark = { phase: 'refused', reason: 'busy' };
    expect(sendLook(refused, row()).refusal).toBe('busy');
    expect(sendLook(refused, row({ delivery: 'note' })).refusal).toBe('busy');
    expect(sendLook(refused, row({ delivery: 'sent' })).refusal).toBeNull();
    expect(sendLook(refused, row({ status: 'resolved' })).refusal).toBeNull();
  });
});

describe('useCommentSendStore', () => {
  afterEach(() => vi.useRealTimers());

  it('holds a send on its way for as long as its request is open, and times a taken mark from the server\'s answer', () => {
    vi.useFakeTimers();
    const { result } = renderHook(() => useCommentSendStore());
    act(() => result.current.sending([{ comment_id: 'a', updated_at: 'v1' }]));
    act(() => vi.advanceTimersByTime(TAKEN_MS * 3));
    expect(result.current.get('a')).toEqual({ phase: 'sending', at: 'v1' });
    act(() => result.current.taken([{ comment_id: 'a' }]));
    act(() => vi.advanceTimersByTime(TAKEN_MS - 1));
    expect(result.current.get('a')).toEqual({ phase: 'taken', at: 'v1' });
    act(() => vi.advanceTimersByTime(1));
    expect(result.current.get('a')).toBeUndefined();
  });

  it('says whether a release let anything go', () => {
    const { result } = renderHook(() => useCommentSendStore());
    act(() => result.current.sending([{ comment_id: 'a' }]));
    let released: boolean[] = [];
    act(() => { released = [result.current.release(['a']), result.current.release(['a'])]; });
    expect(released).toEqual([true, false]);
    expect(result.current.get('a')).toBeUndefined();
  });

  it('lets a taken mark go when no list confirms it in time, and keeps a later refusal', () => {
    vi.useFakeTimers();
    const { result } = renderHook(() => useCommentSendStore());
    act(() => result.current.taken([{ comment_id: 'a', updated_at: 'v1' }, { comment_id: 'b', updated_at: 'v1' }]));
    expect(result.current.get('a')).toEqual({ phase: 'taken', at: 'v1' });
    act(() => result.current.refused(['b'], 'busy'));
    act(() => vi.advanceTimersByTime(TAKEN_MS));
    expect(result.current.get('a')).toBeUndefined();
    expect(result.current.get('b')).toEqual({ phase: 'refused', reason: 'busy' });
  });

  it('times a comment taken again from its latest send', () => {
    vi.useFakeTimers();
    const { result } = renderHook(() => useCommentSendStore());
    act(() => result.current.taken([{ comment_id: 'a', updated_at: 'v1' }]));
    act(() => vi.advanceTimersByTime(TAKEN_MS - 1000));
    act(() => result.current.taken([{ comment_id: 'a' }]));
    act(() => vi.advanceTimersByTime(1000));
    expect(result.current.get('a')).toEqual({ phase: 'taken', at: 'v1' });
    act(() => vi.advanceTimersByTime(TAKEN_MS));
    expect(result.current.get('a')).toBeUndefined();
  });

  it('keeps its writes stable as its marks change', () => {
    const { result } = renderHook(() => useCommentSendStore());
    const { taken, refused } = result.current;
    act(() => result.current.sending([{ comment_id: 'a', updated_at: 'v1' }]));
    expect(result.current.taken).toBe(taken);
    expect(result.current.refused).toBe(refused);
  });
});
