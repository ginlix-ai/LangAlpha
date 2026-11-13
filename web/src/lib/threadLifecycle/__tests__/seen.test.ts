/**
 * The watched-seen guard: a thread on screen is stamped seen only while a
 * person can be looking at the page, and a finish that lands while away is
 * stamped on return only for the thread still open, at its newest run.
 */
import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const post = vi.fn();
vi.mock('@/api/client', () => ({ api: { post: (...a: unknown[]) => post(...a) } }));

import { isPageWatched, isSeenUnconfirmed, markThreadSeen, markWatchedThreadSeen, RETURN_POLL_MS } from '../seen';
import { runAuthResets } from '@/lib/authResets';
import { applyFeedEvent, getSeenThroughSeq, resetThreadLifecycle } from '../store';
import { useActiveThreadPublisher } from '../useActiveThreadPublisher';

let focused = true;
let visibility: DocumentVisibilityState = 'visible';

beforeEach(() => {
  post.mockReset();
  post.mockResolvedValue({ data: { last_seen_run_seq: 1, latest_run_seq: 1 } });
  resetThreadLifecycle();
  focused = true;
  visibility = 'visible';
  vi.spyOn(document, 'hasFocus').mockImplementation(() => focused);
  vi.spyOn(document, 'visibilityState', 'get').mockImplementation(() => visibility);
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const start = (threadId: string, runId: string, seq: number) =>
  act(() => {
    applyFeedEvent({ type: 'run_started', thread_id: threadId, run_id: runId, run_seq: seq, status: 'running' });
  });
const settle = (threadId: string, runId: string, seq: number) =>
  act(() => {
    applyFeedEvent({ type: 'run_settled', thread_id: threadId, run_id: runId, run_seq: seq, status: 'completed' });
  });
const fire = (target: EventTarget, event: Event) =>
  act(() => {
    target.dispatchEvent(event);
  });
const open = (threadId: string) =>
  renderHook(({ id }) => useActiveThreadPublisher(id), { initialProps: { id: threadId } });

describe('markWatchedThreadSeen', () => {
  it('stamps at once while the page is focused and visible', () => {
    markWatchedThreadSeen('t1', 'r1');
    expect(post).toHaveBeenCalledWith('/api/v1/threads/t1/seen', { run_id: 'r1' });
  });

  it('stamps nothing while the page is away', () => {
    focused = false;
    markWatchedThreadSeen('t1', 'r1');
    visibility = 'hidden';
    focused = true;
    markWatchedThreadSeen('t1', 'r1');
    expect(post).not.toHaveBeenCalled();
  });
});

describe('a stamp the server never answered', () => {
  it('stays owed when an older stamp for the thread lands after it', async () => {
    let answerOlder: (v: unknown) => void = () => {};
    post
      .mockImplementationOnce(() => new Promise((resolve) => { answerOlder = resolve; }))
      .mockRejectedValueOnce(new Error('Network Error'));
    markThreadSeen('t9', 'r1');
    markThreadSeen('t9', 'r2');
    await vi.waitFor(() => expect(isSeenUnconfirmed('t9')).toBe(true));
    answerOlder({ data: { last_seen_run_seq: 1, latest_run_seq: 2 } });
    await act(async () => {});
    expect(isSeenUnconfirmed('t9')).toBe(true);

    // A newer stamp's answer settles it.
    markThreadSeen('t9', 'r2');
    await vi.waitFor(() => expect(isSeenUnconfirmed('t9')).toBe(false));
  });

  it('forgets a stamp sent before a sign-out, and sends no repeat of it after', async () => {
    let fail: (e: unknown) => void = () => {};
    post.mockImplementationOnce(() => new Promise((_resolve, reject) => { fail = reject; }));
    markThreadSeen('t7', 'r1');
    markThreadSeen('t7', 'r1'); // queued behind the first
    runAuthResets();
    fail(new Error('Network Error'));
    await act(async () => {});
    expect(isSeenUnconfirmed('t7')).toBe(false);
    expect(post).toHaveBeenCalledTimes(1);
  });

  it('sends a stamp made after signing back in while the old session\'s copy is still out', async () => {
    let fail: (e: unknown) => void = () => {};
    post.mockImplementationOnce(() => new Promise((_resolve, reject) => { fail = reject; }));
    markThreadSeen('t6', 'r1');
    runAuthResets();
    markThreadSeen('t6', 'r1');
    expect(post).toHaveBeenCalledTimes(2);
    fail(new Error('Network Error'));
    await act(async () => {});
    expect(isSeenUnconfirmed('t6')).toBe(false);
  });

  it('a repeat sent after a newer run finished leaves that run unread', async () => {
    settle('t5', 'r1', 1);
    let answer: (v: unknown) => void = () => {};
    post.mockImplementationOnce(() => new Promise((resolve) => { answer = resolve; }));
    markThreadSeen('t5', 'r1');
    markThreadSeen('t5', 'r1'); // queued behind the first
    expect(getSeenThroughSeq('t5')).toBe(1);
    start('t5', 'r2', 2);
    settle('t5', 'r2', 2);
    answer({ data: { last_seen_run_seq: 1, latest_run_seq: 2 } });
    await vi.waitFor(() => expect(post).toHaveBeenCalledTimes(2));
    await act(async () => {});
    expect(getSeenThroughSeq('t5')).toBe(1);
  });

  it('stays owed when a repeat of an older stamp is answered after it', async () => {
    let answerOlder: (v: unknown) => void = () => {};
    post
      .mockImplementationOnce(() => new Promise((resolve) => { answerOlder = resolve; }))
      .mockRejectedValueOnce(new Error('Network Error'));
    markThreadSeen('t8', 'r1');
    markThreadSeen('t8', 'r1'); // asked again while out: goes once the first is back
    markThreadSeen('t8', 'r2');
    await vi.waitFor(() => expect(isSeenUnconfirmed('t8')).toBe(true));
    answerOlder({ data: { last_seen_run_seq: 1, latest_run_seq: 2 } });
    await vi.waitFor(() => expect(post).toHaveBeenCalledTimes(3));
    await act(async () => {});
    expect(isSeenUnconfirmed('t8')).toBe(true);
  });
});

describe('the open thread', () => {
  it('is stamped as its run finishes while watched', () => {
    open('t1');
    settle('t1', 'r1', 1);
    expect(post).toHaveBeenCalledTimes(1);
    expect(post).toHaveBeenCalledWith('/api/v1/threads/t1/seen', { run_id: 'r1' });
  });

  it('holds the stamp while unfocused and makes it on return, at the newest finished run', () => {
    open('t1');
    focused = false;
    settle('t1', 'r1', 1);
    expect(post).not.toHaveBeenCalled();

    // Another run finished while away: coming back shows that one.
    start('t1', 'r2', 2);
    settle('t1', 'r2', 2);
    focused = true;
    fire(window, new Event('focus'));
    expect(post).toHaveBeenCalledTimes(1);
    expect(post).toHaveBeenCalledWith('/api/v1/threads/t1/seen', { run_id: 'r2' });
  });

  it('holds it while hidden, and drops it when another thread is open on return', () => {
    const view = open('t1');
    visibility = 'hidden';
    settle('t1', 'r1', 1);
    view.rerender({ id: 't2' });
    visibility = 'visible';
    fire(document, new Event('visibilitychange'));
    expect(post).not.toHaveBeenCalled();
  });

  it('waits for both focus and visibility', () => {
    open('t1');
    focused = false;
    visibility = 'hidden';
    settle('t1', 'r1', 1);
    visibility = 'visible';
    fire(document, new Event('visibilitychange'));
    expect(post).not.toHaveBeenCalled();
    focused = true;
    fire(window, new Event('focus'));
    expect(post).toHaveBeenCalledWith('/api/v1/threads/t1/seen', { run_id: 'r1' });
  });

  it('notices a return into a frame, which raises no event on the page', () => {
    vi.useFakeTimers();
    open('t1');
    focused = false;
    settle('t1', 'r1', 1);
    act(() => vi.advanceTimersByTime(RETURN_POLL_MS * 3));
    expect(post).not.toHaveBeenCalled();

    // Cmd+Tab back into an artifact's iframe: hasFocus() turns true, nothing fires.
    focused = true;
    act(() => vi.advanceTimersByTime(RETURN_POLL_MS));
    expect(post).toHaveBeenCalledTimes(1);
    expect(post).toHaveBeenCalledWith('/api/v1/threads/t1/seen', { run_id: 'r1' });

    // Stamped: nothing is owed, so the poll stops with it.
    expect(vi.getTimerCount()).toBe(0);
  });

  it('takes a click or focus inside the page as a return', () => {
    open('t1');
    focused = false;
    settle('t1', 'r1', 1);
    fire(document, new Event('pointerdown'));
    expect(post).not.toHaveBeenCalled();
    focused = true;
    fire(document.body, new FocusEvent('focusin', { bubbles: true }));
    expect(post).toHaveBeenCalledWith('/api/v1/threads/t1/seen', { run_id: 'r1' });
  });

  it('does not stamp again on every return once read', () => {
    open('t1');
    settle('t1', 'r1', 1);
    expect(post).toHaveBeenCalledTimes(1);
    focused = false;
    fire(window, new Event('blur'));
    focused = true;
    fire(window, new Event('focus'));
    expect(post).toHaveBeenCalledTimes(1);
  });

  it('listens for a return only while a stamp is owed', () => {
    vi.useFakeTimers();
    open('t1');
    focused = false;
    fire(window, new Event('blur'));
    act(() => vi.advanceTimersByTime(RETURN_POLL_MS * 3));
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('isPageWatched', () => {
  it('needs the page visible and focused', () => {
    expect(isPageWatched()).toBe(true);
    focused = false;
    expect(isPageWatched()).toBe(false);
    focused = true;
    visibility = 'hidden';
    expect(isPageWatched()).toBe(false);
  });
});

describe('a stamp the server never answered', () => {
  it('is sent again the next time the thread is opened', async () => {
    post.mockRejectedValueOnce(new Error('Network Error'));
    const first = open('t9');
    settle('t9', 'r9', 1);
    await act(async () => {});
    expect(post).toHaveBeenCalledTimes(1);
    first.unmount();

    open('t9');
    await act(async () => {});
    expect(post).toHaveBeenCalledTimes(2);
    // Answered now, so a third open owes nothing.
    cleanup();
    open('t9');
    await act(async () => {});
    expect(post).toHaveBeenCalledTimes(2);
  });

  it('a second ask while the first is out goes once the first is back', async () => {
    let answer: (value: unknown) => void = () => {};
    post.mockImplementationOnce(() => new Promise((resolve) => { answer = resolve; }));
    markWatchedThreadSeen('t8', 'r8');
    markWatchedThreadSeen('t8', 'r8');
    expect(post).toHaveBeenCalledTimes(1);
    await act(async () => { answer({ data: { last_seen_run_seq: 0, latest_run_seq: 1 } }); });
    expect(post).toHaveBeenCalledTimes(2);
  });
});
