import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createFrameQueue } from '../frameQueue';
import { isPageUnseen } from '@/lib/pageVisibility';
import { mockFrames, pendingFrames, runFrame, setVisibility, settleFrames } from '@/test/frames';

type Update = (prev: string[]) => string[];

function setup() {
  let state: string[] = [];
  const applied: Update[] = [];
  const unseen: boolean[] = [];
  const apply = (update: Update) => {
    applied.push(update);
    unseen.push(isPageUnseen());
    state = update(state);
  };
  const q = createFrameQueue<string[]>(apply);
  const push = (s: string): Update => (prev) => [...prev, s];
  return { q, push, applied, unseen, read: () => state };
}

beforeEach(() => {
  mockFrames();
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
});

afterEach(() => {
  settleFrames();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('createFrameQueue', () => {
  it('applies everything queued before a frame as one update, in order', () => {
    const { q, push, applied, read } = setup();
    q.queue(push('a'));
    q.queue(push('b'));
    q.queue(push('c'));
    expect(read()).toEqual([]);
    runFrame();
    expect(applied).toHaveLength(1);
    expect(read()).toEqual(['a', 'b', 'c']);
  });

  it('flush applies the queue now and leaves the frame nothing to apply', () => {
    const { q, push, applied, read } = setup();
    q.queue(push('a'));
    q.queue(push('b'));
    q.flush();
    expect(applied).toHaveLength(1);
    expect(read()).toEqual(['a', 'b']);
    runFrame();
    q.flush();
    expect(applied).toHaveLength(1);
  });

  it('batches a hidden page on a timer, where no frame would come', () => {
    const { q, push, applied, read } = setup();
    setVisibility('hidden');
    q.queue(push('a'));
    q.queue(push('b'));
    expect(read()).toEqual([]);
    expect(pendingFrames()).toBe(0);
    vi.advanceTimersByTime(500);
    expect(applied).toHaveLength(1);
    expect(read()).toEqual(['a', 'b']);
  });

  it('applies what is waiting when the page hides', () => {
    const { q, push, read } = setup();
    q.queue(push('a'));
    setVisibility('hidden');
    expect(read()).toEqual(['a']);
  });

  it('applies what the hidden page held on its return, at once and still unseen', () => {
    const { q, push, applied, unseen, read } = setup();
    setVisibility('hidden');
    q.queue(push('a'));
    setVisibility('visible');
    expect(read()).toEqual(['a']);
    expect(unseen).toEqual([true]);
    expect(isPageUnseen()).toBe(false);
    vi.advanceTimersByTime(500);
    runFrame();
    expect(applied).toHaveLength(1);
  });

  it('serves every queue from one frame callback', () => {
    const one = setup();
    const two = setup();
    one.q.queue(one.push('a'));
    two.q.queue(two.push('b'));
    expect(pendingFrames()).toBe(1);
    runFrame();
    expect(one.read()).toEqual(['a']);
    expect(two.read()).toEqual(['b']);
  });

  it('drains at most once per 60 Hz frame on a faster screen', () => {
    const { q, push, applied, read } = setup();
    q.queue(push('a'));
    runFrame();
    expect(read()).toEqual(['a']);

    q.queue(push('b'));
    runFrame(1000 / 120);
    expect(applied).toHaveLength(1);
    expect(pendingFrames()).toBe(1);
    runFrame(1000 / 120);
    expect(read()).toEqual(['a', 'b']);
  });

  it('cancel drops what is queued', () => {
    const { q, push, applied } = setup();
    q.queue(push('a'));
    q.cancel();
    runFrame();
    expect(applied).toHaveLength(0);
  });
});
