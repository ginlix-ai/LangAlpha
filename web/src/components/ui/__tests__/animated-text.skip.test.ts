import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';

// framer with its animations skipped (a hidden tab, see lib/framer) resolves an
// animation's `finished` at once, so animate()'s onComplete runs in a microtask,
// but applies the final value only on its next frame: on return, after the
// completion. This mock keeps exactly that order; `frames` is framer's queue.
type Opts = { onUpdate: (v: number) => void; onComplete: () => void };
let frames: (() => void)[] = [];
vi.mock('@/lib/framer', () => ({
  animate: (_from: number, to: number, opts: Opts) => {
    frames.push(() => opts.onUpdate(to));
    queueMicrotask(opts.onComplete);
    return { stop: () => {} };
  },
}));

import { useAnimatedText } from '../animated-text';

const words = (n: number) => Array.from({ length: n }, (_, i) => `w${i}`).join(' ') + ' ';

describe('useAnimatedText with framer skipping animations', () => {
  beforeEach(() => {
    frames = [];
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('keeps the last word of a reply that ended while hidden', async () => {
    const { result, rerender } = renderHook(({ text, enabled }) => useAnimatedText(text, { enabled }), {
      initialProps: { text: 'seed ', enabled: true },
    });
    const text = 'seed ' + words(10) + 'end.';
    await act(async () => rerender({ text, enabled: true }));
    await act(async () => rerender({ text, enabled: false }));
    expect(result.current).toBe(text);
    // The page comes back and framer runs the frame it queued.
    act(() => {
      for (const run of frames.splice(0)) run();
    });
    expect(result.current).toBe(text);
  });

  // Every chain completes at once here, so each step lands where its chain ends.
  it('reveals a word held at mount once the text goes past it', async () => {
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed Reven' },
    });
    expect(result.current).toBe('seed ');
    await act(async () => rerender({ text: 'seed Revenue grew' }));
    expect(result.current).toBe('seed Revenue ');
  });

  it('reveals a word held at mount when the stream ends', async () => {
    const { result, rerender } = renderHook(({ text, enabled }) => useAnimatedText(text, { enabled }), {
      initialProps: { text: 'seed Reven', enabled: true },
    });
    expect(result.current).toBe('seed ');
    await act(async () => rerender({ text: 'seed Reven', enabled: false }));
    act(() => {
      for (const run of frames.splice(0)) run();
    });
    expect(result.current).toBe('seed Reven');
  });

  // A stream that pauses with its last word still open (the model thinking,
  // a tool call starting) must not keep that word hidden for the whole pause.
  it('types out a word held at mount once the stream goes quiet, and holds again when it resumes', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed Review underway.' },
    });
    expect(result.current).toBe('seed Review ');
    await act(async () => { vi.advanceTimersByTime(999); });
    expect(result.current).toBe('seed Review ');
    await act(async () => { vi.advanceTimersByTime(1); });
    expect(result.current).toBe('seed Review underway.');
    await act(async () => rerender({ text: 'seed Review underway. Next' }));
    expect(result.current).toBe('seed Review underway. ');
  });

  it('types out a word held mid-stream once the stream goes quiet', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed ' },
    });
    await act(async () => rerender({ text: 'seed Review underway.' }));
    expect(result.current).toBe('seed Review ');
    // Each update restarts the wait.
    await act(async () => { vi.advanceTimersByTime(900); });
    await act(async () => rerender({ text: 'seed Review underway. Then' }));
    await act(async () => { vi.advanceTimersByTime(900); });
    expect(result.current).toBe('seed Review underway. ');
    await act(async () => { vi.advanceTimersByTime(100); });
    expect(result.current).toBe('seed Review underway. Then');
  });
});
