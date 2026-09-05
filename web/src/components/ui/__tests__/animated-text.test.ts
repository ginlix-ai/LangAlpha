import { describe, it, expect, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useAnimatedText } from '../animated-text';

const words = (n: number) => Array.from({ length: n }, (_, i) => `w${i}`).join(' ') + ' ';

// These pin the snap thresholds only. jsdom gives framer-motion no frames, so
// the reveal itself never advances here; e2e/perf/typewriter.spec.js covers it.

describe('useAnimatedText catch-up', () => {
  it('types a live-sized delta instead of snapping', () => {
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed ' },
    });
    expect(result.current).toBe('seed ');
    const next = 'seed ' + words(30); // ~150 chars, one short chain
    act(() => rerender({ text: next }));
    expect(result.current.length).toBeLessThan(next.length);
  });

  it('snaps a backlog that lands in one update and keeps typing only the tail', () => {
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed ' },
    });
    const next = 'seed ' + words(400); // ~2.4k chars: a replay or a hidden-tab backlog
    act(() => rerender({ text: next }));
    // Everything but the last few hundred characters is shown at once.
    expect(result.current.length).toBeGreaterThanOrEqual(next.length - 320);
    expect(result.current.length).toBeLessThan(next.length);
    expect(next.startsWith(result.current)).toBe(true);
  });

  it('keeps typing when a fast stream builds the same backlog chunk by chunk', () => {
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed ' },
    });
    let text = 'seed ';
    for (let i = 0; i < 20; i++) {
      text += words(20); // ~100 chars per chunk, 2k in total with no frame in between
      act(() => rerender({ text }));
    }
    // The reveal lags, but nothing jumps ahead to the tail.
    expect(result.current.length).toBeLessThan(text.length - 320);
    expect(text.startsWith(result.current)).toBe(true);
  });

  describe('hidden tab', () => {
    afterEach(() => {
      Object.defineProperty(document, 'hidden', { configurable: true, value: false });
    });

    it('snaps a backlog that arrived while nobody was watching', () => {
      const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
        initialProps: { text: 'seed ' },
      });
      Object.defineProperty(document, 'hidden', { configurable: true, value: true });
      let text = 'seed ';
      for (let i = 0; i < 20; i++) {
        text += words(20);
        act(() => rerender({ text }));
      }
      // The lag is bounded by the snap threshold plus one chunk, not the whole backlog.
      expect(result.current.length).toBeGreaterThanOrEqual(text.length - 720);
      expect(text.startsWith(result.current)).toBe(true);
    });
  });

  it('types out the hidden tail when the stream ends instead of popping it in', () => {
    const { result, rerender } = renderHook(({ text, enabled }) => useAnimatedText(text, { enabled }), {
      initialProps: { text: 'seed ', enabled: true },
    });
    const text = 'seed ' + words(60);
    act(() => rerender({ text, enabled: true }));
    expect(result.current.length).toBeLessThan(text.length);
    act(() => rerender({ text, enabled: false }));
    // The reveal keeps going from where it was rather than jumping to the end.
    expect(result.current.length).toBeLessThan(text.length);
    expect(text.startsWith(result.current)).toBe(true);
  });

  it('shows a finished message in full when it was never animating', () => {
    const { result } = renderHook(() => useAnimatedText('a finished reply', { enabled: false }));
    expect(result.current).toBe('a finished reply');
  });
});
