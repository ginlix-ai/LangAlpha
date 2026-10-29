import { describe, it, expect, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useState } from 'react';
import { flushSync } from 'react-dom';
import { useAnimatedText } from '../animated-text';
import { onPageReturn } from '@/lib/pageVisibility';

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

  it('lands the catch-up cursor on a word boundary, never inside a word', () => {
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed ' },
    });
    // Eleven-char units: the raw snap point (length - 320) falls mid-word.
    const next = 'seed ' + Array.from({ length: 300 }, (_, i) => `word${String(i).padStart(7, "0")}`).join(' ') + ' ';
    act(() => rerender({ text: next }));
    // Within one word of the raw snap point, so the snap itself is what ran.
    expect(result.current.length).toBeGreaterThanOrEqual(next.length - 320 - 12);
    expect(result.current.length).toBeLessThan(next.length);
    expect(next.startsWith(result.current)).toBe(true);
    expect(result.current.endsWith(' ')).toBe(true);
  });

  it('never retracts revealed text when the catch-up snap lands inside a run', () => {
    // Past the word cap, so the mount's cursor already sits inside the run.
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'https://example.com/a/long/path' },
    });
    const shown = result.current;
    expect(shown).toBe('https:/');
    act(() => rerender({ text: 'https://example.com/a/long/path' + 'x'.repeat(1000) }));
    expect(result.current.startsWith(shown)).toBe(true);
    expect(result.current.length).toBeGreaterThanOrEqual(shown.length);
  });

  it('caps the word hold so a long unbroken run still reveals', () => {
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed ' },
    });
    act(() => rerender({ text: 'seed ' + 'a'.repeat(2000) }));
    // 2000 chars in one update is a catch-up; a boundary-only cursor would
    // have snapped back to the run start and shown nothing but the seed.
    // The snap trails the raw point by the cap, never more.
    expect(result.current.length).toBeGreaterThanOrEqual('seed '.length + 2000 - 320 - 24);
    expect(result.current.length).toBeLessThan('seed '.length + 2000 - 320);
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
      // Nobody watched it arrive: all of it is on screen, nothing left to type.
      expect(result.current).toBe(text);
    });

    it('shows a reply that ended while hidden in full', () => {
      const { result, rerender } = renderHook(({ text, enabled }) => useAnimatedText(text, { enabled }), {
        initialProps: { text: 'seed ', enabled: true },
      });
      Object.defineProperty(document, 'hidden', { configurable: true, value: true });
      const text = 'seed ' + words(60) + 'end.';
      act(() => rerender({ text, enabled: true }));
      expect(result.current).toBe('seed ' + words(60));
      act(() => rerender({ text, enabled: false }));
      expect(result.current).toBe(text);
    });

    it('shows what the hidden page held in the commit that brings it back', () => {
      const { result } = renderHook(() => {
        const [text, setText] = useState('seed ');
        return { shown: useAnimatedText(text, { enabled: true }), setText };
      });
      Object.defineProperty(document, 'hidden', { configurable: true, value: true });
      // A live-sized delta, which in view types out (see the first test).
      const text = 'seed ' + words(30);
      let atReturn = '';
      const off = onPageReturn(() => {
        flushSync(() => result.current.setText(text));
        atReturn = result.current.shown;
      });
      Object.defineProperty(document, 'hidden', { configurable: true, value: false });
      act(() => {
        document.dispatchEvent(new Event('visibilitychange'));
      });
      off();
      expect(atReturn).toBe(text);
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

  it('shows text that exists at mount in the first render, with nothing to type', () => {
    // A bubble that mounts with its reply already in (a reconnect catch-up)
    // must not commit empty first: without a paint between the two commits it
    // lays out at zero height, then jumps to full height as a layout shift.
    const renders: string[] = [];
    const text = 'seed ' + words(60);
    const { result } = renderHook(() => {
      const shown = useAnimatedText(text, { enabled: true });
      renders.push(shown);
      return shown;
    });
    expect(renders[0]).toBe(text);
    expect(result.current).toBe(text);
  });

  it('holds a word still arriving at mount and shows the rest in the first render', () => {
    // A batch can end inside a word, and the segment it mounts must not show
    // the stub ("Reven") that the typewriter holds back everywhere else.
    const renders: string[] = [];
    const text = 'seed ' + words(60) + 'Reven';
    const { result } = renderHook(() => {
      const shown = useAnimatedText(text, { enabled: true });
      renders.push(shown);
      return shown;
    });
    expect(renders[0]).toBe('seed ' + words(60));
    expect(result.current).toBe('seed ' + words(60));
  });

  it('trails a run still arriving at mount by the word cap, as later updates do', () => {
    const url = 'https://example.com/' + 'a'.repeat(40);
    const { result } = renderHook(() => useAnimatedText('see ' + url, { enabled: true }));
    expect(result.current).toBe('see ' + url.slice(0, -24));
  });

  it('never takes back text shown before the animation turned on', () => {
    // A block that streams while a tool call above it is still active is not
    // animated, so it shows all it has, a word still arriving included.
    const { result, rerender } = renderHook(({ text, enabled }) => useAnimatedText(text, { enabled }), {
      initialProps: { text: 'seed Reven', enabled: false },
    });
    act(() => rerender({ text: 'seed Revenu', enabled: true }));
    expect(result.current).toBe('seed Reven');
  });

  it('shows a finished message in full when it was never animating', () => {
    const { result } = renderHook(() => useAnimatedText('a finished reply', { enabled: false }));
    expect(result.current).toBe('a finished reply');
  });
});
