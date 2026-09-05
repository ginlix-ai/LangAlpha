import { describe, it, expect, vi, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';

// framer's stop() runs one last tick at the wall clock before tearing down, so
// a chain older than its duration finishes inside stop() and fires onComplete
// synchronously, and a stop() on an already finished animation fires it
// again (JSAnimation.tick treats state "finished" as done). Routine in a
// hidden tab, where the effect re-runs once a second. This mock reproduces
// exactly that, with a clock the test advances by hand.
type Opts = { duration: number; onUpdate: (v: number) => void; onComplete: () => void };
let clock = 0;
const live = new Set<object>();
vi.mock('framer-motion', () => ({
  animate: (from: number, to: number, opts: Opts) => {
    const createdAt = clock;
    let finished = false;
    const controls = {
      stop: () => {
        live.delete(controls);
        const elapsed = (clock - createdAt) / 1000;
        if (finished || elapsed >= opts.duration) {
          finished = true;
          opts.onUpdate(to);
          opts.onComplete();
        } else {
          opts.onUpdate(from + (to - from) * (elapsed / opts.duration));
        }
      },
    };
    live.add(controls);
    return controls;
  },
}));

import { useAnimatedText } from '../animated-text';

const words = (n: number) => Array.from({ length: n }, (_, i) => `w${i}`).join(' ') + ' ';

describe('useAnimatedText chain ownership', () => {
  afterEach(() => {
    Object.defineProperty(document, 'hidden', { configurable: true, value: false });
  });

  it('a chain finished by its own stop() never starts a sibling', () => {
    const { result, rerender } = renderHook(({ text }) => useAnimatedText(text, { enabled: true }), {
      initialProps: { text: 'seed ' },
    });
    // Hidden, seconds between updates (Chrome wakes a hidden tab once a second
    // at best, once a minute under intensive throttling), more than a snap's
    // worth of text in each: the cleanup stop finishes the chain, then the
    // backlog snap stops it again after the target has moved on. That second
    // stop used to spawn a sibling chain aimed at the old target.
    Object.defineProperty(document, 'hidden', { configurable: true, value: true });
    let text = 'seed ';
    for (let i = 0; i < 6; i++) {
      clock += 3000;
      text += words(170); // ~740 chars, over CATCH_UP_CHARS
      act(() => rerender({ text }));
      expect(live.size).toBe(1);
    }
    expect(text.startsWith(result.current)).toBe(true);
  });
});
