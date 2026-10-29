import { afterEach, describe, expect, it, vi } from 'vitest';
import { HANDOFF_CAP_MS, handoffAnimates, markHandoffReady, runHandoff } from '../welcome/handoff';

/** A browser's view transition as far as the hand-off reads one: the update
 *  is called a moment after the start, and the transition finishes once the
 *  update's promise settles. */
function stubTransitions({ fail = false } = {}) {
  const starts: { flagged: boolean; done: Promise<void> }[] = [];
  const start = vi.fn((update: () => Promise<void> | void) => {
    const done = Promise.resolve().then(update);
    starts.push({ flagged: document.documentElement.hasAttribute('data-handoff'), done });
    if (fail) {
      const skipped = () => Promise.reject(new DOMException('Skipped', 'InvalidStateError'));
      done.catch(() => {});
      return { ready: skipped(), updateCallbackDone: skipped(), finished: skipped() };
    }
    return { ready: done, updateCallbackDone: done, finished: done.then(() => undefined) };
  });
  Object.defineProperty(document, 'startViewTransition', { configurable: true, writable: true, value: start });
  return { start, starts };
}

function prefersLessMotion() {
  const real = window.matchMedia;
  vi.spyOn(window, 'matchMedia').mockImplementation((query: string) => ({
    ...real(query),
    matches: query === '(prefers-reduced-motion: reduce)',
  }));
}

/** Lets the stubbed transition's promise chains run out. */
const settled = () => new Promise((resolve) => setTimeout(resolve, 0));

afterEach(() => {
  // A test that leaves a hand-off waiting would hand it to the next one.
  markHandoffReady();
  Reflect.deleteProperty(document, 'startViewTransition');
  document.documentElement.removeAttribute('data-handoff');
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe('runHandoff', () => {
  it('makes the update at once where the browser has no view transitions', () => {
    const update = vi.fn();
    expect(handoffAnimates()).toBe(false);
    runHandoff(update);
    expect(update).toHaveBeenCalledTimes(1);
    expect(document.documentElement.hasAttribute('data-handoff')).toBe(false);
  });

  it('makes the update at once for a reader who asked for less motion', () => {
    const { start } = stubTransitions();
    prefersLessMotion();
    const update = vi.fn();
    expect(handoffAnimates()).toBe(false);
    runHandoff(update);
    expect(update).toHaveBeenCalledTimes(1);
    expect(start).not.toHaveBeenCalled();
    expect(document.documentElement.hasAttribute('data-handoff')).toBe(false);
  });

  it('flags the page, makes the update inside the transition, and holds it until the chat is ready', async () => {
    const { start, starts } = stubTransitions();
    const update = vi.fn();
    expect(handoffAnimates()).toBe(true);
    runHandoff(update);
    expect(start).toHaveBeenCalledTimes(1);
    // The names are given before the browser takes its picture of the page.
    expect(starts[0].flagged).toBe(true);
    expect(update).not.toHaveBeenCalled();

    let done = false;
    void starts[0].done.then(() => (done = true));
    await settled();
    expect(update).toHaveBeenCalledTimes(1);
    expect(done).toBe(false);
    expect(document.documentElement.hasAttribute('data-handoff')).toBe(true);

    markHandoffReady();
    await settled();
    expect(done).toBe(true);
    // Off once the transition has finished, so no name outlasts it.
    expect(document.documentElement.hasAttribute('data-handoff')).toBe(false);
  });

  it('lets the page go at the cap when the chat never says it is ready', async () => {
    vi.useFakeTimers();
    const { starts } = stubTransitions();
    runHandoff(vi.fn());
    let done = false;
    void starts[0].done.then(() => (done = true));

    await vi.advanceTimersByTimeAsync(HANDOFF_CAP_MS - 1);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(done).toBe(true);
    expect(document.documentElement.hasAttribute('data-handoff')).toBe(false);
    // A ready that comes late has nothing left to release.
    expect(() => markHandoffReady()).not.toThrow();
  });

  it('a ready with no hand-off waiting does nothing', () => {
    expect(() => markHandoffReady()).not.toThrow();
  });

  it('swallows a skipped transition and still takes the flag off', async () => {
    const unhandled = vi.fn();
    process.on('unhandledRejection', unhandled);
    try {
      const { starts } = stubTransitions({ fail: true });
      const update = vi.fn();
      runHandoff(update);
      await settled();
      expect(update).toHaveBeenCalledTimes(1);
      markHandoffReady();
      await starts[0].done;
      await settled();
      expect(document.documentElement.hasAttribute('data-handoff')).toBe(false);
      expect(unhandled).not.toHaveBeenCalled();
    } finally {
      process.off('unhandledRejection', unhandled);
    }
  });

  it('makes the update itself when the transition cannot start', () => {
    const update = vi.fn();
    Object.defineProperty(document, 'startViewTransition', {
      configurable: true,
      writable: true,
      value: () => {
        throw new DOMException('Not now', 'InvalidStateError');
      },
    });
    runHandoff(update);
    expect(update).toHaveBeenCalledTimes(1);
    expect(document.documentElement.hasAttribute('data-handoff')).toBe(false);
  });
});
