/**
 * The scroller's own height changes with an on-screen keyboard, which moves the
 * end with nothing scrolled. A reader at the end stays there through it, in both
 * directions; one reading mid-thread stays on what they were reading.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { holdThroughViewResize, type TranscriptFollow } from '../transcriptFollow';

const TALL = 714;
const SHORT = 385;
const CONTENT = 2000;

let fire: () => void = () => {};
const OriginalRO = window.ResizeObserver;

/** A scroller that clamps as a browser does when its height changes. */
function scroller(top: number) {
  const c = document.createElement('div');
  let height = TALL;
  let scrollHeight = CONTENT;
  let scrollTop = top;
  Object.defineProperties(c, {
    clientHeight: { get: () => height },
    scrollHeight: { get: () => scrollHeight },
    scrollTop: { get: () => scrollTop },
  });
  return {
    c,
    resize(next: number) {
      height = next;
      scrollTop = Math.min(scrollTop, Math.max(0, scrollHeight - height));
      fire();
    },
    setScrollHeight(next: number) {
      scrollHeight = next;
    },
  };
}

function followOn({ reapply = false, landing = false } = {}) {
  const engine = { resized: vi.fn(), jump: vi.fn(), landing };
  return { engine, follow: { engine, reapply: vi.fn(() => reapply) } as unknown as TranscriptFollow };
}

beforeEach(() => {
  window.ResizeObserver = class {
    constructor(cb: () => void) {
      fire = cb;
    }
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

afterEach(() => {
  window.ResizeObserver = OriginalRO;
});

describe('holdThroughViewResize', () => {
  it('keeps a reader at the end there when the view shortens', () => {
    const view = scroller(CONTENT - TALL);
    const { engine, follow } = followOn();
    holdThroughViewResize(view.c, follow);

    view.resize(SHORT);

    expect(engine.jump).toHaveBeenCalledWith(CONTENT - SHORT);
  });

  it('leaves a reader mid-thread on what they were reading', () => {
    const view = scroller(500);
    const { engine, follow } = followOn();
    holdThroughViewResize(view.c, follow);

    view.resize(SHORT);

    expect(engine.jump).not.toHaveBeenCalled();
  });

  it('carries a reader the browser clamped to the end onto a floor raised for the new height', () => {
    const view = scroller(CONTENT - SHORT);
    const { engine, follow } = followOn();
    // Shortened by the keyboard, so the height the observer last saw is SHORT.
    view.resize(SHORT);
    engine.jump.mockClear();
    holdThroughViewResize(view.c, follow, {
      settle: () => view.setScrollHeight(CONTENT + 329),
    });

    view.resize(TALL);

    expect(engine.jump).toHaveBeenCalledWith(CONTENT + 329 - TALL);
  });

  it('leaves a mid-thread reader alone when the view grows', () => {
    const view = scroller(500);
    const { engine, follow } = followOn();
    view.resize(SHORT);
    holdThroughViewResize(view.c, follow);

    view.resize(TALL);

    expect(engine.jump).not.toHaveBeenCalled();
  });

  it('gives way to a held place and to a landing', () => {
    const held = scroller(CONTENT - TALL);
    const pinned = followOn({ reapply: true });
    holdThroughViewResize(held.c, pinned.follow);
    held.resize(SHORT);
    expect(pinned.follow.reapply).toHaveBeenCalledWith(0);
    expect(pinned.engine.jump).not.toHaveBeenCalled();

    const landing = scroller(CONTENT - TALL);
    const gliding = followOn({ landing: true });
    holdThroughViewResize(landing.c, gliding.follow);
    landing.resize(SHORT);
    expect(gliding.engine.jump).not.toHaveBeenCalled();
  });

  it('does not move the view before the thread entry has landed', () => {
    const view = scroller(CONTENT - TALL);
    const { engine, follow } = followOn();
    holdThroughViewResize(view.c, follow, { ready: () => false });

    view.resize(SHORT);

    expect(engine.jump).not.toHaveBeenCalled();
  });

  it('ignores a hidden view, and reads no change when it comes back', () => {
    const view = scroller(CONTENT - TALL);
    const { engine, follow } = followOn();
    holdThroughViewResize(view.c, follow);

    view.resize(0);
    view.resize(TALL);

    expect(engine.resized).not.toHaveBeenCalled();
    expect(engine.jump).not.toHaveBeenCalled();
  });
});
