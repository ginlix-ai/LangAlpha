/**
 * The file panel opening beside the chat narrows the transcript, and every
 * line rewraps taller each frame it narrows; closing it does the reverse. That
 * is a relayout, not news: a reader at the end stays at the end, and one
 * mid-thread stays on the row at the top of the view. Growth with no change of
 * width is still news, followed only while a turn runs.
 *
 * jsdom has no layout, so the transcript is a model: one bubble of five rows
 * whose height `rowH` stands in for the column's width, and a tail below it.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatScroll } from '../useChatScroll';

const VIEW_H = 500;
const ROWS = 5;
const TAIL = 300;
let rowH = 400;
let width = 1180;
const contentH = () => ROWS * rowH + TAIL;

const rect = (top: number, height: number) =>
  ({ top, bottom: top + height, height, left: 0, right: 0, width: 0, x: 0, y: 0, toJSON: () => {} }) as DOMRect;

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;
const scrolled = () => viewport().scrollTop;

const measureViewport = (el: HTMLElement | null) => {
  if (!el) return;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { get: contentH, configurable: true });
  el.getBoundingClientRect = () => rect(0, VIEW_H);
};
const measureBubble = (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => rect(-scrolled(), ROWS * rowH);
};
const measureRow = (i: number) => (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => rect(i * rowH - scrolled(), rowH);
};
const rowTop = (i: number) => document.querySelector<HTMLElement>(`[data-row="${i}"]`)!.getBoundingClientRect().top;

let resize: () => void = () => {};
let scroll: ReturnType<typeof useChatScroll>;

function Harness({ isStreaming = false }: { isStreaming?: boolean }) {
  const api = useChatScroll({
    activeAgentId: 'main',
    messages: [{ id: 'a0' }],
    isActive: true,
    isActiveRef: { current: true },
    isLoadingHistory: false,
    historyLoadFailed: false,
    isStreaming,
    currentThreadId: 't1',
    threadId: 't1',
  });
  React.useEffect(() => { scroll = api; });
  // Passed on its own: read off the hook's whole return, the ref looks like a
  // value read during render.
  const { scrollAreaRef } = api;
  return (
    <div ref={scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div data-message-id="a0" ref={measureBubble}>
          {Array.from({ length: ROWS }, (_, i) => <p key={i} data-row={i} ref={measureRow(i)} />)}
        </div>
      </div>
    </div>
  );
}

/** Clamped the way a browser clamps, so a request past the end lands on it. */
function scrollTo(top: number) {
  const clamped = Math.max(0, Math.min(top, contentH() - VIEW_H));
  Object.defineProperty(viewport(), 'scrollTop', { value: clamped, writable: true, configurable: true });
  viewport().dispatchEvent(new Event('scroll'));
}

/** One frame of layout: the column at `w`, its rows `h` tall. The view stays
 *  where it was unless that is past the new end, as with no scroll anchoring. */
function frame(w: number, h: number) {
  width = w;
  rowH = h;
  const max = contentH() - VIEW_H;
  if (scrolled() > max) Object.defineProperty(viewport(), 'scrollTop', { value: max, writable: true, configurable: true });
  act(() => { resize(); });
}

const atEnd = () => contentH() - VIEW_H - scrolled();

/** Past the settle window that holds a thread's opening at its end. */
async function settle() {
  await act(async () => { vi.advanceTimersByTime(2000); });
}

describe('the transcript as the file panel opens and closes', () => {
  const OriginalRO = window.ResizeObserver;

  beforeEach(() => {
    rowH = 400;
    width = 1180;
    vi.useFakeTimers({ shouldAdvanceTime: true });
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number' && this === viewport()) scrollTo(top);
    } as HTMLElement['scrollTo'];
    window.ResizeObserver = class {
      constructor(cb: ResizeObserverCallback) {
        resize = () => cb([{ contentRect: { height: contentH(), width } } as ResizeObserverEntry], this as unknown as ResizeObserver);
      }
      observe() {}
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
  });
  afterEach(() => {
    window.ResizeObserver = OriginalRO;
    vi.useRealTimers();
  });

  it('keeps a reader at the end there through every frame of the change, both ways', async () => {
    render(<Harness />);
    act(() => { resize(); });
    await settle();
    expect(atEnd()).toBe(0);

    for (const [w, h] of [[1000, 400], [800, 450], [600, 520], [515, 560]]) {
      frame(w, h);
      expect(atEnd()).toBe(0);
    }
    for (const [w, h] of [[700, 480], [1000, 400], [1180, 400]]) {
      frame(w, h);
      expect(atEnd()).toBe(0);
    }
    expect(scroll.jumpPill.visible).toBe(false);
  });

  it('leaves growth with no change of width unfollowed on a settled transcript', async () => {
    render(<Harness />);
    act(() => { resize(); });
    await settle();
    const before = scrolled();

    frame(width, 460);

    expect(scrolled()).toBe(before);
    expect(atEnd()).toBe(ROWS * 60);
  });

  it('keeps a reader the follow is carrying at the end, though the glide had not reached it', async () => {
    render(<Harness isStreaming />);
    act(() => { resize(); });
    await settle();
    expect(atEnd()).toBe(0);

    // A line of the reply lands and the glide sets off, short of the end when
    // the panel's first frame rewraps the transcript.
    frame(width, 420);
    expect(atEnd()).toBeGreaterThan(0);
    frame(1000, 480);

    expect(atEnd()).toBe(0);
  });

  it('holds a reader mid-thread on the row at the top of the view', async () => {
    render(<Harness />);
    act(() => { resize(); });
    await settle();

    // The reader scrolls up into the third row, which the view's top cuts.
    act(() => {
      viewport().dispatchEvent(new Event('wheel'));
      scrollTo(1000);
    });
    const held = rowTop(2);
    expect(held).toBeLessThan(0);

    // The column narrows past the transcript's own width only after its first
    // frames, and the rows above the reader then rewrap taller each frame.
    for (const [w, h] of [[1000, 400], [800, 450], [600, 520], [515, 560]]) {
      frame(w, h);
      expect(rowTop(2)).toBe(held);
    }
    // Closing: the rows above shrink back.
    for (const [w, h] of [[700, 480], [1180, 400]]) {
      frame(w, h);
      expect(rowTop(2)).toBe(held);
    }
  });
});
