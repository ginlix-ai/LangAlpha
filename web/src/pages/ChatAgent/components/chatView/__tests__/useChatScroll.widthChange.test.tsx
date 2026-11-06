/**
 * The file panel opening beside the chat narrows the transcript, and every
 * line rewraps taller each frame it narrows; closing it does the reverse. That
 * is a relayout, not news: a reader at the end stays at the end, and one
 * mid-thread stays on the row at the top of the view. A web font swapping in
 * rewraps the same way at the same width, and can land long after the thread
 * has settled. Other growth with no change of width is still news, followed
 * only while a turn runs.
 *
 * jsdom has no layout, so the transcript is a model: one bubble of five rows
 * whose height `rowH` stands in for the column's width, and a tail below it.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { runAuthResets } from '@/lib/authResets';
import { useChatScroll } from '../useChatScroll';

const VIEW_H = 500;
const ROWS = 5;
const TAIL = 300;
let rowH = 400;
let width = 1180;
// The turn pin's floor: a min-height outside the box the observer watches, so
// it moves the scroll height with no observer callback.
let floor = 0;
const contentH = () => ROWS * rowH + TAIL;

const rect = (top: number, height: number) =>
  ({ top, bottom: top + height, height, left: 0, right: 0, width: 0, x: 0, y: 0, toJSON: () => {} }) as DOMRect;

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;
const scrolled = () => viewport().scrollTop;

const measureViewport = (el: HTMLElement | null) => {
  if (!el) return;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { get: () => contentH() + floor, configurable: true });
  el.getBoundingClientRect = () => rect(0, VIEW_H);
};
const measurePadded = (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => rect(-scrolled(), contentH());
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
        <div ref={measurePadded}>
          <div className="max-w-3xl">
            <div data-message-id="a0" ref={measureBubble}>
              {Array.from({ length: ROWS }, (_, i) => <p key={i} data-row={i} ref={measureRow(i)} />)}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/** Clamped the way a browser clamps, so a request past the end lands on it. */
function scrollTo(top: number) {
  const clamped = Math.max(0, Math.min(top, contentH() + floor - VIEW_H));
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

// The document's web fonts, by load status: jsdom has no FontFaceSet.
let faces: { status: string; unicodeRange: string }[] = [];

/** Past the settle window that holds a thread's opening at its end. */
async function settle() {
  await act(async () => { vi.advanceTimersByTime(2000); });
}

describe('the transcript as the file panel opens and closes', () => {
  const OriginalRO = window.ResizeObserver;

  beforeEach(() => {
    rowH = 400;
    width = 1180;
    // Where a view was is kept per thread across mounts; each test opens t1 afresh.
    runAuthResets();
    floor = 0;
    faces = [{ status: 'loaded', unicodeRange: 'U+0-10FFFF' }, { status: 'loading', unicodeRange: 'U+0000-00FF, U+0131' }];
    Object.defineProperty(document, 'fonts', {
      value: { forEach: (cb: (face: (typeof faces)[number]) => void) => faces.forEach(cb) },
      configurable: true,
    });
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
    delete (document as { fonts?: unknown }).fonts;
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

  it('keeps a reader at the end there as a font swaps in after the thread has settled', async () => {
    render(<Harness />);
    act(() => { resize(); });
    await settle();
    expect(atEnd()).toBe(0);

    // The text drew in a fallback, and the font arrives over the network. A
    // layout forced ahead of the frame has the browser's scroll anchoring
    // nudge the view, and its scroll event lands before the observer runs.
    faces[1].status = 'loaded';
    rowH = 410;
    act(() => { scrollTo(scrolled() + 1); });
    frame(width, 410);

    expect(atEnd()).toBe(0);
    expect(scroll.jumpPill.visible).toBe(false);
  });

  it('leaves a reader where they scrolled up to as a font swaps in under them', async () => {
    render(<Harness />);
    act(() => { resize(); });
    await settle();
    expect(atEnd()).toBe(0);

    // The font arrives, and before the observer runs the reader wheels up into
    // the third row: their scroll lands at a size not yet judged.
    faces[1].status = 'loaded';
    rowH = 410;
    act(() => {
      viewport().dispatchEvent(new WheelEvent('wheel', { deltaY: -100 }));
      scrollTo(1000);
    });
    frame(width, 410);

    expect(scrolled()).toBe(1000);
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

  it('leaves growth unfollowed when only a subset for other scripts loads with it', async () => {
    render(<Harness />);
    act(() => { resize(); });
    await settle();
    const before = scrolled();

    // A CJK slice arrives the first time a reply uses one of its characters.
    // It rewraps only the text that needed it, which is ordinary growth.
    faces.push({ status: 'loaded', unicodeRange: 'U+4E00-4E3F, U+4E41' });
    frame(width, 460);

    expect(scrolled()).toBe(before);
  });

  it("judges a reader's scroll after the turn pin's floor moved the scroll height", async () => {
    render(<Harness />);
    act(() => { resize(); });
    await settle();
    expect(atEnd()).toBe(0);

    // The floor moves the end with no observer callback. The reader then
    // scrolls, a hair and then up into the third row, which the view's top cuts.
    floor = 200;
    act(() => {
      viewport().dispatchEvent(new Event('wheel'));
      scrollTo(scrolled() + 3);
    });
    act(() => {
      viewport().dispatchEvent(new Event('wheel'));
      scrollTo(1000);
    });
    const held = rowTop(2);
    expect(held).toBeLessThan(0);

    // The panel opens: the reader is held on their row, not taken to the end.
    for (const [w, h] of [[1000, 400], [800, 450]]) {
      frame(w, h);
      expect(rowTop(2)).toBe(held);
    }
  });
});
