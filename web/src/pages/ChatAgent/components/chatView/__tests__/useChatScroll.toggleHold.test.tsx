/**
 * Opening a row under a streaming turn is a reader choosing a place. When the
 * hold that keeps the row under the pointer lets go, the follow must not carry
 * them to the bottom just because nothing scrolled while it held.
 *
 * The follow glides over animation frames, which the fake timers run, so a
 * check of where it lands lets the glide finish first.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatScroll } from '../useChatScroll';
import { announceAnchoredToggle } from '../../../utils/anchoredToggle';

const VIEW_H = 500;
let contentH = 2000;

const rect = (top: number, height: number) =>
  ({ top, bottom: top + height, height, left: 0, right: 0, width: 0, x: 0, y: 0, toJSON: () => {} }) as DOMRect;

const measureViewport = (el: HTMLElement | null) => {
  if (!el) return;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { get: () => contentH, configurable: true });
  el.getBoundingClientRect = () => rect(0, VIEW_H);
};

// The row sits near the bottom of the view and its unfold grows below it, so
// its own top never moves and the hold has nothing to correct.
const measureRow = (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => rect(VIEW_H - 80, 24);
};

let resize: (height: number) => void = () => {};
let scroll: ReturnType<typeof useChatScroll>;

function Harness() {
  const api = useChatScroll({
    activeAgentId: 'main',
    messages: [{ id: 'a0' }],
    isActive: true,
    isActiveRef: { current: true },
    isLoadingHistory: false,
    historyLoadFailed: false,
    isStreaming: true,
    currentThreadId: 't1',
    threadId: 't1',
  });
  React.useEffect(() => { scroll = api; });
  return (
    <div ref={api.scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div data-message-id="a0"><button data-row ref={measureRow}>row</button></div>
      </div>
    </div>
  );
}

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;

/** Clamped the way a browser clamps, so a request past the end lands on it. */
function scrollTo(top: number) {
  const v = viewport();
  const clamped = Math.max(0, Math.min(top, contentH - VIEW_H));
  Object.defineProperty(v, 'scrollTop', { value: clamped, writable: true, configurable: true });
  v.dispatchEvent(new Event('scroll'));
}

/** Long enough for any glide to land. */
async function glide() {
  await act(async () => { vi.advanceTimersByTime(1000); });
}

describe('a disclosure opened near the bottom of a streaming turn', () => {
  const OriginalRO = window.ResizeObserver;

  beforeEach(() => {
    contentH = 2000;
    vi.useFakeTimers({ shouldAdvanceTime: true });
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number' && this === viewport()) scrollTo(top);
    } as HTMLElement['scrollTo'];
    window.ResizeObserver = class {
      constructor(cb: ResizeObserverCallback) {
        resize = (height) => cb([{ contentRect: { height } } as ResizeObserverEntry], this as unknown as ResizeObserver);
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

  it('stays where the reader opened it once the hold lets go', async () => {
    render(<Harness />);
    act(() => { scrollTo(contentH - VIEW_H); });
    act(() => { resize(contentH); });

    act(() => { announceAnchoredToggle(document.querySelector<HTMLElement>('[data-row]')); });
    contentH += 400;
    act(() => { resize(contentH); });
    const opened = viewport().scrollTop;

    await act(async () => { vi.advanceTimersByTime(1100); });
    contentH += 20;
    act(() => { resize(contentH); });

    expect(viewport().scrollTop).toBe(opened);
  });

  it('keeps following when the hold leaves the reader at the bottom', async () => {
    render(<Harness />);
    act(() => { scrollTo(contentH - VIEW_H); });
    act(() => { resize(contentH); });

    // A collapse: the transcript shrinks and the view clamps onto the end.
    act(() => { announceAnchoredToggle(document.querySelector<HTMLElement>('[data-row]')); });
    contentH -= 200;
    act(() => { scrollTo(contentH - VIEW_H); });
    act(() => { resize(contentH); });

    await act(async () => { vi.advanceTimersByTime(1100); });
    contentH += 20;
    act(() => { resize(contentH); });
    await glide();

    expect(viewport().scrollTop).toBe(contentH - VIEW_H);
  });

  // A steer, a message into the turn still running, goes to its end; a send
  // that opens a turn pins the reader's message instead (ChatView.sendRejoin).
  it('lets a steer right after the toggle take the reader to the end', async () => {
    render(<Harness />);
    act(() => { scrollTo(contentH - VIEW_H); });
    act(() => { resize(contentH); });

    // Scrolled up, the reader opens a row in view and sends straight away.
    act(() => {
      viewport().dispatchEvent(new Event('wheel'));
      scrollTo(800);
    });
    const row = document.querySelector<HTMLElement>('[data-row]')!;
    row.getBoundingClientRect = () => rect(1100 - viewport().scrollTop, 24);
    act(() => { announceAnchoredToggle(row); });
    act(() => { scroll.follow.rejoin('steer'); });
    contentH += 100;
    act(() => { resize(contentH); });
    await glide();
    expect(viewport().scrollTop).toBe(contentH - VIEW_H);

    // The reply's next token lands after both the hold and the settle window.
    await act(async () => { vi.advanceTimersByTime(2000); });
    contentH += 40;
    act(() => { resize(contentH); });
    await glide();
    expect(viewport().scrollTop).toBe(contentH - VIEW_H);
  });

  // From the very end a steer has nowhere to glide, so nothing but the steer
  // itself can end the toggle's hold.
  it('lets a steer from the very end, right after a toggle, carry the reader with the reply', async () => {
    render(<Harness />);
    act(() => { scrollTo(contentH - VIEW_H); });
    act(() => { resize(contentH); });

    // A row collapsed at the end: the transcript shrinks and the view clamps
    // onto its new end. The reader sends into the running turn from there.
    act(() => { announceAnchoredToggle(document.querySelector<HTMLElement>('[data-row]')); });
    contentH -= 200;
    act(() => { scrollTo(contentH - VIEW_H); });
    act(() => { resize(contentH); });
    act(() => { scroll.follow.rejoin('steer'); });

    // Their message and the reply's first lines, inside the hold's window.
    contentH += 100;
    act(() => { resize(contentH); });
    await glide();
    expect(viewport().scrollTop).toBe(contentH - VIEW_H);

    // And the reply's next lines, once the window has passed.
    await act(async () => { vi.advanceTimersByTime(1100); });
    contentH += 40;
    act(() => { resize(contentH); });
    await glide();
    expect(viewport().scrollTop).toBe(contentH - VIEW_H);
  });
});
