/**
 * A move layout makes is not the reader's. When the transcript shrinks under a
 * reader at the end, the browser clamps the view onto the new end, and the
 * scroll event for that clamp arrives a frame later. Growth landing in between
 * leaves the end further away by then, which the scroll event alone reads as
 * the reader scrolling up, and the follow stopped mid-reply.
 *
 * The follow glides over animation frames, which the fake timers run, so a
 * check of where it lands lets the glide finish first.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatScroll } from '../useChatScroll';

const VIEW_H = 500;
let contentH = 2000;

const measureViewport = (el: HTMLElement | null) => {
  if (!el) return;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { get: () => contentH, configurable: true });
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
        <div data-message-id="a0" />
      </div>
    </div>
  );
}

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;
const end = () => contentH - VIEW_H;

/** Moves the view the way a browser does, clamped to the range, with no scroll
 *  event yet: that comes a frame later (`scrollEvent`). */
function moveTo(top: number) {
  const clamped = Math.max(0, Math.min(top, end()));
  Object.defineProperty(viewport(), 'scrollTop', { value: clamped, writable: true, configurable: true });
}
const scrollEvent = () => act(() => { viewport().dispatchEvent(new Event('scroll')); });

/** Long enough for any glide to land. */
async function glide() {
  await act(async () => { vi.advanceTimersByTime(1000); });
}

describe('a move layout makes under a streaming turn', () => {
  const OriginalRO = window.ResizeObserver;

  beforeEach(() => {
    contentH = 2000;
    vi.useFakeTimers({ shouldAdvanceTime: true });
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number' && this === viewport()) {
        moveTo(top);
        this.dispatchEvent(new Event('scroll'));
      }
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

  /** A reader riding the end of the reply, once the thread's opening has let
   *  go of the view. */
  async function ridingTheEnd() {
    render(<Harness />);
    act(() => { moveTo(end()); });
    scrollEvent();
    act(() => { resize(contentH); });
    await act(async () => { vi.advanceTimersByTime(2000); });
  }

  /** Then a row closes under them: the view clamps onto the new end, and the
   *  observer sees that layout. */
  async function clampAtEnd() {
    await ridingTheEnd();
    contentH -= 200;
    act(() => { moveTo(end()); });
    act(() => { resize(contentH); });
  }

  it('keeps following when growth lands before the clamp is seen', async () => {
    await clampAtEnd();
    contentH += 300;
    scrollEvent();
    act(() => { resize(contentH); });
    await glide();

    expect(viewport().scrollTop).toBe(end());
    expect(scroll.jumpPill.visible).toBe(false);
  });

  it('still lets a reader who scrolls up in that frame leave', async () => {
    await clampAtEnd();
    act(() => {
      viewport().dispatchEvent(new Event('wheel'));
      moveTo(viewport().scrollTop - 200);
    });
    const left = viewport().scrollTop;
    contentH += 300;
    scrollEvent();
    act(() => { resize(contentH); });
    await glide();

    expect(viewport().scrollTop).toBe(left);
    expect(scroll.jumpPill.visible).toBe(true);
  });

  // Where a scroll is applied on the main thread, the observer can see it
  // before its scroll event: it must not take it for layout's.
  it("leaves a reader's move up that the observer sees first to its scroll event", async () => {
    await ridingTheEnd();
    act(() => {
      viewport().dispatchEvent(new Event('wheel'));
      moveTo(end() - 100);
    });
    const left = viewport().scrollTop;
    contentH += 50;
    act(() => { resize(contentH); });
    scrollEvent();
    await glide();

    expect(viewport().scrollTop).toBe(left);
    expect(scroll.jumpPill.visible).toBe(true);
  });
});
