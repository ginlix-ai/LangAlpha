/**
 * The end of a reply lands after isStreaming has gone false: the commit that
 * ends the turn adds its actions, then the typewriter types out what it still
 * held. A reader riding the end must be carried through that, and a settled
 * transcript must not be followed for good.
 *
 * The follow glides over animation frames, which the fake timers run, so each
 * check lets a glide finish first.
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

function Harness({ isStreaming }: { isStreaming: boolean }) {
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
  return (
    <div ref={api.scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div data-message-id="a0" />
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

function grow(by: number) {
  contentH += by;
  act(() => { resize(contentH); });
}

/** Long enough for any glide to land. */
async function glide() {
  await act(async () => { vi.advanceTimersByTime(1000); });
}

describe('a reader following a reply as its turn ends', () => {
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

  async function followToTurnEnd() {
    const { rerender } = render(<Harness isStreaming />);
    // The reader's own scroll ends the entry pin, so only the follow is left
    // to carry them.
    act(() => {
      viewport().dispatchEvent(new Event('wheel'));
      scrollTo(contentH - VIEW_H);
    });
    act(() => { resize(contentH); });
    grow(40);
    await glide();
    expect(viewport().scrollTop).toBe(contentH - VIEW_H);
    rerender(<Harness isStreaming={false} />);
  }

  it('is carried to the end of what lands after the stream stops', async () => {
    await followToTurnEnd();
    grow(32);
    await act(async () => { vi.advanceTimersByTime(300); });
    grow(49);
    await glide();
    expect(viewport().scrollTop).toBe(contentH - VIEW_H);
  });

  it('is left alone once the finished transcript has been quiet', async () => {
    await followToTurnEnd();
    grow(32);
    await act(async () => { vi.advanceTimersByTime(1600); });
    const settled = viewport().scrollTop;
    expect(settled).toBe(contentH - VIEW_H);
    grow(200);
    await glide();
    expect(viewport().scrollTop).toBe(settled);
  });

  it('glides to the end over several frames rather than jumping there', async () => {
    await followToTurnEnd();
    const from = viewport().scrollTop;
    grow(120);
    await act(async () => { vi.advanceTimersByTime(17); });
    const first = viewport().scrollTop;
    expect(first).toBeGreaterThan(from);
    expect(first).toBeLessThan(contentH - VIEW_H);
    await glide();
    expect(viewport().scrollTop).toBe(contentH - VIEW_H);
  });
});
