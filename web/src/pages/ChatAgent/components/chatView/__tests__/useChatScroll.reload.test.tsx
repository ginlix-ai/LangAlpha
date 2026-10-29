/**
 * A reload of the thread on screen (a catch-up after a report-back, a mux
 * resync) drops the history bubbles and replays them. The transcript shrinks,
 * the view clamps, and the reader must land back where they were, not at the
 * top.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatScroll } from '../useChatScroll';
import { scrollMemory } from '@/lib/scrollMemory';

const VIEW_H = 500;
let contentH = 2000;
/** Each bubble's top within the transcript. */
let layout: Record<string, number> = {};
/** Reports the transcript grown to a height, as the content's ResizeObserver would. */
let grow: (height: number) => void = () => {};

const measureViewport = (el: HTMLElement | null) => {
  if (!el) return;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { get: () => contentH, configurable: true });
};

const placeBubble = (id: string) => (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => ({ top: layout[id] - viewport().scrollTop }) as DOMRect;
};

function Harness({ isLoadingHistory, bubbles = [], failed = false }: { isLoadingHistory: boolean; bubbles?: string[]; failed?: boolean }) {
  const api = useChatScroll({
    activeAgentId: 'main',
    messages: [{ id: 'a0' }],
    isActive: true,
    isActiveRef: { current: true },
    isLoadingHistory,
    historyLoadFailed: failed,
    isStreaming: false,
    currentThreadId: 't1',
    threadId: 't1',
  });
  return (
    <div ref={api.scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div className="max-w-3xl">
          {bubbles.map((id) => <div key={id} data-message-id={id} ref={placeBubble(id)} />)}
        </div>
      </div>
    </div>
  );
}

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;

/** Clamped the way a browser clamps, so a request past the end lands on it,
 *  and announced only if the view moved, as a browser announces it. */
function scrollTo(top: number) {
  const v = viewport();
  const from = v.scrollTop;
  const clamped = Math.max(0, Math.min(top, contentH - VIEW_H));
  Object.defineProperty(v, 'scrollTop', { value: clamped, writable: true, configurable: true });
  if (clamped !== from) v.dispatchEvent(new Event('scroll'));
}

/** A reader's own scroll: the wheel takes control, then the view moves. */
function userScrollTo(top: number) {
  viewport().dispatchEvent(new Event('wheel'));
  scrollTo(top);
}

describe('a reload of the thread on screen', () => {
  const OriginalRO = window.ResizeObserver;
  beforeEach(() => {
    contentH = 2000;
    layout = {};
    scrollMemory.clear();
    window.ResizeObserver = class {
      constructor(cb: ResizeObserverCallback) {
        grow = (height) => {
          contentH = height;
          cb([{ contentRect: { height } } as ResizeObserverEntry], this as unknown as ResizeObserver);
        };
      }
      observe() {}
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
    vi.useFakeTimers({ shouldAdvanceTime: true });
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number' && this === viewport()) scrollTo(top);
    } as HTMLElement['scrollTo'];
  });
  afterEach(() => {
    vi.useRealTimers();
    window.ResizeObserver = OriginalRO;
  });

  async function reload(rerender: (ui: React.ReactElement) => void, grownH: number, bubbles: string[] = [], failed = false) {
    rerender(<Harness isLoadingHistory />);
    // The replay starts by dropping the bubbles it is about to rebuild.
    contentH = 300;
    act(() => { scrollTo(viewport().scrollTop); });
    contentH = grownH;
    rerender(<Harness isLoadingHistory={false} bubbles={bubbles} failed={failed} />);
    await act(async () => { vi.advanceTimersByTime(50); });
  }

  const TURNS = ['history-user-0', 'history-assistant-0', 'history-user-1', 'history-assistant-1'];
  const laidOut = { 'history-user-0': 0, 'history-assistant-0': 100, 'history-user-1': 1000, 'history-assistant-1': 1100 };

  it('keeps a reader mid-thread where they were', async () => {
    const { rerender } = render(<Harness isLoadingHistory={false} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(800); });

    await reload(rerender, 2100);

    expect(viewport().scrollTop).toBe(800);
  });

  it('keeps a reader at the bottom on the new bottom', async () => {
    const { rerender } = render(<Harness isLoadingHistory={false} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(contentH - VIEW_H); });

    await reload(rerender, 2100);

    expect(viewport().scrollTop).toBe(2100 - VIEW_H);
  });

  it.each([
    ['turn', 'history-user-1', 'history-assistant-1'],
    ['steering reply', 'history-steering-user-1-1-0', 'history-assistant-steering-1-1'],
  ])('follows the stream once a fork has cut the %s the reader was on', async (_what, userId, replyId) => {
    const bubbles = [...TURNS.slice(0, 2), userId, replyId];
    layout = { ...laidOut, [userId]: 1000, [replyId]: 1100 };
    const { rerender } = render(<Harness isLoadingHistory={false} bubbles={bubbles} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(1200); });

    // The replay ends at the fork, and the run replacing turn 1 streams its
    // backlog in one go.
    await reload(rerender, 900, bubbles.slice(0, 2));
    act(() => { grow(4500); });

    expect(viewport().scrollTop).toBe(4500 - VIEW_H);
  });

  it('keeps the offset of a reader on a live bubble, which the replay renames', async () => {
    const live = [...TURNS.slice(0, 2), 'user-1790000000000', 'assistant-1790000000000'];
    layout = { ...laidOut, 'user-1790000000000': 1000, 'assistant-1790000000000': 1100 };
    const { rerender } = render(<Harness isLoadingHistory={false} bubbles={live} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(1200); });

    await reload(rerender, 2100, TURNS);

    expect(viewport().scrollTop).toBe(1200);
  });

  it('keeps a reader on the turn they were reading as the replay lays out', async () => {
    layout = { ...laidOut };
    const { rerender } = render(<Harness isLoadingHistory={false} bubbles={TURNS} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(1200); });

    // The replay first measures short while media above the turn lays out, and
    // then renders the turn above taller than the live stream did.
    layout = { ...laidOut, 'history-user-1': 600, 'history-assistant-1': 700 };
    await reload(rerender, 1300, TURNS);
    layout = { ...laidOut, 'history-user-1': 1400, 'history-assistant-1': 1500 };
    act(() => { grow(2400); });

    expect(viewport().scrollTop).toBe(1600);
  });

  // The list holds what it drew through the load (MessageList), so the
  // reader can read on while it runs, and the reload lands where they went.
  it('keeps where the reader scrolled the held transcript during the reload', async () => {
    layout = { ...laidOut };
    const { rerender } = render(<Harness isLoadingHistory={false} bubbles={TURNS} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(1200); });

    rerender(<Harness isLoadingHistory bubbles={TURNS} />);
    act(() => { userScrollTo(150); });
    rerender(<Harness isLoadingHistory={false} bubbles={TURNS} />);
    await act(async () => { vi.advanceTimersByTime(50); });

    expect(viewport().scrollTop).toBe(150);
  });

  it('keeps a reader on their turn through a failed replay, for the retry that lands', async () => {
    layout = { ...laidOut };
    const { rerender } = render(<Harness isLoadingHistory={false} bubbles={TURNS} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(1200); });

    // The replay fails after stripping the bubbles it was to rebuild, and the
    // retry lands a while later with the turn above rendered taller.
    await reload(rerender, 300, [], true);
    await act(async () => { vi.advanceTimersByTime(5000); });
    layout = { ...laidOut, 'history-user-1': 1400, 'history-assistant-1': 1500 };
    await reload(rerender, 2400, TURNS);

    expect(viewport().scrollTop).toBe(1600);
  });
});
