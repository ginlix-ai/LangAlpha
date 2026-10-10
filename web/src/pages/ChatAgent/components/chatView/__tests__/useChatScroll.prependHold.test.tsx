/**
 * An older page of a paged thread lands above the reader. The transcript grows
 * upward by the page's height, and what the reader is on must stay where it is
 * on screen, not be pushed down by that height.
 */
import React, { useLayoutEffect, useRef } from 'react';
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
let scroll: ReturnType<typeof useChatScroll>;

const measureViewport = (el: HTMLElement | null) => {
  if (!el) return;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { get: () => contentH, configurable: true });
};

const placeBubble = (id: string) => (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => ({ top: layout[id] - viewport().scrollTop }) as DOMRect;
};

function Harness({ bubbles, isStreaming = false, isActive = true, activeAgentId = 'main' }: {
  bubbles: string[];
  isStreaming?: boolean;
  isActive?: boolean;
  activeAgentId?: string;
}) {
  const messages = React.useMemo(() => bubbles.map((id) => ({ id })), [bubbles]);
  const isActiveRef = useRef(isActive);
  useLayoutEffect(() => {
    isActiveRef.current = isActive;
  });
  const api = useChatScroll({
    activeAgentId,
    messages,
    isActive,
    isActiveRef,
    isLoadingHistory: false,
    historyLoadFailed: false,
    isStreaming,
    currentThreadId: 't1',
    threadId: 't1',
  });
  const { scrollAreaRef } = api;
  React.useEffect(() => { scroll = api; });
  // A subagent tab replaces the main transcript's viewport.
  if (activeAgentId !== 'main') return null;
  return (
    <div ref={scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div className="max-w-3xl">
          {bubbles.map((id) => <div key={id} data-message-id={id} ref={placeBubble(id)} />)}
        </div>
      </div>
    </div>
  );
}

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;
const viewTop = (id: string) => layout[id] - viewport().scrollTop;

/** Clamped the way a browser clamps, so a request past the end lands on it. */
function scrollTo(top: number) {
  const v = viewport();
  const clamped = Math.max(0, Math.min(top, contentH - VIEW_H));
  Object.defineProperty(v, 'scrollTop', { value: clamped, writable: true, configurable: true });
  v.dispatchEvent(new Event('scroll'));
}

/** A reader's own scroll: the wheel takes control, then the view moves. */
function userScrollTo(top: number) {
  viewport().dispatchEvent(new Event('wheel'));
  scrollTo(top);
}

// Turns 75 and 76 are on screen; the older page brings turn 74, 800px tall.
const PAGE = ['history-user-75', 'history-assistant-75', 'history-user-76', 'history-assistant-76'];
const OLDER = ['history-user-74', 'history-assistant-74'];
const PAGE_LAYOUT = { 'history-user-75': 16, 'history-assistant-75': 100, 'history-user-76': 1000, 'history-assistant-76': 1100 };
const OLDER_H = 800;

/** The older page committed above the transcript, `extra` px of it still laying out. */
function prepend(rerender: (ui: React.ReactElement) => void, extra = 0, view: Partial<React.ComponentProps<typeof Harness>> = {}) {
  layout = {
    'history-user-74': 16,
    'history-assistant-74': 300,
    ...Object.fromEntries(Object.entries(PAGE_LAYOUT).map(([id, top]) => [id, top + OLDER_H + extra])),
  };
  contentH = 2000 + OLDER_H + extra;
  rerender(<Harness bubbles={[...OLDER, ...PAGE]} {...view} />);
}

describe('an older page landing above the reader', () => {
  const OriginalRO = window.ResizeObserver;
  beforeEach(() => {
    contentH = 2000;
    layout = { ...PAGE_LAYOUT };
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

  async function mountAt(top: number) {
    const view = render(<Harness bubbles={PAGE} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    act(() => { userScrollTo(top); });
    return view;
  }

  it('keeps the bubble the reader is in at the same place on screen', async () => {
    const { rerender } = await mountAt(1050);
    expect(viewTop('history-user-76')).toBe(-50);

    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender)); });

    expect(viewport().scrollTop).toBe(1050 + OLDER_H);
    expect(viewTop('history-user-76')).toBe(-50);
  });

  it('keeps holding while the page above finishes laying out', async () => {
    const { rerender } = await mountAt(1050);

    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender)); });
    // A chart in turn 74 renders taller once its data lays out.
    layout = Object.fromEntries(Object.entries(layout).map(([id, top]) => [id, id.endsWith('-74') ? top : top + 200]));
    act(() => { grow(contentH + 200); });

    expect(viewTop('history-user-76')).toBe(-50);
  });

  it('keeps the gap over the first bubble for a reader at the very top', async () => {
    const { rerender } = await mountAt(0);
    expect(viewTop('history-user-75')).toBe(16);

    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender)); });

    expect(viewport().scrollTop).toBe(OLDER_H);
    expect(viewTop('history-user-75')).toBe(16);
  });

  it('holds the place the reader is at as the page lands, and leaves nothing pending', async () => {
    const { rerender } = await mountAt(1050);
    // The reader scrolls on while the page is on its way.
    act(() => { userScrollTo(1000); });
    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender)); });

    expect(viewport().scrollTop).toBe(1000 + OLDER_H);
    expect(viewTop('history-user-76')).toBe(0);

    // The hold was read and applied with the page, so their next scroll is
    // theirs alone, and the page's late layout does not pull them back.
    act(() => { userScrollTo(1500); });
    layout = Object.fromEntries(Object.entries(layout).map(([id, top]) => [id, id.endsWith('-74') ? top : top + 200]));
    act(() => { grow(contentH + 200); });
    expect(viewport().scrollTop).toBe(1500);
  });

  it('puts the reader back on their bubble when a page landed while the thread was hidden', async () => {
    const { rerender } = await mountAt(1050);
    // The reader switches to another thread with the page on its way.
    rerender(<Harness bubbles={PAGE} isActive={false} />);

    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender, 0, { isActive: false })); });
    // A hidden view keeps its offset, which now sits in the page.
    expect(viewport().scrollTop).toBe(1050);

    rerender(<Harness bubbles={[...OLDER, ...PAGE]} />);
    expect(viewTop('history-user-76')).toBe(-50);
  });

  it('puts the reader back on their bubble when a page landed behind a subagent tab', async () => {
    const { rerender } = await mountAt(1050);
    // The tab switch saves the transcript's offset to restore on return.
    scroll.scrollPositionsRef.current.main = viewport().scrollTop;
    rerender(<Harness bubbles={PAGE} activeAgentId="task:t" />);

    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender, 0, { activeAgentId: 'task:t' })); });

    rerender(<Harness bubbles={[...OLDER, ...PAGE]} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    expect(viewTop('history-user-76')).toBe(-50);
  });

  it('leaves a reader held at the end on the end', async () => {
    // The thread just opened: the entry restore holds the bottom.
    const { rerender } = render(<Harness bubbles={PAGE} />);
    await act(async () => { vi.advanceTimersByTime(50); });
    expect(scroll.pinTargetRef.current).toEqual({ mode: 'bottom' });

    // The page lands and a streamed line grows the end in the same frame.
    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender)); });
    expect(scroll.pinTargetRef.current).toEqual({ mode: 'bottom' });
    act(() => { grow(contentH + 200); });

    expect(viewport().scrollTop).toBe(contentH - VIEW_H);
  });

  it('does not count the page as new messages on the jump pill', async () => {
    const { rerender } = await mountAt(1050);
    expect(scroll.jumpPill.visible).toBe(true);

    act(() => { scroll.holdPlace(OLDER.length, () => prepend(rerender)); });
    // The hold lets go once the page has settled.
    await act(async () => { vi.advanceTimersByTime(2000); });

    layout = { ...layout, 'user-live': 2700 };
    rerender(<Harness bubbles={[...OLDER, ...PAGE, 'user-live']} />);

    expect(scroll.jumpPill).toEqual({ visible: true, hasNew: true, newCount: 1 });
  });
});
