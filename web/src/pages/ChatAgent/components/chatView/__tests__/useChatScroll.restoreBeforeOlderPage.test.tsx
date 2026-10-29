/**
 * The row at the top of a paged transcript asks for an older page when the
 * view is near it. Until the reader's place is restored, a thread just entered
 * or shown again sits at the top, so the row must not observe before the
 * restore has landed: it would load a page the reader never scrolled to, and
 * the hold that keeps a prepended page out of sight would then keep them there.
 */
import React, { useLayoutEffect, useMemo, useRef } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatScroll } from '../useChatScroll';
import { OlderHistoryRow } from '../OlderHistoryRow';
import { scrollMemory } from '@/lib/scrollMemory';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const VIEW_H = 500;
const CONTENT_H = 8000;
// The row asks for a page when the view is this close to the top of the transcript.
const PREFETCH_PX = 600;
const BUBBLES = Array.from({ length: 20 }, (_, i) => `history-${i % 2 ? 'assistant' : 'user'}-${51 + (i >> 1)}`);

interface Observer {
  root: HTMLElement;
  callback: IntersectionObserverCallback;
  targets: Element[];
  disconnected: boolean;
  /** Where the view was when the observer started. */
  startedAt: number;
}
let observers: Observer[] = [];

/** One rendering update: every live observer reports whether the row is in range. */
function frame() {
  act(() => {
    for (const o of observers) {
      if (o.disconnected || o.targets.length === 0) continue;
      const isIntersecting = o.root.scrollTop <= PREFETCH_PX;
      o.callback(o.targets.map((target) => ({ isIntersecting, target }) as IntersectionObserverEntry), {} as IntersectionObserver);
    }
  });
}

const measureViewport = (el: HTMLElement | null) => {
  if (!el || el.dataset.measured) return;
  el.dataset.measured = '1';
  let top = 0;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { value: CONTENT_H, configurable: true });
  Object.defineProperty(el, 'scrollTop', {
    get: () => top,
    set: (v: number) => { top = Math.max(0, Math.min(v, CONTENT_H - VIEW_H)); },
    configurable: true,
  });
};

const onLoad = vi.fn();

function Harness({ isActive, isLoadingHistory, activeAgentId = 'main' }: { isActive: boolean; isLoadingHistory: boolean; activeAgentId?: string }) {
  const messages = useMemo(() => (isLoadingHistory ? [] : BUBBLES.map((id) => ({ id }))), [isLoadingHistory]);
  const isActiveRef = useRef(isActive);
  useLayoutEffect(() => {
    isActiveRef.current = isActive;
  });
  const scroll = useChatScroll({
    activeAgentId,
    messages,
    isActive,
    isActiveRef,
    isLoadingHistory,
    historyLoadFailed: false,
    isStreaming: false,
    currentThreadId: 't1',
    threadId: 't1',
  });
  const { scrollAreaRef, getScrollContainer, entryRestored } = scroll;
  const getRoot = React.useCallback(() => getScrollContainer(scrollAreaRef), [getScrollContainer, scrollAreaRef]);
  // A subagent tab replaces the main transcript's viewport.
  if (activeAgentId !== 'main') return null;
  return (
    <div ref={scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div className="max-w-3xl">
          {!isLoadingHistory && (
            <OlderHistoryRow
              getRoot={getRoot}
              status="idle"
              canLoad={isActive && entryRestored}
              onLoad={onLoad}
            />
          )}
          {messages.map(({ id }) => <div key={id} data-message-id={id} />)}
        </div>
      </div>
    </div>
  );
}

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;

describe('the older-history row waits for the reader\'s place', () => {
  const OriginalIO = window.IntersectionObserver;
  const OriginalRO = window.ResizeObserver;
  beforeEach(() => {
    observers = [];
    onLoad.mockClear();
    scrollMemory.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
    window.ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
    window.IntersectionObserver = class {
      private o: Observer;
      constructor(callback: IntersectionObserverCallback, options?: IntersectionObserverInit) {
        const root = options!.root as HTMLElement;
        this.o = { root, callback, targets: [], disconnected: false, startedAt: root.scrollTop };
        observers.push(this.o);
      }
      observe(target: Element) { this.o.targets.push(target); }
      unobserve() {}
      disconnect() { this.o.disconnected = true; }
      takeRecords() { return []; }
    } as unknown as typeof IntersectionObserver;
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number') this.scrollTop = top;
    } as HTMLElement['scrollTo'];
  });
  afterEach(() => {
    vi.useRealTimers();
    window.IntersectionObserver = OriginalIO;
    window.ResizeObserver = OriginalRO;
    scrollMemory.clear();
  });

  async function settle() {
    await act(async () => { vi.advanceTimersByTime(50); });
  }

  it('enters a thread at the remembered place before the row observes', async () => {
    scrollMemory.set('thread:t1', 3000);
    const { rerender } = render(<Harness isActive isLoadingHistory />);
    rerender(<Harness isActive isLoadingHistory={false} />);
    await settle();
    frame();

    expect(viewport().scrollTop).toBe(3000);
    expect(observers.length).toBeGreaterThan(0);
    expect(observers.map((o) => o.startedAt)).toEqual(observers.map(() => 3000));
    expect(onLoad).not.toHaveBeenCalled();
  });

  it('restores a thread whose history landed while it was hidden before the row observes', async () => {
    scrollMemory.set('thread:t1', 3000);
    const { rerender } = render(<Harness isActive={false} isLoadingHistory />);
    rerender(<Harness isActive={false} isLoadingHistory={false} />);
    await settle();
    expect(observers).toEqual([]);

    rerender(<Harness isActive isLoadingHistory={false} />);
    await settle();
    frame();

    expect(observers.length).toBeGreaterThan(0);
    expect(observers.map((o) => o.startedAt)).toEqual(observers.map(() => 3000));
    expect(onLoad).not.toHaveBeenCalled();
  });

  it('asks for nothing when a cached view is shown again where it was left', async () => {
    scrollMemory.set('thread:t1', 3000);
    const { rerender } = render(<Harness isActive isLoadingHistory />);
    rerender(<Harness isActive isLoadingHistory={false} />);
    await settle();
    rerender(<Harness isActive={false} isLoadingHistory={false} />);
    expect(observers.every((o) => o.disconnected)).toBe(true);

    rerender(<Harness isActive isLoadingHistory={false} />);
    await settle();
    frame();

    expect(observers.filter((o) => !o.disconnected).map((o) => o.startedAt)).toEqual([3000]);
    expect(onLoad).not.toHaveBeenCalled();
  });

  it('restores a thread opened on a subagent tab when its transcript is first shown', async () => {
    scrollMemory.set('thread:t1', 3000);
    const { rerender } = render(<Harness isActive isLoadingHistory activeAgentId="task:t" />);
    rerender(<Harness isActive isLoadingHistory={false} activeAgentId="task:t" />);
    await settle();

    rerender(<Harness isActive isLoadingHistory={false} />);
    await settle();
    frame();

    expect(viewport().scrollTop).toBe(3000);
    expect(observers.length).toBeGreaterThan(0);
    expect(observers.map((o) => o.startedAt)).toEqual(observers.map(() => 3000));
    expect(onLoad).not.toHaveBeenCalled();
  });

  it('still asks for the page when the restored place is near the top', async () => {
    scrollMemory.set('thread:t1', 200);
    const { rerender } = render(<Harness isActive isLoadingHistory />);
    rerender(<Harness isActive isLoadingHistory={false} />);
    await settle();
    frame();

    expect(viewport().scrollTop).toBe(200);
    expect(onLoad).toHaveBeenCalledTimes(1);
  });
});
