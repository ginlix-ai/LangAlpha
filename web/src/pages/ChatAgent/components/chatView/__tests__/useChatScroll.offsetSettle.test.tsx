/**
 * A remembered offset is restored off the end, so the jump pill shows and the
 * follow lets go until the reader scrolls. A transcript shorter than the view
 * never scrolls, so once the held place settles at its end the pill goes and
 * the follow takes the reader again; a long one keeps both as they were.
 */
import { useLayoutEffect, useMemo, useRef } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatScroll } from '../useChatScroll';
import { scrollMemory } from '@/lib/scrollMemory';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const VIEW_H = 500;
// Past the settle window's quiet period, short of its hard cap.
const SETTLED_MS = 2000;
const BUBBLES = ['history-user-1', 'history-assistant-1'];

let contentHeight = 0;
let scroll: ReturnType<typeof useChatScroll>;

const measureViewport = (el: HTMLElement | null) => {
  if (!el || el.dataset.measured) return;
  el.dataset.measured = '1';
  let top = 0;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { get: () => contentHeight, configurable: true });
  Object.defineProperty(el, 'scrollTop', {
    get: () => top,
    set: (v: number) => { top = Math.max(0, Math.min(v, contentHeight - VIEW_H)); },
    configurable: true,
  });
};

function Harness({ isLoadingHistory }: { isLoadingHistory: boolean }) {
  const messages = useMemo(() => (isLoadingHistory ? [] : BUBBLES.map((id) => ({ id }))), [isLoadingHistory]);
  const isActiveRef = useRef(true);
  useLayoutEffect(() => {
    isActiveRef.current = true;
  });
  const current = useChatScroll({
    activeAgentId: 'main',
    messages,
    isActive: true,
    isActiveRef,
    isLoadingHistory,
    historyLoadFailed: false,
    isStreaming: false,
    currentThreadId: 't1',
    threadId: 't1',
  });
  useLayoutEffect(() => {
    scroll = current;
  });
  const { scrollAreaRef } = current;
  return (
    <div ref={scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div className="max-w-3xl">
          {messages.map(({ id }) => <div key={id} data-message-id={id} />)}
        </div>
      </div>
    </div>
  );
}

describe('an offset restore once it settles', () => {
  const OriginalRO = window.ResizeObserver;
  beforeEach(() => {
    scrollMemory.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
    window.ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    } as unknown as typeof ResizeObserver;
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number') this.scrollTop = top;
    } as HTMLElement['scrollTo'];
  });
  afterEach(() => {
    vi.useRealTimers();
    window.ResizeObserver = OriginalRO;
    scrollMemory.clear();
  });

  async function enter() {
    scrollMemory.set('thread:t1', 3000);
    const { rerender } = render(<Harness isLoadingHistory />);
    rerender(<Harness isLoadingHistory={false} />);
    await act(async () => { vi.advanceTimersByTime(50); });
  }

  it('drops the pill and follows again on a transcript shorter than the view', async () => {
    contentHeight = 300;
    await enter();
    expect(scroll.jumpPill.visible).toBe(true);
    expect(scroll.isNearBottomRef.current).toBe(false);

    await act(async () => { vi.advanceTimersByTime(SETTLED_MS); });
    expect(scroll.jumpPill.visible).toBe(false);
    expect(scroll.isNearBottomRef.current).toBe(true);
  });

  it('leaves a reader the top bar sent up a short scroll where they went', async () => {
    contentHeight = VIEW_H + 60;
    const { rerender } = render(<Harness isLoadingHistory />);
    rerender(<Harness isLoadingHistory={false} />);
    await act(async () => { vi.advanceTimersByTime(SETTLED_MS); });
    expect(scroll.isNearBottomRef.current).toBe(true);

    await act(async () => { scroll.toTop(); });
    await act(async () => { vi.advanceTimersByTime(SETTLED_MS); });
    expect(scroll.isNearBottomRef.current).toBe(false);
  });

  it('keeps the pill on a long transcript held mid-thread', async () => {
    contentHeight = 8000;
    await enter();
    expect(scroll.jumpPill.visible).toBe(true);

    await act(async () => { vi.advanceTimersByTime(SETTLED_MS); });
    expect(scroll.jumpPill.visible).toBe(true);
    expect(scroll.isNearBottomRef.current).toBe(false);
  });
});
