/**
 * Opening a deliverables deck under a streaming turn is a small scroll to a
 * chosen place. The reader who had stepped out of the follow to open it should
 * still be there afterwards.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatScroll } from '../useChatScroll';

const VIEW_H = 500;
const CONTENT_H = 2000;

const rect = (top: number, height: number) =>
  ({ top, bottom: top + height, height, left: 0, right: 0, width: 0, x: 0, y: 0, toJSON: () => {} }) as DOMRect;

/** jsdom lays nothing out, so the geometry the controller reads is stated here. */
const measureViewport = (el: HTMLElement | null) => {
  if (!el) return;
  Object.defineProperty(el, 'clientHeight', { value: VIEW_H, configurable: true });
  Object.defineProperty(el, 'scrollHeight', { value: CONTENT_H, configurable: true });
  el.getBoundingClientRect = () => rect(0, VIEW_H);
};

// The reader has paused the follow 200px short of the end, outside the 120px
// band. The deck hangs 100px below the fold, so revealing it moves exactly that
// far and lands 100px from the end, inside the band. That landing is the whole
// point of the test: it is the shape that used to be read as rejoining.
const PAUSED_AT = CONTENT_H - VIEW_H - 200;
const measureDeck = (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => rect(VIEW_H - 12, 100);
};

let api: ReturnType<typeof useChatScroll>;

function Harness() {
  api = useChatScroll({
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
  // Ref callbacks run at commit, ahead of the controller's own effects, which
  // measure on mount.
  return (
    <div ref={api.scrollAreaRef}>
      <div data-radix-scroll-area-viewport ref={measureViewport}>
        <div data-message-id="a0"><div className="turn-files" ref={measureDeck}>cards</div></div>
      </div>
    </div>
  );
}

const viewport = () => document.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]')!;

function scrollTo(top: number) {
  const v = viewport();
  Object.defineProperty(v, 'scrollTop', { value: top, writable: true, configurable: true });
  v.dispatchEvent(new Event('scroll'));
}

/** The reader steps out of the follow, which is what makes the reveal matter. */
const leaveTheBottom = () => act(() => { scrollTo(PAUSED_AT); });

describe('revealing a deck under a streaming turn', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    // jsdom has no scrollTo on an element; the controller's is the only caller.
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number' && this === viewport()) scrollTo(top);
    } as HTMLElement['scrollTo'];
  });
  afterEach(() => { vi.useRealTimers(); });

  it('leaves the follow paused, however near the bottom the cards sit', async () => {
    render(<Harness />);
    leaveTheBottom();
    expect(api.isNearBottomRef.current).toBe(false);

    await act(async () => { api.revealFiles('a0'); await Promise.resolve(); });

    // The pin owns this position, so its own scroll is not the reader saying
    // they rejoined the stream. Counted as one, the follow came back when the
    // settle window let go and took the cards off screen.
    expect(api.isNearBottomRef.current).toBe(false);
    expect(api.pinTargetRef.current).toMatchObject({ mode: 'reveal', id: 'a0' });
  });

  it('still lets an ordinary scroll to the bottom rejoin the stream', () => {
    render(<Harness />);
    leaveTheBottom();

    // The control: the same landing, reached by the reader rather than by a pin.
    act(() => { scrollTo(CONTENT_H - VIEW_H); });

    expect(api.isNearBottomRef.current).toBe(true);
  });
});

// A hand-off's report link: the report row 1200px into the transcript, short
// of its end, and a thread kept mounted behind this one that holds a row of
// the same name.
const REPORT_AT = 1200;
const measureReport = (el: HTMLElement | null) => {
  if (el) el.getBoundingClientRect = () => rect(REPORT_AT - viewport().scrollTop, 26);
};

function LinkHarness() {
  api = useChatScroll({
    activeAgentId: 'main',
    messages: [{ id: 'a0' }, { id: 'r1' }],
    isActive: true,
    isActiveRef: { current: true },
    isLoadingHistory: false,
    historyLoadFailed: false,
    isStreaming: false,
    currentThreadId: 't1',
    threadId: 't1',
  });
  return (
    <>
      <div style={{ display: 'none' }}><div data-message-id="r1" tabIndex={-1} data-testid="behind" /></div>
      <div ref={api.scrollAreaRef}>
        <div data-radix-scroll-area-viewport ref={measureViewport}>
          <div data-message-id="a0">reply</div>
          <div data-message-id="r1" tabIndex={-1} ref={measureReport} data-testid="report">report</div>
        </div>
      </div>
    </>
  );
}

describe('revealing a message a link points to', () => {
  beforeEach(() => {
    HTMLElement.prototype.scrollTo = function (this: HTMLElement, opts?: unknown) {
      const top = (opts as { top?: number } | undefined)?.top;
      if (typeof top === 'number' && this === viewport()) scrollTo(top);
    } as HTMLElement['scrollTo'];
  });

  it('lands this transcript\'s own row under the view top and focuses it', async () => {
    const { getByTestId } = render(<LinkHarness />);
    let shown = false;
    await act(async () => { shown = api.revealMessage('r1'); await Promise.resolve(); });

    expect(shown).toBe(true);
    expect(getByTestId('report')).toHaveFocus();
    expect(api.pinTargetRef.current).toMatchObject({ mode: 'anchor', id: 'r1' });
    // The jump is the follow's own glide, a frame at a time, so it is read
    // once it has landed.
    await vi.waitFor(() => expect(viewport().scrollTop).toBeLessThan(REPORT_AT));
    expect(viewport().scrollTop).toBeGreaterThan(REPORT_AT - 100);
    // Above the end, so the follow stays off and the row stays put.
    expect(api.isNearBottomRef.current).toBe(false);
  });

  it('says it could not when the row is not drawn, so the link opens the thread instead', () => {
    render(<LinkHarness />);
    expect(api.revealMessage('gone')).toBe(false);
  });
});
