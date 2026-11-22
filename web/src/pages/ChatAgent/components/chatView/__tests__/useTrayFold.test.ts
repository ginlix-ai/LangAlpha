/**
 * The tray folds into its dock while a row holds its place above the
 * composer (another person's turn), as it does while the reader is up, and
 * comes back out once the row goes. A peek pours it out over the row.
 */
import { describe, it, expect, afterEach, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { LIQUID_FALLBACK_MS, SHAPE_RUN } from '../trayLiquid';
import { useTrayFold } from '../useTrayFold';
import type { TrayContents } from '../useTrayContents';

type Props = { covered?: boolean; scrolledUp?: boolean; docked?: boolean };

function renderFold(initial: Props) {
  return renderHook(
    ({ covered = false, scrolledUp = false, docked = false }: Props) =>
      // No contents: the fold runs instantly, with no liquid to time.
      useTrayFold({ scrolledUp, jumping: false, contents: null, getScroller: () => null, docked, covered }),
    { initialProps: initial },
  );
}

describe('useTrayFold: a row above the composer', () => {
  it('folds the tray while a row holds its place, and opens it when the row goes', () => {
    const fold = renderFold({ covered: true });
    expect(fold.result.current.phase).toBe('folded');
    fold.rerender({ covered: false });
    expect(fold.result.current.phase).toBe('open');
  });

  it('keeps the tray folded when the row goes while the reader is up or the composer is docked', () => {
    const up = renderFold({ covered: true, scrolledUp: true });
    up.rerender({ covered: false, scrolledUp: true });
    expect(up.result.current.phase).toBe('folded');

    const docked = renderFold({ covered: true, docked: true });
    docked.rerender({ covered: false, docked: true });
    expect(docked.result.current.phase).toBe('folded');
  });

  it('pours the tray out over the row on a peek, until the row goes', () => {
    const fold = renderFold({ covered: true });
    act(() => fold.result.current.peek());
    expect(fold.result.current.phase).toBe('open');
    fold.rerender({ covered: true });
    expect(fold.result.current.phase).toBe('open');
    fold.rerender({ covered: false });
    expect(fold.result.current.phase).toBe('open');
    fold.rerender({ covered: true });
    expect(fold.result.current.phase).toBe('folded');
  });
});

/** A scroller whose geometry the test moves, firing a scroll as the page would. */
function fakeScroller(scrollHeight: number, scrollTop: number) {
  const el = document.createElement('div');
  const box = { scrollHeight, scrollTop, clientHeight: 400 };
  Object.defineProperty(el, 'scrollHeight', { get: () => box.scrollHeight });
  Object.defineProperty(el, 'clientHeight', { get: () => box.clientHeight });
  Object.defineProperty(el, 'scrollTop', { get: () => box.scrollTop });
  const move = (next: Partial<typeof box>) => {
    Object.assign(box, next);
    el.dispatchEvent(new Event('scroll'));
  };
  return { el, move };
}

function stubOffsetTop(el: HTMLElement, top: number) {
  Object.defineProperty(el, 'offsetTop', { configurable: true, get: () => top });
}

describe('useTrayFold: measuring the tray', () => {
  let stack: HTMLElement | null = null;
  afterEach(() => stack?.remove());

  it('measures the tray to the next row drawn, past an empty notice row', () => {
    // The tray (36px with its gap) over a hidden notice row and the composer.
    stack = document.createElement('div');
    const tray = document.createElement('div');
    const notices = document.createElement('div');
    notices.hidden = true;
    const composer = document.createElement('div');
    stack.append(tray, notices, composer);
    document.body.append(stack);
    stubOffsetTop(tray, 0);
    stubOffsetTop(notices, 0);
    stubOffsetTop(composer, 36);
    const { el } = fakeScroller(1000, 580);

    const fold = renderHook(
      ({ scrolledUp }: { scrolledUp: boolean }) =>
        useTrayFold({ scrolledUp, jumping: false, contents: null, getScroller: () => el }),
      { initialProps: { scrolledUp: false } },
    );
    fold.result.current.trayRef.current = tray;
    // 20px from the end, inside the tray's 36: folding would clamp the reader
    // back to the end, so the tray stays out.
    fold.rerender({ scrolledUp: true });
    expect(fold.result.current.phase).toBe('open');
  });

  it('keeps a peek out while a live turn carries the reader at the end', () => {
    const { el, move } = fakeScroller(1000, 600);
    const fold = renderHook(
      ({ covered }: { covered: boolean }) =>
        useTrayFold({ scrolledUp: false, jumping: false, contents: null, getScroller: () => el, covered }),
      { initialProps: { covered: true } },
    );
    act(() => fold.result.current.peek());
    expect(fold.result.current.phase).toBe('open');

    // The turn grows the transcript a block at a time and the follow glides
    // after it: the reader drifts 50px from the end for a moment and is
    // carried 120px down in all, with no hand on the page.
    act(() => move({ scrollHeight: 1050, scrollTop: 602 }));
    act(() => move({ scrollTop: 650 }));
    act(() => move({ scrollHeight: 1120, scrollTop: 652 }));
    act(() => move({ scrollTop: 720 }));
    expect(fold.result.current.phase).toBe('open');

    // The reader scrolls back up: the peek ends and the tray folds again.
    act(() => move({ scrollTop: 660 }));
    expect(fold.result.current.phase).toBe('folded');
  });

  it('keeps a peek made up in the history out as the reader reads on down, until they scroll back up', () => {
    const { el, move } = fakeScroller(1000, 300);
    const fold = renderHook(() =>
      useTrayFold({ scrolledUp: true, jumping: false, contents: null, getScroller: () => el }));
    act(() => fold.result.current.peek());
    expect(fold.result.current.phase).toBe('open');
    act(() => move({ scrollTop: 400 }));
    expect(fold.result.current.phase).toBe('open');
    act(() => move({ scrollTop: 340 }));
    expect(fold.result.current.phase).toBe('folded');
  });

  it('keeps a peek made just above the end out as the reader scrolls back to the end and is followed', () => {
    const { el, move } = fakeScroller(1000, 590);
    const getScroller = () => el;
    const fold = renderHook(
      ({ scrolledUp }: { scrolledUp: boolean }) =>
        useTrayFold({ scrolledUp, jumping: false, contents: null, getScroller, covered: true }),
      { initialProps: { scrolledUp: true } },
    );
    act(() => fold.result.current.peek());
    // The tray poured out and the turn grow the transcript under the reader,
    // who then scrolls back down to the end in steps and is followed on.
    act(() => move({ scrollHeight: 1100 }));
    for (const scrollTop of [610, 630, 650, 670, 690, 700]) act(() => move({ scrollTop }));
    fold.rerender({ scrolledUp: false });
    act(() => move({ scrollHeight: 1160, scrollTop: 760 }));
    expect(fold.result.current.phase).toBe('open');
  });

  it('keeps a peek out at the end when the transcript shrinks under the reader', () => {
    const { el, move } = fakeScroller(1000, 600);
    const fold = renderHook(() =>
      useTrayFold({ scrolledUp: false, jumping: false, contents: null, getScroller: () => el, covered: true }));
    act(() => fold.result.current.peek());
    act(() => move({ scrollHeight: 940, scrollTop: 540 }));
    expect(fold.result.current.phase).toBe('open');
    act(() => move({ scrollTop: 480 }));
    expect(fold.result.current.phase).toBe('folded');
  });
});

const CONTENTS: TrayContents = {
  plan: null, watch: { symbols: ['NVDA'] }, wakeups: null, commands: null, tasks: null, analysts: null, reportBack: null,
};

/** A tray with chips, so a fold runs as liquid; a notice above the composer folds it. */
function renderRun() {
  return renderHook(
    ({ covered }: { covered: boolean }) =>
      useTrayFold({ scrolledUp: false, jumping: false, contents: CONTENTS, getScroller: () => null, covered }),
    { initialProps: { covered: false } },
  );
}

describe('useTrayFold: a liquid run', () => {
  afterEach(() => vi.useRealTimers());

  it('ends when the liquid lands, and only the run that is going', () => {
    const fold = renderRun();
    fold.rerender({ covered: true });
    expect(fold.result.current.phase).toBe('folding');
    // A late word from another run moves nothing.
    act(() => fold.result.current.settle('unfolding'));
    expect(fold.result.current.phase).toBe('folding');
    act(() => fold.result.current.settle('folding'));
    expect(fold.result.current.phase).toBe('folded');

    fold.rerender({ covered: false });
    expect(fold.result.current.phase).toBe('unfolding');
    act(() => fold.result.current.settle('unfolding'));
    expect(fold.result.current.phase).toBe('open');
  });

  it('waits for the liquid past any count of chips, then moves on without it', () => {
    vi.useFakeTimers();
    const fold = renderRun();
    fold.rerender({ covered: true });
    // Where a timer counting one chip would have ended it, mid-fall.
    act(() => vi.advanceTimersByTime(SHAPE_RUN * 1000));
    expect(fold.result.current.phase).toBe('folding');
    // A run that never drew (nothing to measure) still ends.
    act(() => vi.advanceTimersByTime(LIQUID_FALLBACK_MS - SHAPE_RUN * 1000));
    expect(fold.result.current.phase).toBe('folded');
  });
});
