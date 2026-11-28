/**
 * The mode hint steps out while the open box covers it, measured again as
 * the box grows, and takes no Tab while it is out.
 */
import { act, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createRef } from 'react';
import { renderWithProviders } from '@/test/utils';
import { ModeHintBar } from '../ModeHintBar';

const rect = (top: number, bottom: number) => ({
  left: 0, right: 200, top, bottom, width: 200, height: bottom - top, x: 0, y: top, toJSON: () => ({}),
}) as DOMRect;
const frames = () => act(() => new Promise<void>((done) => requestAnimationFrame(() => requestAnimationFrame(() => done()))));

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('the mode hint under an open box', () => {
  it('steps out once the box grows over it, and leaves the tab order', async () => {
    const reports: (() => void)[] = [];
    vi.stubGlobal('ResizeObserver', class {
      constructor(private report: () => void) {}
      observe() { reports.push(this.report); }
      disconnect() { reports.splice(reports.indexOf(this.report), 1); }
    });
    const boxEl = document.createElement('div');
    document.body.append(boxEl);
    const ref = createRef<HTMLElement | null>() as React.MutableRefObject<HTMLElement | null>;
    ref.current = boxEl;
    let boxBottom = 100;
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
      return this === boxEl ? rect(0, boxBottom) : rect(300, 340);
    });

    renderWithProviders(
      <ModeHintBar
        shown
        takesParts={false}
        adding={false}
        box={{ ref, at: 'pick:10:10' }}
        size={{ width: 800, height: 400 }}
        onExit={vi.fn()}
      />,
    );
    await frames();
    const hint = document.querySelector('.page-comments-hint');
    expect(hint).not.toHaveAttribute('data-covered');
    expect(screen.getByRole('button', { name: /exit/i }).closest('[inert]')).toBeNull();

    // Typing grows the box down over the hint; neither the spot nor the pane moved.
    boxBottom = 320;
    act(() => reports.forEach((report) => report()));
    await frames();
    expect(hint).toHaveAttribute('data-covered');
    expect(hint).toHaveAttribute('inert');
    boxEl.remove();
  });

  it('measures again each time it is shown, since the hint is a new element then', async () => {
    const boxEl = document.createElement('div');
    document.body.append(boxEl);
    const ref = createRef<HTMLElement | null>() as React.MutableRefObject<HTMLElement | null>;
    ref.current = boxEl;
    // The box sits over where the hint goes from the start.
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
      return this === boxEl ? rect(0, 320) : rect(300, 340);
    });
    const bar = (shown: boolean) => (
      <ModeHintBar
        shown={shown}
        takesParts={false}
        adding={false}
        box={{ ref, at: 'pick:10:10' }}
        size={{ width: 800, height: 400 }}
        onExit={vi.fn()}
      />
    );

    // Hidden (the comment list is open): there is no hint to measure.
    const { rerender } = renderWithProviders(bar(false));
    await frames();
    expect(document.querySelector('.page-comments-hint')).toBeNull();

    rerender(bar(true));
    await frames();
    expect(document.querySelector('.page-comments-hint')).toHaveAttribute('data-covered');
    boxEl.remove();
  });

  it('measures again when a scroll bar moves the hint, with nothing resized', async () => {
    const boxEl = document.createElement('div');
    document.body.append(boxEl);
    const ref = createRef<HTMLElement | null>() as React.MutableRefObject<HTMLElement | null>;
    ref.current = boxEl;
    // The hint sits clear of the box until a scroll bar at the foot lifts it.
    let hintTop = 300;
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
      return this === boxEl ? rect(0, 290) : rect(hintTop, hintTop + 40);
    });
    const bar = (bottom: number) => (
      <ModeHintBar
        shown
        takesParts={false}
        adding={false}
        insets={{ bottom, right: 0 }}
        box={{ ref, at: 'pick:10:10' }}
        size={{ width: 800, height: 400 }}
        onExit={vi.fn()}
      />
    );

    const { rerender } = renderWithProviders(bar(0));
    await frames();
    expect(document.querySelector('.page-comments-hint')).not.toHaveAttribute('data-covered');

    hintTop = 284;
    rerender(bar(16));
    await frames();
    expect(document.querySelector('.page-comments-hint')).toHaveAttribute('data-covered');
    boxEl.remove();
  });
});
