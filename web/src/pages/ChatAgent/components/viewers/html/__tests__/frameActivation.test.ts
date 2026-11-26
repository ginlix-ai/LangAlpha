import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  FRAME_PROMPT_GAP_MS,
  resetFrameActivation,
  retryAfterRefusal,
  userActedInFrame,
  watchFrameActivation,
  WIDGET_FRAME_ATTR,
} from '../frameActivation';

// jsdom never moves focus into a frame on its own, so each case puts it there
// with `frame.focus()` and replays what the parent document would have seen.
describe('userActedInFrame', () => {
  let frame: HTMLIFrameElement;
  let composer: HTMLTextAreaElement;

  const activation = (isActive: boolean | undefined) =>
    vi.stubGlobal('navigator', isActive === undefined ? {} : { userActivation: { isActive } });
  const pointerOnto = (el: Element) => el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
  const parentClick = (el: Element) => el.dispatchEvent(new Event('pointerdown', { bubbles: true }));
  const parentKey = (key: string) => window.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
  // Focus landing in the frame, as the parent sees it: the window blurs.
  const focusEnters = () => {
    frame.focus();
    window.dispatchEvent(new Event('blur'));
  };

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['performance', 'setTimeout', 'Date'] });
    // Well past any activation window the suite's own setup could open.
    vi.advanceTimersByTime(60_000);
    frame = document.createElement('iframe');
    frame.setAttribute(WIDGET_FRAME_ATTR, '');
    composer = document.createElement('textarea');
    document.body.append(composer, frame);
    watchFrameActivation();
    activation(true);
  });

  afterEach(() => {
    resetFrameActivation();
    frame.remove();
    composer.remove();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('is yes for a click inside the frame', () => {
    pointerOnto(frame);
    focusEnters();
    expect(userActedInFrame(frame)).toBe(true);
  });

  // A widget that calls `input.focus()` on a timer holds page focus with no
  // click, so the activation is whatever the user did elsewhere.
  it('is no with focus in the frame but the pointer elsewhere', () => {
    pointerOnto(composer);
    focusEnters();
    expect(userActedInFrame(frame)).toBe(false);
  });

  it('is no while a gesture of the page itself could be what made it active', () => {
    pointerOnto(frame);
    focusEnters();
    parentClick(composer);
    expect(userActedInFrame(frame)).toBe(false);
    vi.advanceTimersByTime(4900);
    expect(userActedInFrame(frame)).toBe(false);
    vi.advanceTimersByTime(200);
    expect(userActedInFrame(frame)).toBe(true);
  });

  // The refusal the user meets after a click elsewhere: the host says when
  // a click will count, and says nothing for a call the user did not make.
  it('says when a click refused inside the window will count', () => {
    parentClick(composer);
    const clickedAt = performance.now();
    vi.advanceTimersByTime(1000);
    pointerOnto(frame);
    focusEnters();
    expect(userActedInFrame(frame)).toBe(false);
    expect(retryAfterRefusal(frame)).toBe(clickedAt + 5000);
    vi.advanceTimersByTime(4000);
    expect(retryAfterRefusal(frame)).toBeNull();
    expect(userActedInFrame(frame)).toBe(true);
  });

  it('says nothing for a call with the pointer elsewhere, or one spent by the gap', () => {
    parentClick(composer);
    pointerOnto(composer);
    focusEnters();
    expect(retryAfterRefusal(frame)).toBeNull();
    vi.advanceTimersByTime(5000);
    pointerOnto(frame);
    frame.focus();
    expect(userActedInFrame(frame)).toBe(true);
    expect(userActedInFrame(frame)).toBe(false);
    expect(retryAfterRefusal(frame)).toBeNull();
  });

  it('is no for a key pressed in the page', () => {
    pointerOnto(frame);
    focusEnters();
    parentKey('a');
    expect(userActedInFrame(frame)).toBe(false);
  });

  // Tabbing in is itself the page's key press: a widget answering its own
  // focus event at once is refused, and a key inside it counts once that
  // activation has lapsed.
  it('takes focus brought in by Tab, once the Tab itself has lapsed', () => {
    pointerOnto(composer);
    // The key's default action moves focus before the next task.
    parentKey('Tab');
    focusEnters();
    vi.advanceTimersByTime(0);
    expect(document.activeElement).toBe(frame);
    expect(userActedInFrame(frame)).toBe(false);
    vi.advanceTimersByTime(5000);
    expect(userActedInFrame(frame)).toBe(true);
  });

  // A widget answering any Tab by taking focus a task later is not the key's landing.
  it('does not count focus taken after a Tab as brought in by it', () => {
    composer.focus();
    pointerOnto(composer);
    parentKey('Tab');
    vi.advanceTimersByTime(0);
    focusEnters();
    vi.advanceTimersByTime(0);
    expect(document.activeElement).toBe(composer);
    vi.advanceTimersByTime(6000);
    frame.focus();
    expect(userActedInFrame(frame)).toBe(false);
  });

  it('answers one call per frame in each gap', () => {
    pointerOnto(frame);
    focusEnters();
    expect(userActedInFrame(frame)).toBe(true);
    expect(userActedInFrame(frame)).toBe(false);
    vi.advanceTimersByTime(FRAME_PROMPT_GAP_MS - 1);
    expect(userActedInFrame(frame)).toBe(false);
    vi.advanceTimersByTime(1);
    expect(userActedInFrame(frame)).toBe(true);
  });

  it('is no without the page active, or where the browser cannot say', () => {
    pointerOnto(frame);
    focusEnters();
    activation(false);
    expect(userActedInFrame(frame)).toBe(false);
    activation(undefined);
    expect(userActedInFrame(frame)).toBe(false);
  });

  it('is no with focus outside the frame', () => {
    pointerOnto(frame);
    composer.focus();
    expect(userActedInFrame(frame)).toBe(false);
  });

  it('is no before anything is watched', () => {
    resetFrameActivation();
    frame.focus();
    expect(userActedInFrame(frame)).toBe(false);
  });

  describe('focus a widget takes', () => {
    it('goes back to where it was', () => {
      composer.focus();
      pointerOnto(composer);
      focusEnters();
      vi.advanceTimersByTime(0);
      expect(document.activeElement).toBe(composer);
    });

    // Keys that landed in the frame before focus went back were gestures in it.
    it('silences the frame for an activation window', () => {
      pointerOnto(composer);
      focusEnters();
      vi.advanceTimersByTime(0);
      pointerOnto(frame);
      frame.focus();
      expect(userActedInFrame(frame)).toBe(false);
      vi.advanceTimersByTime(5000);
      expect(userActedInFrame(frame)).toBe(true);
    });

    it('stays where the pointer put it', () => {
      composer.focus();
      pointerOnto(frame);
      focusEnters();
      vi.advanceTimersByTime(0);
      expect(document.activeElement).toBe(frame);
    });

    it('leaves a frame that is not a widget alone', () => {
      frame.removeAttribute(WIDGET_FRAME_ATTR);
      composer.focus();
      pointerOnto(composer);
      focusEnters();
      vi.advanceTimersByTime(0);
      expect(document.activeElement).toBe(frame);
    });
  });
});
