/**
 * iOS never resizes the layout viewport for the on-screen keyboard, so the shell
 * is fitted to the visual viewport here. These pin the readings Safari actually
 * gives: `innerHeight` falls to the visible height while the page is panned, and
 * an overlay's field needs the pan a shrunken shell would take away.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { trackVisualViewport } from '../visualViewport';

const LAYOUT_H = 714;
const KEYBOARD_UP_H = 385;

const viewport = Object.assign(new EventTarget(), { height: LAYOUT_H, scale: 1 });
const root = document.documentElement;
const scrollTo = vi.fn();
const scrollIntoView = vi.fn();

function move(next: Partial<typeof viewport>) {
  Object.assign(viewport, next);
  viewport.dispatchEvent(new Event('resize'));
}

function field(parent: HTMLElement = document.body) {
  const el = document.createElement('textarea');
  parent.appendChild(el);
  return el;
}

function shell() {
  const el = document.createElement('div');
  el.className = 'app-layout';
  document.body.appendChild(el);
  return el;
}

beforeAll(() => {
  Object.defineProperty(window, 'visualViewport', { configurable: true, value: viewport });
  Object.defineProperty(root, 'clientHeight', { configurable: true, value: LAYOUT_H });
  window.scrollTo = scrollTo;
  Element.prototype.scrollIntoView = scrollIntoView;
  trackVisualViewport();
});

afterEach(() => {
  move({ height: LAYOUT_H, scale: 1 });
  document.body.replaceChildren();
  vi.clearAllMocks();
});

describe('trackVisualViewport', () => {
  it('fits the shell and undoes the pan for a field in it', () => {
    field(shell()).focus();
    move({ height: KEYBOARD_UP_H });

    expect(root.dataset.keyboardOpen).toBe('');
    expect(root.style.getPropertyValue('--keyboard-viewport-height')).toBe(`${KEYBOARD_UP_H}px`);
    expect(root.style.getPropertyValue('--keyboard-overlap')).toBe(`${LAYOUT_H - KEYBOARD_UP_H}px`);
    expect(scrollTo).toHaveBeenCalledWith(0, 0);
  });

  it('stays fitted once innerHeight falls to the visible height', () => {
    const innerHeight = vi.spyOn(window, 'innerHeight', 'get').mockReturnValue(KEYBOARD_UP_H);
    field(shell()).focus();
    move({ height: KEYBOARD_UP_H });

    expect(root.dataset.keyboardOpen).toBe('');
    innerHeight.mockRestore();
  });

  it('reveals a field once, so a growing textarea does not fight the reader', () => {
    field(shell()).focus();
    move({ height: KEYBOARD_UP_H });
    move({ height: KEYBOARD_UP_H - 10 });

    expect(scrollIntoView).toHaveBeenCalledTimes(1);
  });

  it('lets go when the keyboard closes', () => {
    field(shell()).focus();
    move({ height: KEYBOARD_UP_H });
    move({ height: LAYOUT_H });

    expect(root.dataset.keyboardOpen).toBeUndefined();
    expect(root.style.getPropertyValue('--keyboard-overlap')).toBe('');
  });

  it.each([
    ['a portaled dialog', () => document.body],
    ['a dialog in the shell', () => {
      const dialog = document.createElement('div');
      dialog.setAttribute('role', 'dialog');
      shell().appendChild(dialog);
      return dialog;
    }],
    ['a sheet in the shell', () => {
      const sheet = document.createElement('div');
      sheet.dataset.sheet = '';
      shell().appendChild(sheet);
      return sheet;
    }],
  ])('leaves the pan to Safari for a field in %s', (_, parent) => {
    field(parent()).focus();
    move({ height: KEYBOARD_UP_H });

    expect(root.dataset.keyboardOpen).toBeUndefined();
    expect(scrollTo).not.toHaveBeenCalled();
  });

  it('lets go when focus crosses into a dialog with the keyboard up', () => {
    field(shell()).focus();
    move({ height: KEYBOARD_UP_H });
    expect(root.dataset.keyboardOpen).toBe('');

    field().focus();
    expect(root.dataset.keyboardOpen).toBeUndefined();
  });

  it('stays fitted when focus moves into a composer picker, a dialog that opens from the composer', () => {
    const composer = field(shell());
    composer.focus();
    move({ height: KEYBOARD_UP_H });
    const picker = document.createElement('div');
    picker.setAttribute('role', 'dialog');
    picker.dataset.composerPanel = '';
    composer.parentElement!.appendChild(picker);
    field(picker).focus();
    window.dispatchEvent(new Event('focusin'));
    expect(root.dataset.keyboardOpen).toBe('');
  });

  it('lets go when a pinch takes over with the keyboard up', () => {
    field(shell()).focus();
    move({ height: KEYBOARD_UP_H });
    expect(root.dataset.keyboardOpen).toBe('');
    move({ scale: 2 });
    expect(root.dataset.keyboardOpen).toBeUndefined();
  });

  it('does not take a pinch for a keyboard', () => {
    field(shell()).focus();
    move({ height: KEYBOARD_UP_H, scale: 2 });

    expect(root.dataset.keyboardOpen).toBeUndefined();
    expect(scrollTo).not.toHaveBeenCalled();
  });
});
