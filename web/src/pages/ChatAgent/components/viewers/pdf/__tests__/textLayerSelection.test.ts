/**
 * A press selecting a PDF's words holds the selection to them: its text layer
 * is marked `selecting`, and the root for as long as the press lasts. In
 * comment mode the press is stopped at the column, so react-pdf's own mark,
 * made from React's `onMouseDown`, never came: Chrome then ran the selection
 * to the end of the page at each gap between lines, or selected nothing.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { holdTextLayers } from '../textLayerSelection';

const root = document.documentElement;

function column(): HTMLElement {
  const el = document.createElement('div');
  el.innerHTML = '<div class="pdf-page"><div class="textLayer"><span>first page</span></div></div>'
    + '<div class="pdf-page"><div class="textLayer"><span>second page</span></div></div>'
    + '<div class="pdf-gap"></div>';
  document.body.append(el);
  return el;
}

function press(target: Element, button = 0, pointerType = 'mouse'): void {
  target.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true, cancelable: true, button, pointerType }));
  target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, button }));
}

const selecting = (el: HTMLElement) => [...el.querySelectorAll('.textLayer')].map((l) => l.classList.contains('selecting'));

afterEach(() => {
  document.body.replaceChildren();
  delete root.dataset.pdfSelecting;
});

describe('holdTextLayers', () => {
  it('marks the pressed page\'s words while the comment mode stops the press', () => {
    const el = column();
    // The comment mode's guard, as `ViewerPage` attaches it: first, capturing, stopping the press.
    for (const type of ['pointerdown', 'mousedown']) el.addEventListener(type, (e) => e.stopPropagation(), { capture: true });
    const stop = holdTextLayers(el);

    press(el.querySelector('span')!);
    expect(selecting(el)).toEqual([true, false]);
    expect(root.dataset.pdfSelecting).toBe('');

    // The press blurs whatever had the focus; that is not the window losing it.
    const button = document.createElement('button');
    document.body.append(button);
    button.focus();
    button.blur();
    expect(selecting(el)).toEqual([true, false]);

    window.dispatchEvent(new Event('pointerup'));
    expect(selecting(el)).toEqual([false, false]);
    expect(root.dataset.pdfSelecting).toBeUndefined();
    stop();
  });

  it('lets go when the window loses the focus mid-press, and when it is torn down', () => {
    const el = column();
    const stop = holdTextLayers(el);
    press(el.querySelector('span')!);
    window.dispatchEvent(new Event('blur'));
    expect(selecting(el)).toEqual([false, false]);

    press(el.querySelector('span')!);
    stop();
    expect(selecting(el)).toEqual([false, false]);
    expect(root.dataset.pdfSelecting).toBeUndefined();
  });

  it('marks nothing for a press off the words or with another button', () => {
    const el = column();
    const stop = holdTextLayers(el);
    press(el.querySelector('.pdf-gap')!);
    press(el.querySelector('span')!, 2);
    expect(selecting(el)).toEqual([false, false]);
    expect(root.dataset.pdfSelecting).toBeUndefined();
    stop();
  });

  it('marks nothing for a tap, whose mousedown comes after its pointerup', () => {
    const el = column();
    const stop = holdTextLayers(el);
    const span = el.querySelector('span')!;
    // A touch screen's order: the pointer events, then the mouse ones made up for them.
    span.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true, button: 0, pointerType: 'touch' }));
    window.dispatchEvent(new PointerEvent('pointerup', { pointerType: 'touch' }));
    span.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, button: 0 }));
    span.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, button: 0 }));
    span.dispatchEvent(new MouseEvent('click', { bubbles: true, button: 0 }));
    expect(selecting(el)).toEqual([false, false]);
    expect(root.dataset.pdfSelecting).toBeUndefined();
    stop();
  });

  it('marks a pen\'s press as a mouse\'s', () => {
    const el = column();
    const stop = holdTextLayers(el);
    press(el.querySelector('span')!, 0, 'pen');
    expect(selecting(el)).toEqual([true, false]);
    stop();
  });
});
