import { renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ESCAPE_GRACE_MS, useEscapeClosesFile } from '../useEscapeClosesFile';

let now = 1000;
// What the comment layer does with an Escape it takes: marks it, on the document.
let layerTakes = false;
const layer = (e: KeyboardEvent) => {
  if (layerTakes && e.key === 'Escape') e.preventDefault();
};

function press(init: KeyboardEventInit = {}) {
  document.body.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true, ...init }));
}

beforeEach(() => {
  now = 1000;
  layerTakes = false;
  vi.spyOn(performance, 'now').mockImplementation(() => now);
  document.addEventListener('keydown', layer);
});

afterEach(() => {
  document.removeEventListener('keydown', layer);
  vi.restoreAllMocks();
});

describe('Escape on an open Drive file', () => {
  it('closes the file when nothing else took it', () => {
    const close = vi.fn();
    renderHook(() => useEscapeClosesFile(true, close));
    press();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('leaves the file open for a moment after the comment layer took an Escape, then closes it', () => {
    const close = vi.fn();
    renderHook(() => useEscapeClosesFile(true, close));
    layerTakes = true;
    press();
    expect(close).not.toHaveBeenCalled();

    // The layer has nothing left to close; the next press comes quickly.
    layerTakes = false;
    now += ESCAPE_GRACE_MS - 50;
    press();
    expect(close).not.toHaveBeenCalled();

    // A later one, counted from the last press the layer took, closes the file.
    now += 100;
    press();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('counts an Escape its taker stopped on the way down (an edit box, a draft card) as taken', () => {
    const close = vi.fn();
    renderHook(() => useEscapeClosesFile(true, close));
    // The edit box: on the window, in the capture phase, and it lets nothing else hear the key.
    let boxOpen = true;
    const box = (e: KeyboardEvent) => {
      if (boxOpen && e.key === 'Escape') e.stopPropagation();
    };
    window.addEventListener('keydown', box, { capture: true });
    try {
      press();
      expect(close).not.toHaveBeenCalled();

      boxOpen = false;
      now += ESCAPE_GRACE_MS - 50;
      press();
      expect(close).not.toHaveBeenCalled();

      now += 100;
      press();
      expect(close).toHaveBeenCalledTimes(1);
    } finally {
      window.removeEventListener('keydown', box, { capture: true });
    }
  });

  it('never closes the file on a held key', () => {
    const close = vi.fn();
    renderHook(() => useEscapeClosesFile(true, close));
    press({ repeat: true });
    expect(close).not.toHaveBeenCalled();
  });

  it('does nothing while off (no file, or the editor open)', () => {
    const close = vi.fn();
    renderHook(() => useEscapeClosesFile(false, close));
    press();
    expect(close).not.toHaveBeenCalled();
  });
});
