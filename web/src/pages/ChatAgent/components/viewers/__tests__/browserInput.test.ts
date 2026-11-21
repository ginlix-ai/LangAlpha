// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { encodeInput } from '../../filePanel/browserProtocol';
import type { FrameGeometry } from '../../filePanel/browserStream';
import { keyInput, pagePoint } from '../browserInput';

describe('pagePoint', () => {
  // A 960x600 frame of a page 1280 wide and 800 tall, whose metadata says 943.
  const frame: FrameGeometry = { width: 960, height: 600, pageWidth: 1280 };

  it('scales by the frame shape, not the device height the stream reports', () => {
    const box = { left: 0, top: 0, width: 960, height: 600 };
    expect(pagePoint(480, 600, box, frame)).toEqual({ x: 640, y: 800 });
  });

  it('finds a frame held at the top of a tall panel', () => {
    const box = { left: 100, top: 50, width: 480, height: 800 };
    expect(pagePoint(100, 50, box, frame)).toEqual({ x: 0, y: 0 });
    expect(pagePoint(580, 50 + 300, box, frame)).toEqual({ x: 1280, y: 800 });
    expect(pagePoint(340, 50 + 301, box, frame)).toBeNull();
  });

  it('finds a frame centred across a wide panel', () => {
    const box = { left: 0, top: 0, width: 1200, height: 300 };
    expect(pagePoint(360, 0, box, frame)).toEqual({ x: 0, y: 0 });
    expect(pagePoint(359, 0, box, frame)).toBeNull();
  });
});

describe('keyInput', () => {
  it('passes the key on as the DOM names it, with the modifiers held', () => {
    const e = { key: 'a', code: 'KeyA', keyCode: 65, altKey: false, ctrlKey: true, metaKey: false, shiftKey: true };
    expect(keyInput('down', e)).toEqual({
      kind: 'key', event: 'down', key: 'a', code: 'KeyA', keyCode: 65,
      modifiers: { alt: false, ctrl: true, meta: false, shift: true },
    });
  });

  it('types what AltGr makes, which the platform reports with Ctrl and Alt held', () => {
    const altGr = (state: boolean) => ({
      key: '@', code: 'KeyQ', keyCode: 81, altKey: true, ctrlKey: true, metaKey: false, shiftKey: false,
      getModifierState: (key: string) => key === 'AltGraph' && state,
    });
    const typed = keyInput('down', altGr(true));
    expect(typed).toMatchObject({ modifiers: { alt: false, ctrl: false } });
    expect(JSON.parse(encodeInput(typed)[0])).toMatchObject({ text: '@', modifiers: 0 });
    // Ctrl and Alt held for a shortcut are still a shortcut.
    expect(JSON.parse(encodeInput(keyInput('down', altGr(false)))[0])).not.toHaveProperty('text');
  });
});
