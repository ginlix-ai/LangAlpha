import type { BrowserInput, Modifiers, MouseButton } from '../filePanel/browserProtocol';
import type { FrameGeometry } from '../filePanel/browserStream';

type ModifierKeys = { altKey: boolean; ctrlKey: boolean; metaKey: boolean; shiftKey: boolean };

export function modifiersOf(e: ModifierKeys): Modifiers {
  return { alt: e.altKey, ctrl: e.ctrlKey, meta: e.metaKey, shift: e.shiftKey };
}

export function mouseButton(button: number): MouseButton {
  if (button === 1) return 'middle';
  if (button === 2) return 'right';
  return 'left';
}

type KeyEvent = ModifierKeys & { key: string; code: string; keyCode: number; getModifierState?: (key: string) => boolean };

export function keyInput(event: 'down' | 'up', e: KeyEvent): BrowserInput {
  const modifiers = modifiersOf(e);
  // AltGr types a character, and Windows and Linux report it as Ctrl and Alt
  // held: the page gets the character, not a shortcut that types nothing.
  if (e.getModifierState?.('AltGraph')) Object.assign(modifiers, { ctrl: false, alt: false });
  return { kind: 'key', event, key: e.key, code: e.code, keyCode: e.keyCode, modifiers };
}

/**
 * A point on the canvas as a point on the page, or null in the letterbox. The
 * canvas fills the panel and contains the frame, and the frame is a scaled copy,
 * so both steps come from the frame itself: under one viewport size the stream
 * can send frames of another, and scaling from the viewport lands clicks off.
 */
export function pagePoint(
  clientX: number, clientY: number,
  box: { left: number; top: number; width: number; height: number },
  frame: FrameGeometry,
): { x: number; y: number } | null {
  const scale = Math.min(box.width / frame.width, box.height / frame.height);
  const shownWidth = frame.width * scale;
  const shownHeight = frame.height * scale;
  const fx = (clientX - box.left - (box.width - shownWidth) / 2) / shownWidth;
  // Centred across, at the top down: `object-position: top` in BrowserViewer.css.
  const fy = (clientY - box.top) / shownHeight;
  if (fx < 0 || fx > 1 || fy < 0 || fy > 1) return null;
  const perFramePixel = frame.pageWidth / frame.width;
  return { x: Math.round(fx * frame.width * perFramePixel), y: Math.round(fy * frame.height * perFramePixel) };
}
