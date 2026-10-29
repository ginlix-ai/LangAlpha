import type { KeyboardEvent as ReactKeyboardEvent } from 'react';

/**
 * True when a key belongs to an IME composition, such as the Enter that
 * commits Latin letters typed in a Chinese input method. Such a key edits the
 * draft and must never submit it. Safari ends the composition before the
 * committing keydown, so `isComposing` is already false there and only the
 * 229 keyCode IMEs report gives it away.
 */
export function isImeComposing(e: KeyboardEvent | ReactKeyboardEvent): boolean {
  const native = 'nativeEvent' in e ? e.nativeEvent : e;
  return native.isComposing || native.keyCode === 229;
}
