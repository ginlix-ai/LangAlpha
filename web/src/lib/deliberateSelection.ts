/**
 * Makes `data-selectable="deliberate"` mean "selectable only on purpose".
 *
 * The element is non-selectable until the pointer arrives on it with no button
 * held, which is the difference between reaching for a value and dragging past
 * it. Arming has to happen before mousedown: the browser picks a selection
 * anchor as the button goes down, and a style applied at `:active` is already
 * too late (measured, and the reason a CSS-only version of this does nothing).
 *
 * Shared with the account console, because a control has to behave the same in
 * whichever of the two the desktop shell is showing. This copy is canonical and
 * the console vendors it; a drift check belongs on that side, since this repo is
 * public and cannot reach the other one.
 *
 * NOT wired in langalpha today. Nothing here is marked
 * `data-selectable="deliberate"` yet, so calling this would install a
 * document-wide capture listener that runs `closest()` on every pointer move
 * for a selector that never matches. Call it from `main.tsx`, before React, the
 * moment something is marked: the listener has to be armed for the first
 * pointerover the document ever sees, including one on chrome that renders
 * before the app mounts.
 */

const HOST = '[data-selectable="deliberate"]';

let armed: Element | null = null;

function disarm() {
  armed?.removeAttribute('data-armed');
  armed = null;
}

function onPointerOver(event: PointerEvent) {
  const target = event.target as Element | null;
  const host = target?.closest?.(HOST) ?? null;

  if (armed && armed !== host) disarm();
  if (!host) return;

  // A pointer that arrives with a button down is a drag passing through, which
  // is the accident this whole mechanism exists to skip.
  if (event.buttons === 0) {
    host.setAttribute('data-armed', '');
    armed = host;
  } else if (armed === host) {
    disarm();
  }
}

export function initDeliberateSelection(): () => void {
  document.addEventListener('pointerover', onPointerOver, true);
  return () => {
    document.removeEventListener('pointerover', onPointerOver, true);
    disarm();
  };
}
