/**
 * Which input device the user last reached for.
 *
 * Chromium propagates `:focus-visible` across a *programmatic* focus move: an
 * element focused by script inherits the state of the element focus came from.
 * Radix overlays hand focus back to their trigger on close, so a menu opened
 * and dismissed entirely with the mouse still lights that trigger's focus ring,
 * and leaves it lit until the next click. Overlays consult this to skip the
 * restore when no keyboard was involved.
 */

/** Held alone, a modifier is someone reaching for Cmd-Tab, not navigating. */
const MODIFIERS = new Set(['Meta', 'Control', 'Alt', 'Shift']);

/**
 * Set on the document element while the focus the page currently holds arrived
 * by pointer. `tokens.css` reads it to keep the ring off a text field the mouse
 * focused -- the one control `:focus-visible` matches for either device, so a
 * selector alone cannot tell a click from a Tab. One element holds focus at a
 * time, so a single record says everything a mark on each element would, and
 * leaves nothing behind on every field the mouse has ever touched.
 */
const POINTER_FOCUS = 'data-pointer-focus';

let pointer = false;

/**
 * What held focus when the browser window last lost it.
 *
 * Leaving the window does not release element focus, it parks it:
 * `document.activeElement` is unchanged throughout, and the browser fires a
 * `focusout`/`focusin` pair around the trip purely as bookkeeping. Nobody moved
 * focus, so nothing about how it was reached has changed -- but the restoring
 * `focusin` looks like every other one, and by then the flag below has been
 * flipped to keyboard by the user's own typing. Re-reading it there re-decides
 * a settled question with the wrong evidence and rings a field the mouse
 * focused, which is the bug: click the composer, type, switch to another app,
 * come back, ring. Remembering the parked element is what lets the restore be
 * recognised and passed over.
 */
let parked: EventTarget | null = null;

if (typeof window !== 'undefined') {
  // Capture phase: a handler that stops propagation must not be able to hide
  // the interaction from this.
  window.addEventListener('pointerdown', () => { pointer = true; parked = null; }, true);
  window.addEventListener(
    'keydown',
    (event) => {
      if (!MODIFIERS.has(event.key)) pointer = false;
      parked = null;
    },
    true,
  );
  // Not capture phase, and on `window` rather than `document`: element blur does
  // not bubble, so a listener reached only at its own target hears window
  // deactivation and nothing else -- which is the one case where focus is parked
  // rather than moved.
  window.addEventListener('blur', () => { parked = document.activeElement; });
  // Written when focus moves rather than read at paint: typing into a field is
  // a keydown, so a rule consulting the live flag would light a ring under the
  // user mid-sentence. Freezing it here answers what the ring actually asks,
  // which is how the control holding focus was reached.
  window.addEventListener(
    'focusin',
    (event) => {
      const restored = event.target === parked;
      parked = null;
      if (restored) return;
      document.documentElement.toggleAttribute(POINTER_FOCUS, pointer);
    },
    true,
  );
}

export function lastInputWasPointer(): boolean {
  return pointer;
}
