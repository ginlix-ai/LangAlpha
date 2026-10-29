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

import { registerAuthReset } from '@/lib/authResets';

/** Held alone, a modifier is someone reaching for Cmd-Tab, not navigating. */
const MODIFIERS = new Set(['Meta', 'Control', 'Alt', 'Shift']);

/**
 * Input types that take a printable key as *content*. ``el.type`` reports "text"
 * for a missing or unrecognized attribute, so a bare `<input>` is covered
 * without being listed.
 *
 * The stepper types are here on purpose, though their arrow keys command the
 * control rather than write into it: someone who clicked a number or a date
 * field and then arrows it keeps no ring. That is the trade this whole module
 * is built around -- a ring nobody asked for is the one users notice, and the
 * field has its own affordance anyway.
 */
const TEXT_ENTRY_TYPES = new Set([
  'text', 'email', 'password', 'search', 'tel', 'url',
  'number', 'date', 'datetime-local', 'month', 'time', 'week',
]);

/** Would the next printable key land in this control as text the user is writing? */
function acceptsTextEntry(el: Element | null): boolean {
  if (!(el instanceof HTMLElement)) return false;
  if (el.isContentEditable) return true;
  if (el instanceof HTMLTextAreaElement) return true;
  return el instanceof HTMLInputElement && TEXT_ENTRY_TYPES.has(el.type);
}

/**
 * Set on the document element while the focus the page currently holds was not
 * placed by the keyboard: a click, a hover, script on its own. `tokens.css`
 * reads it to hold the focus ring off, since `:focus-visible` alone cannot
 * tell a click from a Tab on a text field or a `<select>`, nor a control
 * focused by script after a click from one focused by script after a key. One
 * element holds focus at a time, so a single record says everything a mark on
 * each element would, and leaves nothing behind on every field the mouse has
 * ever touched.
 */
const POINTER_FOCUS = 'data-pointer-focus';

/** A press since the last key: what the overlay autofocus guards ask. */
let pointer = false;

/**
 * Whether a key was the last thing the user touched. This is what the ring
 * asks, and it is false until the first key: focus the page places on its own
 * -- a composer autofocused on load, a field focused once its data arrives --
 * was asked for by nobody and draws no ring. A mouse move clears it as a press
 * does, since a menu focuses the item under the mouse as it passes (Radix does
 * it from pointermove) and nothing is pressed for that. Kept apart from
 * `pointer` because a move says nothing about what opened or dismissed an
 * overlay, and the guards read presses only.
 */
let keyboard = false;

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
  window.addEventListener(
    'pointerdown',
    () => {
      pointer = true;
      keyboard = false;
      parked = null;
      // Focus does not move when the press lands on the control that already
      // holds it, so no focusin follows to re-decide the record -- and a field
      // reached by Tab a moment ago would wear its ring through the click that
      // took it over. Stamped here rather than left to the focusin below,
      // which is a no-op when the record already says pointer.
      document.documentElement.toggleAttribute(POINTER_FOCUS, true);
    },
    true,
  );
  window.addEventListener('pointermove', () => { keyboard = false; }, true);
  window.addEventListener(
    'keydown',
    (event) => {
      // Any key, modifier included, is a real keystroke: a parked focus that
      // sees one was not restored by a window trip.
      parked = null;
      // A held modifier makes the keystroke a shortcut rather than a move.
      // Cmd-C sends a second keydown whose `key` is the letter with `metaKey`
      // still set, and reading that as navigation rings the <select> the user
      // clicked a moment ago, for as long as the focus stays there. Shift is
      // not one of these: Shift-Tab is how a keyboard user walks backwards.
      if (MODIFIERS.has(event.key) || event.metaKey || event.ctrlKey || event.altKey) return;
      pointer = false;
      keyboard = true;
      // Focus does not move when someone clicks a control and then drives it
      // from the keyboard, so no focusin fires and the record below would keep
      // saying pointer for the rest of that interaction. Refresh it here for a
      // control that cannot be typed into: someone arrowing a <select> to a new
      // option is navigating by keyboard and has to be ringed like one.
      //
      // The text-entry guard is the entire reason this is a refresh and not an
      // unstamp. Typing is a keydown, and both readers key off a text field --
      // tokens.css suppresses the baseline ring on one the mouse focused, and
      // LoginPage.css paints its ember on the record's *absence*. Clearing it
      // under a printable key lights both mid-sentence, which is the regression
      // the record was introduced to fix.
      if (!acceptsTextEntry(document.activeElement)) {
        document.documentElement.toggleAttribute(POINTER_FOCUS, false);
      }
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
  // user mid-sentence. Writing it here answers what the ring actually asks,
  // which is how the control holding focus was reached -- amended above only
  // where a later keystroke cannot be that user typing.
  window.addEventListener(
    'focusin',
    (event) => {
      const restored = event.target === parked;
      parked = null;
      if (restored) return;
      document.documentElement.toggleAttribute(POINTER_FOCUS, !keyboard);
    },
    true,
  );
  // Sign-out and account switch clear the record with the session. Nothing
  // here is user-scoped, and the next press or keystroke would overwrite it
  // anyway, but a `keyboard` left true carries one account's keystroke into
  // the first focus the next one is handed, and focus the page places on its
  // own is exactly what this module holds the ring off for.
  registerAuthReset(() => {
    pointer = false;
    keyboard = false;
    parked = null;
    document.documentElement.toggleAttribute(POINTER_FOCUS, false);
  });
}

export function lastInputWasPointer(): boolean {
  return pointer;
}

/**
 * `onCloseAutoFocus` for a Radix overlay. Radix hands focus back to the trigger
 * on close so a keyboard user keeps their place, and Chromium carries
 * :focus-visible across that programmatic move, so an overlay opened and
 * dismissed with the mouse would leave the trigger ringed until the next click.
 * Skip the restore when no key was pressed: focus falls to <body>, which is
 * where clicking anywhere else would have put it anyway.
 */
export function closeAutoFocus(userHandler?: (event: Event) => void) {
  return (event: Event) => {
    userHandler?.(event);
    if (!event.defaultPrevented && pointer) event.preventDefault();
  };
}
