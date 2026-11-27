/**
 * Whether a widget frame's prompt call or zoom comes from the user, judged
 * from this side on evidence the frame cannot fake.
 *
 * The frame's own check runs in the widget's realm, where its code can skip
 * it, and `navigator.userActivation` here is this document's: a gesture
 * anywhere in the app makes it active for about five seconds, and a widget can
 * take page focus on a timer with `input.focus()`. So focus and activation
 * alone let a widget post as the user after any click elsewhere. What a
 * cross-origin frame cannot reach is this document's own event stream: a
 * gesture inside the frame fires no event here, while the boundary events of
 * a pointer moving onto the frame element do (for a mouse, and for a tap's
 * compatibility mouse events, in Chromium, WebKit and Gecko alike).
 *
 * The tradeoff is the keyboard. Tabbing into a widget is itself a key press
 * here, which makes this document active, so Enter or Space inside the widget
 * counts only once that activation has lapsed (`ACTIVATION_MS`); a click in
 * the app followed within that window by a click in the widget is refused the
 * same way, as is a retry before the window ends. Refusing costs the user a
 * wait, which `retryAfterRefusal` lets the host show; accepting the wrong one
 * sends a message as the user.
 */

/** Chromium's transient activation lifetime; a parent gesture this recent may be what makes the page active. */
const ACTIVATION_MS = 5000;
/** The host cannot count gestures inside the frame, so it bounds what one frame can be answered. */
export const FRAME_PROMPT_GAP_MS = 1500;
/** Marks a frame whose focus is watched (`WIDGET_FRAME` carries it). */
export const WIDGET_FRAME_ATTR = 'data-widget-frame';
/**
 * Gecko can move focus into a frame by script with no event here at all, so
 * focus is also read on a timer.
 */
const FOCUS_POLL_MS = 250;

let lastParentGesture = -Infinity;
let underPointer: Element | null = null;
let lastFocused: HTMLElement | null = null;
let keyboardEntered: HTMLIFrameElement | null = null;
/** A Tab's landing is still to be read: the focus check waits for it. */
let tabPending = false;
let lastAccepted = new WeakMap<HTMLIFrameElement, number>();
let tookFocus = new WeakMap<HTMLIFrameElement, number>();
let watching = false;
let poll: ReturnType<typeof setInterval> | undefined;
let lastActive: Element | null = null;

function isWidgetFrame(el: Element | null): el is HTMLIFrameElement {
  return el instanceof HTMLIFrameElement && el.hasAttribute(WIDGET_FRAME_ATTR);
}

/**
 * Focus brought in by Tab lands as the key's own default action, before the
 * next task; a frame focused by then was reached by the key, where one
 * focused later took it.
 */
function onTab() {
  const from = document.activeElement;
  tabPending = true;
  setTimeout(() => {
    tabPending = false;
    const el = document.activeElement;
    if (el !== from && isWidgetFrame(el)) keyboardEntered = el;
    checkFocusEntry();
  }, 0);
}

function onGesture(e: Event) {
  lastParentGesture = performance.now();
  if (e.type === 'keydown' && (e as KeyboardEvent).key === 'Tab') onTab();
}

function onPointerOver(e: Event) {
  underPointer = e.target instanceof Element ? e.target : null;
}

function onPointerOut(e: Event) {
  // Out of the window: nothing here is under it any more.
  if (!(e as MouseEvent).relatedTarget) underPointer = null;
}

function onFocusIn(e: Event) {
  const t = e.target;
  if (t instanceof HTMLElement && !(t instanceof HTMLIFrameElement)) lastFocused = t;
  keyboardEntered = null;
}

/**
 * Focus that moved into a widget frame with neither the pointer on it nor a
 * Tab is the widget taking it, and every key the user meant for the composer
 * would land in the widget, so it goes back where it was. Nothing here can
 * stop the frame taking it again (Blink lets script focus inside even an
 * `inert` frame), so a key typed between a take and its return still lands
 * there, and that key is a gesture inside the frame as the browser sees it:
 * the frame is not answered for an activation window after it took focus.
 */
function checkFocusEntry() {
  const el = document.activeElement;
  if (tabPending || !isWidgetFrame(el) || keyboardEntered === el || underPointer === el) return;
  tookFocus.set(el, performance.now());
  if (lastFocused?.isConnected) lastFocused.focus({ preventScroll: true });
  else el.blur();
}

function onFocusLeft() {
  // Once the move has landed: Gecko reports focus leaving for a frame before
  // `activeElement` names it, and Blink moving focus back from inside the
  // blur loses the race to the frame.
  setTimeout(checkFocusEntry, 0);
}

function pollFocus() {
  const el = document.activeElement;
  if (el === lastActive) return;
  lastActive = el;
  checkFocusEntry();
}

/** Starts watching this document's gestures and focus; idempotent. */
export function watchFrameActivation(): void {
  if (watching || typeof window === 'undefined') return;
  watching = true;
  window.addEventListener('pointerdown', onGesture, true);
  window.addEventListener('keydown', onGesture, true);
  // Mouse boundary events, not pointer ones: a tap fires its compatibility
  // mouseover on the frame element here, where its pointer events stay inside.
  document.addEventListener('mouseover', onPointerOver, true);
  document.addEventListener('mouseout', onPointerOut, true);
  document.addEventListener('focusin', onFocusIn, true);
  window.addEventListener('blur', onFocusLeft);
  document.addEventListener('focusout', onFocusLeft, true);
  poll = setInterval(pollFocus, FOCUS_POLL_MS);
}

/** Forgets what was seen and stops watching. For tests. */
export function resetFrameActivation(): void {
  if (watching) {
    window.removeEventListener('pointerdown', onGesture, true);
    window.removeEventListener('keydown', onGesture, true);
    document.removeEventListener('mouseover', onPointerOver, true);
    document.removeEventListener('mouseout', onPointerOut, true);
    document.removeEventListener('focusin', onFocusIn, true);
    window.removeEventListener('blur', onFocusLeft);
    document.removeEventListener('focusout', onFocusLeft, true);
    clearInterval(poll);
  }
  watching = false;
  tabPending = false;
  lastParentGesture = -Infinity;
  underPointer = null;
  lastFocused = null;
  keyboardEntered = null;
  lastActive = null;
  lastAccepted = new WeakMap();
  tookFocus = new WeakMap();
}

/**
 * Whether the user just clicked or pressed a key inside `frame`, and this
 * call may act on it. Yes only with the frame focused, this page active, no
 * gesture of this page's own in the activation window (so the activation came
 * from a frame), the frame not caught taking focus in that window, the
 * pointer on this frame or focus brought in by Tab, and no call accepted from
 * it in the last `FRAME_PROMPT_GAP_MS`. A yes spends that allowance. A
 * browser without `userActivation` cannot say, so it is no.
 */
export function userActedInFrame(frame: HTMLIFrameElement): boolean {
  if (!watching) return false;
  if (document.activeElement !== frame || navigator.userActivation?.isActive !== true) return false;
  const now = performance.now();
  if (now - lastParentGesture < ACTIVATION_MS) return false;
  if (now - (tookFocus.get(frame) ?? -Infinity) < ACTIVATION_MS) return false;
  if (underPointer !== frame && keyboardEntered !== frame) return false;
  if (now - (lastAccepted.get(frame) ?? -Infinity) < FRAME_PROMPT_GAP_MS) return false;
  lastAccepted.set(frame, now);
  return true;
}

/**
 * When a call `userActedInFrame` refused will count, if the refusal looks like
 * the user's own click or key arriving inside the activation window: the frame
 * focused, this page active, the pointer on the frame or focus brought in by
 * Tab, and only the window in the way. Null otherwise, so a widget posting on
 * its own, or a quick second click spent by the gap, shows the user nothing.
 */
export function retryAfterRefusal(frame: HTMLIFrameElement): number | null {
  if (!watching) return null;
  if (document.activeElement !== frame || navigator.userActivation?.isActive !== true) return null;
  if (underPointer !== frame && keyboardEntered !== frame) return null;
  const at = Math.max(lastParentGesture, tookFocus.get(frame) ?? -Infinity) + ACTIVATION_MS;
  return at > performance.now() ? at : null;
}
