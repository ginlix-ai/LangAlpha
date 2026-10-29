/**
 * A disclosure toggle in the transcript (turn fold, activity accordion,
 * reasoning row) announces itself so the scroll controller keeps the toggled
 * row where the reader clicked it and lets the content grow downward. Without
 * this, the streaming follow treats the growth like new content and re-pins the
 * bottom, so the row slides up and away from the pointer.
 *
 * A bubbling DOM event rather than a prop chain: the buttons are many levels
 * below the scroll container and none of the layers between care.
 */
export const ANCHORED_TOGGLE_EVENT = 'la:anchored-toggle';

export function announceAnchoredToggle(el: HTMLElement | null | undefined): void {
  el?.dispatchEvent(new CustomEvent(ANCHORED_TOGGLE_EVENT, { bubbles: true }));
}
