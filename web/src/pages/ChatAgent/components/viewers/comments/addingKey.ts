/**
 * Whether a click adds to the draft's parts rather than starting a comment:
 * the modifier held, or the next tap asked for by the viewer's "Add a part".
 * The bridge's `comments-parts.js` keeps the same two.
 */
import { addKeyDown } from './partModifier';

export class AddingKey {
  held = false;
  next = false;

  /** `announce` hears each change, for the viewer to show it. */
  constructor(private readonly announce: (held: boolean, next: boolean) => void) {}

  get on(): boolean {
    return this.held || this.next;
  }

  /** Returns whether anything changed. */
  set(held: boolean, next: boolean): boolean {
    if (held === this.held && next === this.next) return false;
    this.held = held;
    this.next = next;
    this.announce(held, next);
    return true;
  }

  /** Whether a press or click with `e`'s keys adds. */
  adds(e: MouseEvent): boolean {
    return addKeyDown(e) || this.next;
  }

  /** A modifier pressed while the focus was elsewhere reaches the page only on the events it carries. Returns whether it changed. */
  read(e: MouseEvent): boolean {
    return this.set(addKeyDown(e), this.next);
  }
}
