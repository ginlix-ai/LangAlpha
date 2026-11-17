/**
 * The presses on a viewer's content. Each is numbered, as a framed page's
 * are, so the viewer pairs a pick with the press that made it; in comment
 * mode a press becomes a click, an area dragged out, or words selected, and
 * the host hears which once it is let go.
 */
import type { PageToViewerWire } from '../html/pageProtocol';
import { toFrame } from './pageAnchors';
import { contextPress } from './partModifier';
import type { Edges } from './spotStrategy';

/** How far a press travels, in px, before it is a drag rather than a click. */
const DRAG_START = 6;

/** A press under way in comment mode, in content coordinates so a scroll mid-drag grows the area. */
interface Drag {
  id: number;
  press: number;
  /**
   * Still a click (`pending`), an area being drawn, words the browser is
   * selecting, or called off by Esc (`dropped`), after which the press picks nothing.
   */
  phase: 'pending' | 'area' | 'words' | 'dropped';
  moved: boolean;
  /** It began on glyphs: it selects words, as anywhere else. */
  text: boolean;
  x: number;
  y: number;
  cx: number;
  cy: number;
  /** The modifier was held at the press: what it takes joins the draft's parts. */
  extend: boolean;
}

interface Held {
  id: number;
  cx: number;
  cy: number;
  extend: boolean;
}

/** An area let go: its box and where it began, in client coordinates. */
export interface ReleasedArea {
  press: number;
  extend: boolean;
  box: Edges;
  x: number;
  y: number;
}

export interface PressHost {
  mode(): boolean;
  /** Whether a press with `e`'s keys adds to the draft's parts; a tap, with no keys, only when the next is asked to. */
  extending(e?: MouseEvent): boolean;
  /** Reads the modifier off a press in comment mode, which a key held while the focus was elsewhere reaches only this way. */
  readKeys(e: MouseEvent): void;
  /** Whether a press at (x, y) selects words rather than drawing an area. */
  selectsWords(x: number, y: number, extend: boolean): boolean;
  /** A press began in comment mode. */
  began(): void;
  /** The press became a drag: the hover outline steps aside until the release. */
  dragging(phase: 'area' | 'words'): void;
  area(area: ReleasedArea): void;
  words(press: number, release: PointerEvent): void;
  send(wire: PageToViewerWire): void;
}

function clamp(n: number, low: number, high: number): number {
  return Math.min(high, Math.max(low, n));
}

export class PressTracker {
  private presses = 0;
  /** The press that adds to the draft: numbered, but never heard by the viewer, as it must not close the box the part joins. */
  private addingPress = -1;
  /** The press whose click a drag ate: the browser still fires one at the release. */
  private swallowed = 0;
  private held: Held | null = null;
  private heldTimer: ReturnType<typeof setTimeout> | null = null;
  private tapping = false;
  private drag: Drag | null = null;

  constructor(
    private readonly scroller: HTMLElement,
    private readonly content: HTMLElement,
    private readonly host: PressHost,
  ) {}

  /** A press is under way in comment mode: a native drag of the content must not start. */
  get underway(): boolean {
    return this.drag !== null;
  }

  /** A drag or a called-off press is under way: hover waits for its release. */
  pressing(): boolean {
    return !!this.drag && this.drag.phase !== 'pending';
  }

  /**
   * A press on the content, in either mode, first closes whatever the viewer
   * has open over it. Each is numbered, and the pick its own click reports
   * carries the number, so the viewer passes over that pick alone.
   */
  private press(quiet: boolean): void {
    this.presses += 1;
    if (quiet) this.addingPress = this.presses;
    else this.host.send({ type: 'page:press', press: this.presses });
  }

  down(e: PointerEvent): void {
    // A Ctrl-press on a Mac opens the context menu, which keeps its release.
    if (e.button !== 0 || contextPress(e)) return;
    // A touch counts once it lifts as a tap: one that scrolls is cancelled first.
    if (e.pointerType === 'touch') {
      this.tapping = true;
      return;
    }
    this.releaseHeld(false);
    this.host.readKeys(e);
    this.held = { id: e.pointerId, cx: e.clientX, cy: e.clientY, extend: this.host.extending(e) };
    this.heldTimer = setTimeout(() => this.releaseHeld(false), 0);
  }

  /**
   * The second and later press of a multi-click belong to the first: they
   * close, pick and start nothing. Only mousedown carries the click count,
   * and it follows its pointerdown, so the press waits for it.
   */
  mouseDown(e: MouseEvent): void {
    if (e.button !== 0) return;
    this.releaseHeld(e.detail > 1);
    // A multi-click's default selects a word or a line, which picks nothing in the mode.
    if (e.detail > 1 && this.host.mode()) e.preventDefault();
  }

  private releaseHeld(continuation: boolean): void {
    const h = this.held;
    this.held = null;
    if (this.heldTimer) clearTimeout(this.heldTimer);
    this.heldTimer = null;
    if (!h || continuation) return;
    this.press(h.extend);
    this.end();
    if (!this.host.mode()) return;
    const text = this.host.selectsWords(h.cx, h.cy, h.extend);
    this.drag = {
      id: h.id, press: this.presses, phase: 'pending', moved: false, text, extend: h.extend,
      x: h.cx + this.scroller.scrollLeft, y: h.cy + this.scroller.scrollTop, cx: h.cx, cy: h.cy,
    };
    // Words pressed on begin a selection of their own, and the mark of words
    // not yet answered for goes: the viewer has had its chance.
    if (text) window.getSelection()?.removeAllRanges();
    this.host.began();
  }

  move(e: PointerEvent): void {
    const d = this.drag;
    if (!d || e.pointerId !== d.id || d.phase === 'dropped') return;
    // A release the page never heard (let go outside the window) ends the press.
    if ((e.buttons & 1) === 0) {
      this.end();
      return;
    }
    d.cx = e.clientX;
    d.cy = e.clientY;
    if (d.phase === 'words') return;
    if (d.phase === 'pending') {
      const dx = e.clientX + this.scroller.scrollLeft - d.x;
      const dy = e.clientY + this.scroller.scrollTop - d.y;
      if (dx || dy) d.moved = true;
      if (dx * dx + dy * dy < DRAG_START * DRAG_START) return;
      if (d.text) {
        // Selecting words, which the browser does as anywhere.
        d.phase = 'words';
        this.host.dragging('words');
        return;
      }
      d.phase = 'area';
      this.holdSelection(true);
      this.host.dragging('area');
    }
    this.reportDrag();
  }

  up(e: PointerEvent): void {
    if (this.tapping) {
      this.tapping = false;
      this.press(this.host.extending());
    }
    const d = this.drag;
    if (!d || e.pointerId !== d.id) return;
    if (d.phase === 'area') {
      d.cx = e.clientX;
      d.cy = e.clientY;
      this.swallowed = d.press;
      const area = { press: d.press, extend: d.extend, box: this.dragArea(d), x: d.x - this.scroller.scrollLeft, y: d.y - this.scroller.scrollTop };
      try {
        this.host.area(area);
      } finally {
        this.end();
      }
    } else if (d.phase === 'words' || d.phase === 'dropped') {
      this.swallowed = d.press;
      this.end();
      if (d.phase === 'words') this.host.words(d.press, e);
      else window.getSelection()?.removeAllRanges();
    } else {
      // A press that moved a little is still a click: what the browser selected on the way is not the reader's.
      const sel = window.getSelection();
      if (sel && !sel.isCollapsed && d.moved) sel.removeAllRanges();
      this.end();
    }
  }

  /** A touch scrolled, or the browser took the press. */
  cancel(): void {
    this.tapping = false;
    this.end();
  }

  /**
   * Esc mid-drag calls the area or the selection off, and only that: the
   * release that follows picks nothing, and the mode stays on.
   */
  escape(e: KeyboardEvent): void {
    const d = this.drag;
    if (!d || d.phase === 'pending') return;
    e.preventDefault();
    e.stopPropagation();
    if (d.phase === 'dropped') return;
    this.swallowed = d.press;
    if (d.phase === 'area') this.end();
    else {
      d.phase = 'dropped';
      this.holdSelection(true);
    }
  }

  /**
   * What a click in comment mode answers to: null for one whose drag was
   * reported, or called off, at the release. Else the press it ends, where
   * the pointer made it, and whether it adds to the draft's parts; a click
   * a key made has no press, and its own keys say.
   */
  click(e: MouseEvent): { press: number | null; adds: boolean } | null {
    if (e.detail === 0) return { press: null, adds: this.host.extending(e) };
    if (this.swallowed && this.swallowed === this.presses) return null;
    return { press: this.presses || null, adds: this.addingPress === this.presses };
  }

  /** The content scrolled under a drag, which grows the area. */
  scrolled(): void {
    if (this.drag?.phase === 'area') this.reportDrag();
  }

  /** Ends the press under way, cleared from the viewer. */
  end(): void {
    const was = this.drag;
    this.drag = null;
    if (was && (was.phase === 'area' || was.phase === 'dropped')) this.holdSelection(false);
    if (was?.phase === 'area') this.host.send({ type: 'page:area-drag', clear: true });
  }

  dispose(): void {
    this.end();
    if (this.heldTimer) clearTimeout(this.heldTimer);
    this.heldTimer = null;
    this.held = null;
    delete this.content.dataset.commentHold;
  }

  /** No words are selected while an area is drawn: the browser began a selection at the press. */
  private holdSelection(on: boolean): void {
    if (!on) {
      delete this.content.dataset.commentHold;
      return;
    }
    this.content.dataset.commentHold = '';
    const sel = window.getSelection();
    if (sel && !sel.isCollapsed) sel.removeAllRanges();
  }

  /** The area dragged out, in client coordinates, kept to the viewport. */
  private dragArea(d: Drag): Edges {
    const v = this.scroller.getBoundingClientRect();
    const x0 = d.x - this.scroller.scrollLeft;
    const y0 = d.y - this.scroller.scrollTop;
    const x1 = clamp(d.cx, v.left, v.right);
    const y1 = clamp(d.cy, v.top, v.bottom);
    return { left: Math.min(x0, x1), top: Math.min(y0, y1), right: Math.max(x0, x1), bottom: Math.max(y0, y1) };
  }

  private reportDrag(): void {
    const d = this.drag;
    if (d?.phase === 'area') this.host.send({ type: 'page:area-drag', press: d.press, rect: toFrame(this.scroller, this.dragArea(d)) });
  }
}
