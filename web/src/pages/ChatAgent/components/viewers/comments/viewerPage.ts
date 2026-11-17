/**
 * The page's side of the comment protocol for a viewer drawn in the app
 * (Markdown, code, a PDF, a sheet) rather than a framed HTML file's bridge
 * (`src/server/preview_proxy/bridge`). The comment layer is written against
 * `PageBridge`: it posts what `ViewerToPage` lists and hears what the page
 * reports, and this answers both the way the bridge does, so the same layer
 * draws the pins, boxes and list over any viewer.
 *
 * What a click lands on, what a comment saves and how it is found again are
 * the viewer's (`SpotStrategy`). The rest is the same for every viewer:
 * presses (`PressTracker`), the adding key (`AddingKey`), the draft's parts
 * (`DraftParts`), hover (`PageHover`), anchors (`PageAnchors`), and here the
 * protocol, picks, words and scrolling.
 *
 * Every message goes through `parsePageMessage`, so what reaches the layer is
 * cleaned and cut as a framed page's would be.
 */
import {
  MAX_AREA_COUNT, MAX_TITLE, parsePageMessage,
  type PageSelection, type PageToViewer, type PageToViewerWire, type ViewerToPage,
} from '../html/pageProtocol';
import { AddingKey } from './addingKey';
import { DraftParts } from './draftParts';
import { partEntries } from '../../artifactComments/anchorKinds';
import { PageAnchors, round, toFrame } from './pageAnchors';
import { PressTracker, type ReleasedArea } from './pressTracker';
import { clip, squash, trimmed } from './spotKit';
import type { SpotAnchor, SpotPicker, SpotStrategy } from './spotStrategy';
import { selectedWords } from './selectedWords';
import { ADD_KEY } from './partModifier';
import { canMark } from './textMarks';
import { glyphAt } from './glyphs';
import { PageHover } from './pageHover';
import './viewerPage.css';

export interface ViewerPageOptions {
  /** The workspace file shown, which the hello names. */
  path: string;
  /**
   * The title a comment keeps for the file, read off its content; left out,
   * it keeps the file's path. A module's function, as a new one remakes the page.
   */
  title?: (content: Element) => string | null;
  emit: (message: PageToViewer) => void;
  /** How a report waits for the next frame; tests run it at once. */
  schedule?: (run: () => void) => () => void;
}

/** A pick of spots, as the page sends it: the anchor is what a viewer's spots save. */
type SpotWire = Extract<PageToViewerWire, { type: 'page:pick' | 'page:area' }> & { anchor: SpotAnchor };
/** Words picked, as the page sends them. */
type WordsWire = Extract<PageToViewerWire, { type: 'page:selection'; anchor: unknown }>;

/** Events a press in comment mode is kept from: it is the reader's pick, not the viewer's. */
const GUARDED = ['pointerdown', 'pointerup', 'mousedown', 'mouseup', 'touchstart', 'touchend', 'dblclick', 'auxclick', 'contextmenu'] as const;
const SCROLL_IDLE_MS = 120;
/** How long a scroll to a parked comment goes on to its spot once that is drawn; later, the reader has moved on. */
const FOLLOW_MS = 3000;

const nextFrame = (run: () => void) => {
  const id = requestAnimationFrame(run);
  return () => cancelAnimationFrame(id);
};

export class ViewerPage {
  private mode = false;
  private readonly anchors: PageAnchors;
  private readonly parts: DraftParts;
  private readonly hover: PageHover;
  private readonly key: AddingKey;
  private readonly tracker: PressTracker;
  private settlePending = true;
  private scrollTimer: ReturnType<typeof setTimeout> | null = null;
  /** Lets the selection go after the release that made it. */
  private unselectTimer: ReturnType<typeof setTimeout> | null = null;
  private pointerWatched = false;
  /** The last press the viewer said opens nothing (`page:declined`). */
  private declined: number | null = null;
  private cancelFrame: (() => void) | null = null;
  /** A comment scrolled to while parked, and where: the scroll goes on as it moves, until its spot is drawn. */
  private following: { id: string; until: number; at: Element } | null = null;
  private readonly schedule: (run: () => void) => () => void;
  /** What a click picks; none where the content takes words alone. */
  private readonly picker: SpotPicker | undefined;

  constructor(
    private readonly scroller: HTMLElement,
    private readonly content: HTMLElement,
    private readonly strategy: SpotStrategy,
    private readonly opts: ViewerPageOptions,
  ) {
    this.schedule = opts.schedule ?? nextFrame;
    this.picker = strategy.spots;
    this.anchors = new PageAnchors(scroller, strategy, (wire) => this.send(wire));
    this.parts = new DraftParts(this.anchors, this.picker);
    this.hover = new PageHover(scroller, content, this.picker, this.anchors, (wire) => this.send(wire), (spot) => (this.key.on ? this.parts.around(spot) : spot));
    this.key = new AddingKey((held, next) => {
      if (this.mode) this.send({ type: 'page:adding', held, next });
    });
    this.tracker = new PressTracker(scroller, content, {
      mode: () => this.mode,
      extending: (e) => this.mode && (e ? this.key.adds(e) : this.key.next),
      readKeys: (e) => this.readModifier(e),
      // A press adding parts takes spots, so on glyphs too it draws an area.
      selectsWords: (x, y, extend) => !this.picker || (!extend && this.wordsAt(x, y)),
      began: () => this.anchors.markPending(null),
      dragging: (phase) => {
        this.hover.set(null);
        if (phase === 'words') this.showCursor('text');
      },
      area: (area) => this.reportArea(area),
      words: (press, release) => this.composeText(press, release),
      send: (wire) => this.send(wire),
    });
  }

  /** Starts listening and says hello; returns the teardown. */
  attach(): () => void {
    const c = this.content;
    const listening = new AbortController();
    const { signal } = listening;
    const capture = { capture: true, signal };
    c.addEventListener('pointerdown', (e) => this.tracker.down(e), capture);
    c.addEventListener('mousedown', (e) => this.tracker.mouseDown(e), capture);
    for (const type of GUARDED) c.addEventListener(type, (e) => this.guard(e), capture);
    c.addEventListener('click', (e) => this.onClick(e), capture);
    c.addEventListener('dragstart', (e) => { if (this.mode && this.tracker.underway) e.preventDefault(); }, capture);
    c.addEventListener('pointerover', (e) => this.onPointerOver(e), capture);
    c.addEventListener('pointermove', (e) => this.onHoverMove(e), { passive: true, signal });
    c.addEventListener('pointerleave', () => { if (this.mode) this.hover.clear(); }, { signal });
    // The press's moves and release keep coming here once the pointer leaves the content.
    window.addEventListener('pointermove', (e) => this.tracker.move(e), { capture: true, passive: true, signal });
    window.addEventListener('pointerup', (e) => this.onPointerUp(e), capture);
    window.addEventListener('pointercancel', () => this.tracker.cancel(), capture);
    window.addEventListener('keydown', (e) => this.onKey(e), capture);
    window.addEventListener('keyup', (e) => this.onKey(e), capture);
    window.addEventListener('blur', () => this.key.set(false, this.key.next), { signal });
    this.scroller.addEventListener('scroll', () => this.onScroll(), { passive: true, signal });

    if (typeof ResizeObserver !== 'undefined') {
      const sizes = new ResizeObserver(() => this.request());
      sizes.observe(this.scroller);
      sizes.observe(c);
      signal.addEventListener('abort', () => sizes.disconnect());
    }
    if (typeof MutationObserver !== 'undefined') {
      // The file read again (the agent's edit) renders new blocks, and a
      // viewer showing another part of it in the same cells (a sheet) says so
      // in `data-shows`: each anchor is looked for afresh.
      const changes = new MutationObserver(() => {
        this.anchors.changed();
        this.request();
      });
      changes.observe(c, { childList: true, subtree: true, characterData: true, attributes: true, attributeFilter: ['data-shows'] });
      signal.addEventListener('abort', () => changes.disconnect());
    }

    this.send({ type: 'page:ready', path: this.opts.path });
    this.request();
    return () => {
      listening.abort();
      this.tracker.dispose();
      this.cancelFrame?.();
      this.cancelFrame = null;
      for (const timer of [this.scrollTimer, this.unselectTimer]) if (timer) clearTimeout(timer);
      this.scrollTimer = null;
      this.unselectTimer = null;
      this.hover.dispose();
      this.anchors.dispose();
      delete c.dataset.commentCursor;
    };
  }

  /** What the layer posts, as a framed page's bridge would take it. */
  handle(message: ViewerToPage): void {
    switch (message.type) {
      case 'page:comment-mode':
        this.setMode(message.on);
        return;
      case 'page:anchors':
        if (!this.anchors.set(message.items)) {
          // No comment is being written: there are no parts to add to.
          this.parts.keep(null, null);
          this.key.set(this.key.held, false);
        }
        // Settled again after the answer: a layer that asks after the first
        // report has to hear it to know a spot it does not see is gone.
        this.settlePending = true;
        this.request();
        return;
      case 'page:scroll-to':
        if ('id' in message) this.scrollToId(message.id);
        return;
      case 'page:pointer-watch':
        this.pointerWatched = true;
        return;
      case 'page:clear-selection':
        window.getSelection()?.removeAllRanges();
        this.anchors.markPending(null);
        return;
      case 'page:drop-part':
        this.sendParts(this.parts.without(message.part));
        return;
      case 'page:outlined':
        this.anchors.outline(message.id);
        this.request();
        return;
      case 'page:declined':
        this.declined = message.press;
        return;
      case 'page:extend-next':
        this.key.set(this.key.held, this.mode && message.on);
        this.hoverAgain();
        return;
      default:
        // The theme, a restored scroll and an address are a framed document's; a viewer is drawn in the app.
    }
  }

  /** Run the pending report now. */
  flush(): void {
    this.cancelFrame?.();
    this.cancelFrame = null;
    this.tick();
  }

  private send(wire: PageToViewerWire): void {
    const message = parsePageMessage(wire);
    if (message) this.opts.emit(message);
  }

  private request(): void {
    if (this.cancelFrame) return;
    this.cancelFrame = this.schedule(() => {
      this.cancelFrame = null;
      this.tick();
    });
  }

  private tick(): void {
    if (!this.anchors.report()) this.request();
    const f = this.following;
    if (f && Date.now() > f.until) this.following = null;
    else if (f && this.anchors.parkedIn(f.id)?.parked !== f.at) this.scrollToId(f.id, f.until);
    if (this.settlePending) {
      this.settlePending = false;
      this.send({ type: 'page:settled' });
    }
    if (this.hover.showing()) this.hover.report();
  }

  private within(node: Node | null): boolean {
    return !!node && this.content.contains(node);
  }

  // --- Comment mode ---

  private setMode(on: boolean): void {
    // Told while the mode is still on, so the viewer's "Add a part" lets go.
    if (!on) this.key.set(false, false);
    this.mode = on;
    if (on) this.showCursor('crosshair');
    else {
      delete this.content.dataset.commentCursor;
      this.tracker.end();
      this.hover.clear();
    }
  }

  private showCursor(name: 'crosshair' | 'text' | 'copy'): void {
    if (this.content.dataset.commentCursor !== name) this.content.dataset.commentCursor = name;
  }

  private readModifier(e: MouseEvent): void {
    if (this.mode && this.key.read(e)) this.hoverAgain();
  }

  private onKey(e: KeyboardEvent): void {
    if (!this.mode) return;
    if (e.key === ADD_KEY) {
      this.key.set(e.type === 'keydown', this.key.next);
      this.hoverAgain();
      return;
    }
    if (e.type === 'keydown' && e.key === 'Escape') this.tracker.escape(e);
  }

  /** In comment mode a press is the reader's: nothing the viewer listens for hears it. */
  private guard(e: Event): void {
    if (this.mode) e.stopPropagation();
  }

  // --- Picks ---

  private onPointerUp(e: PointerEvent): void {
    this.tracker.up(e);
    if (this.mode && e.pointerType !== 'touch') this.hover.at(e.target, e.clientX, e.clientY);
  }

  private onClick(e: MouseEvent): void {
    if (!this.mode) return;
    e.preventDefault();
    e.stopPropagation();
    const click = this.tracker.click(e);
    if (!click || e.detail > 1 || e.button !== 0 || !(e.target instanceof Element)) return;
    const spot = this.picker?.spotAt(e.target) ?? null;
    if (click.adds) {
      this.key.set(this.key.held, false);
      if (spot) this.sendParts(this.parts.toggled(spot));
      return;
    }
    // A click the pointer made ends the latest press; a key's ends none.
    const message = spot ? this.spotMessage([spot], { press: click.press }) : null;
    if (message) this.sendSpot(message);
  }

  /**
   * A released area: two spots or more are one comment on all of them; one,
   * or none, is that spot's comment as a click makes it, the one the drag
   * began on when it touched none.
   */
  private reportArea(area: ReleasedArea): void {
    const found = this.picker?.spotsIn(area.box) ?? [];
    if (area.extend) {
      // The drag was the asked-for tap: its click is swallowed, so it is let go here.
      this.key.set(this.key.held, false);
      if (found.length) this.sendParts(this.parts.join(this.parts.current(), found));
      return;
    }
    // A drag lists up to the API's limit of parts and counts the rest, as a page's does.
    const spots = this.parts.join([], found, MAX_AREA_COUNT);
    if (!spots.length) {
      const under = document.elementFromPoint(area.x, area.y);
      const spot = under && this.within(under) ? (this.picker?.spotAt(under) ?? null) : null;
      if (spot) spots.push(spot);
    }
    const message = spots.length ? this.spotMessage(spots, { press: area.press }) : null;
    if (!message) return;
    this.sendSpot(message);
    // One too large to list is no list of parts, and one the viewer declined
    // is not the draft's: a modifier click starts from the draft's own.
    const listed = partEntries(message.anchor);
    if (listed && area.press !== this.declined) this.parts.keep(spots, listed);
  }

  /** The pick for spots in document order: one spot's anchor, or what several save. */
  private spotMessage(spots: Element[], { press, extend }: { press?: number | null; extend?: boolean }): SpotWire | null {
    const edges = this.anchors.edgesOfSpots(spots);
    const anchor = edges ? (this.picker?.anchorOf(spots) ?? null) : null;
    if (!edges || !anchor) return null;
    const rest = { rect: toFrame(this.scroller, edges), ...(press ? { press } : {}), ...(extend ? { extend } : {}) };
    return anchor.kind === 'area' ? { type: 'page:area', anchor, ...rest } : { type: 'page:pick', anchor, ...rest };
  }

  /** The file's title, which a new comment keeps. */
  private withTitle<T extends SpotWire | WordsWire>(message: T): T {
    const title = clip(squash(this.opts.title?.(this.content)), MAX_TITLE);
    return title ? { ...message, title } : message;
  }

  /** Any spot but the draft's parts grown or shrunk starts a new comment. */
  private sendSpot(message: SpotWire): void {
    if (!message.extend) this.parts.keep(null, null);
    this.send(this.withTitle(message));
  }

  // --- The draft's parts ---

  /** The draft's parts are now `spots`: sent with `extend`, so the viewer keeps the box and its words on them. */
  private sendParts(spots: Element[] | null): void {
    const message = spots?.length ? this.spotMessage(spots, { extend: true }) : null;
    if (!spots || !message) return;
    this.parts.keep(spots, partEntries(message.anchor));
    this.sendSpot(message);
    this.hoverAgain();
  }

  // --- Hover ---

  private onPointerOver(e: PointerEvent): void {
    if (!this.mode || e.pointerType === 'touch' || this.tracker.pressing()) return;
    // Not read for the add key: Chrome also sends this when what is under a
    // still pointer changes, as the box fading over it, and off a Mac with no
    // keys held. A real move's pointermove reads it.
    this.hover.at(e.target, e.clientX, e.clientY);
  }

  private onHoverMove(e: PointerEvent): void {
    if (this.pointerWatched) {
      this.pointerWatched = false;
      this.send({ type: 'page:pointer-in' });
    }
    if (!this.mode || e.pointerType === 'touch' || this.tracker.pressing()) return;
    this.readModifier(e);
    this.hover.pointer = { x: e.clientX, y: e.clientY };
    this.showCursor(this.key.on ? 'copy' : !this.picker || this.wordsAt(e.clientX, e.clientY) ? 'text' : 'crosshair');
  }

  /** Whether a press here selects words: on glyphs, in a viewer that saves words at all (a sheet saves cells). */
  private wordsAt(x: number, y: number): boolean {
    return !!this.strategy.words && glyphAt(x, y, this.content);
  }

  /** What a click under a still pointer takes changed: the spot, or the part around it. */
  private hoverAgain(): void {
    if (this.mode && !this.tracker.pressing()) this.hover.again();
  }

  // --- Words ---

  private selectedRange(): Range | null {
    const sel = window.getSelection();
    if (!sel || sel.isCollapsed || !sel.rangeCount) return null;
    const range = sel.getRangeAt(0);
    return this.within(range.commonAncestorContainer) ? range : null;
  }

  /**
   * Words selected in comment mode: their box opens at once (`compose`, no
   * chip), paired with the press that made them as a pick is. Marked rather
   * than left selected where the browser can mark them, since a selection
   * greys out once the box takes the focus.
   */
  private composeText(press: number, release: PointerEvent): void {
    const found = selectedWords(this.selectedRange(), release, this.strategy, this.scroller);
    if (!found) return;
    this.parts.keep(null, null);
    // Marked first: a layer that lets the words go answers at once, and its clear must stand.
    const marks = canMark();
    if (marks) this.anchors.markPending(trimmed(found.range));
    this.send(this.withTitle<WordsWire>({ type: 'page:selection', press, compose: true, anchor: found.anchor, rect: found.rect }));
    if (!marks) return;
    // After the release's own handling, which still reads the selection.
    if (this.unselectTimer) clearTimeout(this.unselectTimer);
    this.unselectTimer = setTimeout(() => {
      this.unselectTimer = null;
      window.getSelection()?.removeAllRanges();
    }, 0);
  }

  /**
   * Words the panel's selection toolbar asks a comment on, outside comment
   * mode: the pick a framed page's chip offers, which the layer opens a box
   * on as the chip's Comment does. Marked as words picked in the mode are,
   * since the box takes the focus, and the selection with it.
   */
  /** Whether `range` is in this viewer's content, which the toolbar's Comment can open a box on. */
  holds(range: Range): boolean {
    return this.within(range.commonAncestorContainer);
  }

  wordsPick(live: Range): PageSelection | null {
    if (!this.within(live.commonAncestorContainer)) return null;
    const found = selectedWords(live, null, this.strategy, this.scroller);
    if (!found) return null;
    const message = parsePageMessage(this.withTitle<WordsWire>({ type: 'page:selection', anchor: found.anchor, rect: found.rect }));
    if (message?.type !== 'page:selection' || !message.selection) return null;
    this.parts.keep(null, null);
    if (canMark()) {
      this.anchors.markPending(trimmed(found.range));
      window.getSelection()?.removeAllRanges();
    }
    return message.selection;
  }

  // --- Scrolling ---

  private onScroll(): void {
    const v = this.scroller;
    this.send({ type: 'page:scroll', x: round(v.scrollLeft), y: round(v.scrollTop), moving: true });
    if (this.scrollTimer) clearTimeout(this.scrollTimer);
    this.scrollTimer = setTimeout(() => {
      this.scrollTimer = null;
      this.send({ type: 'page:scroll', x: round(v.scrollLeft), y: round(v.scrollTop), moving: false });
    }, SCROLL_IDLE_MS);
    this.tracker.scrolled();
    this.request();
  }

  /**
   * Centres a comment's spot in the viewport, scrolling only the viewport
   * (scrollIntoView would move the panel too). A parked one's element comes
   * into view instead, and the scroll goes on once its spot is drawn.
   */
  private scrollToId(id: string, until = Date.now() + FOLLOW_MS): void {
    const parked = this.anchors.parkedIn(id);
    this.following = parked ? { id, at: parked.parked, until } : null;
    // One waiting on a view not drawn (another sheet): the viewer draws it, and the follow scrolls on.
    if (parked?.show) {
      parked.show();
      return;
    }
    const edges = this.anchors.edgesFor(id);
    if (!edges) return;
    const v = this.scroller;
    const box = v.getBoundingClientRect();
    const top = v.scrollTop + edges.top - box.top - Math.max(0, (v.clientHeight - (edges.bottom - edges.top)) / 2);
    // Sideways only when it is out of view sideways: a wide sheet's far columns.
    const across = edges.left < box.left || edges.right > box.right;
    const left = across ? v.scrollLeft + edges.left - box.left - Math.max(0, (v.clientWidth - (edges.right - edges.left)) / 2) : v.scrollLeft;
    const still = typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
    v.scrollTo({ top: Math.max(0, top), left: Math.max(0, left), behavior: still ? 'auto' : 'smooth' });
  }
}
