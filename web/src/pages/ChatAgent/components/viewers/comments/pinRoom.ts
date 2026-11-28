/**
 * Where a pin goes so it covers no words: the same code in the app's viewers
 * (`pageAnchors.ts`) and in a framed page, whose bridge part
 * `comments-shared.generated.js` is compiled from this file and `pageText.ts`
 * (see there for what that asks of the code). A pin is a bubble whose
 * bottom-left corner is its tail, so a point here is that corner and the
 * bubble fills the square above and to the right of it.
 *
 * Words take their pin just past the end of the selection's last line, in the
 * room after that line's text and level with it, out into the pane's padding
 * if it must; where the line runs to the pane's edge, in the margin beside
 * its block, right then left. A block keeps the place it always had (where it
 * was clicked, or its top-right corner) unless that covers its own text, and
 * then moves the same way, level with its first line. Only where neither has
 * room does a pin sit on text.
 *
 * Measuring that room reads every line box near a spot, which a report made
 * on each scroll frame cannot afford for every spot. So a report keeps each
 * pin from the last one while its spot has only scrolled (`PinMemo`), and
 * measures a block's text once for all the pins beside it (`PinPass`).
 */

/** A box in client coordinates. */
export interface Edges {
  left: number;
  top: number;
  right: number;
  bottom: number;
}

export type Point = { x: number; y: number };

/** The box that holds both; the first is null before any box was taken. */
export function union(a: Edges | null, r: Edges): Edges {
  if (!a) return { left: r.left, top: r.top, right: r.right, bottom: r.bottom };
  return {
    left: Math.min(a.left, r.left), top: Math.min(a.top, r.top),
    right: Math.max(a.right, r.right), bottom: Math.max(a.bottom, r.bottom),
  };
}

/** A pin's size in px: the bubble is this square, its tail one corner of it. */
export const PIN_SIZE = 22;
/**
 * Room between a pin and the text beside it: the open pin's rings reach
 * 4.5 px past its edge (`PinBadge.css`), and about 6 px clear of the ink
 * beyond them reads as a pin beside the word, not one touching it.
 */
export const PIN_GAP = 10;
/**
 * Characters measured for a pin's room: a block's own up to the first (a PDF
 * page is one), its neighbours' within the second. Past them a pin keeps to
 * what it was given, so a huge block costs no frame.
 */
export const PIN_MAX_OWN = 12000;
export const PIN_MAX_NEAR = 4000;

/**
 * Where a pin stands with nothing to move it off: a spot clicked at a point,
 * at that point; words at their top-right corner; any other spot inside its
 * top-right corner.
 */
export function pinDefault(
  anchor: { kind: string; point?: Point | null }, rect: { x: number; y: number; width: number; height: number },
): Point {
  const point = anchor.point;
  if (point) return { x: rect.x + point.x * rect.width, y: rect.y + point.y * rect.height };
  if (anchor.kind === 'text') return { x: rect.x + rect.width, y: rect.y };
  return { x: rect.x + Math.max(0, rect.width - PIN_SIZE - 2), y: rect.y + Math.min(rect.height, PIN_SIZE + 2) };
}

/** Line boxes of the text under some elements, with what could move that text apart from a spot beside it. */
export interface PinText {
  rects: DOMRect[];
  /** How many of `rects` are the block's own, ahead of its neighbours'. */
  own: number;
  /** Some element of it is fixed or sticky, so its text need not move as the content scrolls. */
  pinned: boolean;
  /** Elements of it that scroll their own content: its text sits at their offsets. */
  scrollers: Element[];
}

/**
 * Text measured for pins during one report, by element, so pins beside one
 * block measure its text once. Layout holds still within a report, so a pass
 * serves one report and the next starts its own.
 */
export interface PinPass {
  own: Map<Element, PinText>;
  around: Map<Element, PinText | null>;
}

export function pinPass(): PinPass {
  return { own: new Map(), around: new Map() };
}

/**
 * A pin placed on an earlier frame, kept as an offset from its spot with
 * what it was placed for. Everything a pin's place is read from moves with
 * the content as it scrolls except the view, which enters through the floor
 * its top edge sets on a pin (`pinBesideLine`) and through its left and
 * right. So a spot of the same size, at the same place across a view of the
 * same width, takes its kept pin moved with it while the floor stays clear
 * of it or its line is out above the view, the text measured holds nothing fixed or sticky, every scroller in
 * that text keeps its offsets, a block's default point (`at`) stays on the
 * same side of every line it could cover, and several spots keep their
 * places in one another. The keeper drops it whenever anchors are found
 * again, since layout can change without the spot's own box changing.
 */
export interface PinMemo {
  /** The spot's left edge from the view's, its size, and the view's width. */
  left: number;
  width: number;
  height: number;
  span: number;
  /** Null kept: the caller's default stands. Else the pin from the spot's top-left corner. */
  moved: boolean;
  dx: number;
  dy: number;
  /** The lowest level a pin was tried at, from the spot's top; Infinity where none was. */
  floor: number;
  /** The foot of the line it was placed beside, from the spot's top: the floor holds only while that line shows. */
  lineBottom: number;
  /** A block's default point from the spot's left, whether it covered text, and the band of its level, from the spot's top, over which that holds. */
  atX: number;
  covered: boolean;
  atLow: number;
  atHigh: number;
  scrollers: Element[];
  /** Each scroller's left and top offsets, in turn. */
  scrolls: number[];
  /** Several spots, each one's top-left corner from the spot's, in turn. */
  parts: Element[];
  partAt: number[];
}

/** A pin's place, null for the caller's default, with what keeps it for the next report. */
export interface PinPlaced {
  at: Point | null;
  memo: PinMemo | null;
}

/** What placing one pin read beyond its spot's box, from which its memo is made. */
interface PinMeasure {
  floor: number;
  /** The foot of the line the pin was placed beside, above which the view's top sets no floor. */
  lineBottom: number;
  /** The floor moved a level, so the place depends on where the view's top is. */
  clamped: boolean;
  pinned: boolean;
  scrollers: Element[];
  covered: boolean;
  atLow: number;
  atHigh: number;
}

function pinMeasure(): PinMeasure {
  return { floor: Infinity, lineBottom: -Infinity, clamped: false, pinned: false, scrollers: [], covered: false, atLow: -Infinity, atHigh: Infinity };
}

function pinNote(m: PinMeasure | null, text: PinText): void {
  if (!m) return;
  if (text.pinned) m.pinned = true;
  for (let i = 0; i < text.scrollers.length; i++) m.scrollers.push(text.scrollers[i]);
}

function pinPushAll(out: DOMRect[], rects: readonly DOMRect[]): void {
  for (let i = 0; i < rects.length; i++) out.push(rects[i]);
}

function pinPushRects(range: Range, out: DOMRect[]): DOMRect[] {
  // A browser without line boxes (a test's) has nothing to measure.
  if (typeof range.getClientRects !== 'function') return out;
  const rects = range.getClientRects();
  for (let i = 0; i < rects.length; i++) if (rects[i].width > 0 && rects[i].height > 0) out.push(rects[i]);
  return out;
}

function pinRangeRects(range: Range): DOMRect[] {
  return pinPushRects(range, []);
}

function pinScrolls(overflow: string): boolean {
  return overflow === 'auto' || overflow === 'scroll' || overflow === 'hidden' || overflow === 'overlay';
}

function pinNoteBox(el: Element, view: Window | null, text: PinText): void {
  if (!view) return;
  const style = view.getComputedStyle(el);
  if (style.position === 'fixed' || style.position === 'sticky') text.pinned = true;
  if (pinScrolls(style.overflowX) || pinScrolls(style.overflowY)) text.scrollers.push(el);
}

/** The line boxes of the text under `node`, text alone: a range over an element also lists the boxes of the elements in it. */
function pinTextRects(node: Element): PinText {
  const out: PinText = { rects: [], own: 0, pinned: false, scrollers: [] };
  const doc = node.ownerDocument;
  if (!doc) return out;
  const view = doc.defaultView;
  const range = doc.createRange();
  pinNoteBox(node, view, out);
  // 5: NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT.
  const walker = doc.createTreeWalker(node, 5);
  for (let n = walker.nextNode(); n; n = walker.nextNode()) {
    // An element with nothing in it holds no text to move.
    if (n.nodeType === 1) {
      if (n.firstChild) pinNoteBox(n as Element, view, out);
      continue;
    }
    if (!/\S/.test(n.nodeValue || '')) continue;
    range.selectNodeContents(n);
    pinPushRects(range, out.rects);
  }
  out.own = out.rects.length;
  return out;
}

function pinTextOf(el: Element, pass: PinPass | null): PinText {
  const kept = pass ? pass.own.get(el) : undefined;
  if (kept) return kept;
  const text = pinTextRects(el);
  if (pass) pass.own.set(el, text);
  return text;
}

/** The box a line of text is laid out in: the nearest ancestor in the flow that is not inline. */
function pinBlockOf(node: Node, root: Element, m: PinMeasure | null): Element {
  let el: Element | null = node.nodeType === 1 ? (node as Element) : node.parentElement;
  const view = node.ownerDocument ? node.ownerDocument.defaultView : null;
  while (el && el !== root && view) {
    const style = view.getComputedStyle(el);
    const flows = style.position === 'static' || style.position === 'relative' || style.position === 'sticky';
    if (flows && style.display.indexOf('inline') !== 0 && style.display !== 'contents') return el;
    // Words in a fixed or sticky box passed over need not move with their block.
    if (m && (style.position === 'fixed' || style.position === 'sticky')) m.pinned = true;
    el = el.parentElement;
  }
  return el || root;
}

function pinTextLength(el: Element): number {
  return (el.textContent || '').length;
}

/** The text a pin beside `block` could cover: its own and, where they are short enough, its neighbours'. Null when the block itself is too long. */
function pinTextAround(block: Element, pass: PinPass | null): PinText | null {
  const kept = pass ? pass.around.get(block) : undefined;
  if (kept !== undefined) return kept;
  let out: PinText | null = null;
  if (pinTextLength(block) <= PIN_MAX_OWN) {
    const near: Element[] = [];
    if (block.previousElementSibling) near.push(block.previousElementSibling);
    if (block.nextElementSibling) near.push(block.nextElementSibling);
    let length = 0;
    for (let i = 0; i < near.length; i++) length += pinTextLength(near[i]);
    const own = pinTextOf(block, pass);
    out = own;
    if (near.length && length <= PIN_MAX_NEAR) {
      // A copy: the block's own measure stays as it is for the pins that want it alone.
      out = { rects: own.rects.slice(), own: own.rects.length, pinned: own.pinned, scrollers: own.scrollers.slice() };
      for (let j = 0; j < near.length; j++) {
        const text = pinTextOf(near[j], pass);
        pinPushAll(out.rects, text.rects);
        if (text.pinned) out.pinned = true;
        for (let k = 0; k < text.scrollers.length; k++) out.scrollers.push(text.scrollers[k]);
      }
    }
  }
  if (pass) pass.around.set(block, out);
  return out;
}

function pinCovers(x: number, y: number, text: readonly Edges[]): boolean {
  for (let i = 0; i < text.length; i++) {
    const r = text[i];
    if (r.left < x + PIN_SIZE && r.right > x && r.top < y && r.bottom > y - PIN_SIZE) return true;
  }
  return false;
}

/**
 * Notes the band of levels over which a pin at `at`, moved up or down alone,
 * covers `text` or not as it does now: whether a block's pin moves is decided
 * by that, and its default point comes from a rounded box, so it shifts by a
 * fraction of a pixel against the text as the content scrolls.
 */
function pinAtBand(m: PinMeasure, at: Point, text: readonly Edges[]): void {
  let coverLow = Infinity;
  let coverHigh = -Infinity;
  let low = -Infinity;
  let high = Infinity;
  for (let i = 0; i < text.length; i++) {
    const r = text[i];
    if (!(r.left < at.x + PIN_SIZE && r.right > at.x)) continue;
    const from = r.top;
    const to = r.bottom + PIN_SIZE;
    if (from < at.y && to > at.y) {
      coverLow = Math.min(coverLow, from);
      coverHigh = Math.max(coverHigh, to);
    } else if (to <= at.y) low = Math.max(low, to);
    else high = Math.min(high, from);
  }
  m.covered = coverLow < coverHigh;
  m.atLow = m.covered ? coverLow : low;
  m.atHigh = m.covered ? coverHigh : high;
}

/** The line `ref` is on: every box whose middle is within it, spanned top to bottom and to its right end. */
function pinLineBand(rects: readonly DOMRect[], ref: DOMRect): { top: number; bottom: number; right: number } {
  const band = { top: ref.top, bottom: ref.bottom, right: ref.right };
  for (let i = 0; i < rects.length; i++) {
    const mid = (rects[i].top + rects[i].bottom) / 2;
    if (mid < ref.top || mid > ref.bottom) continue;
    band.top = Math.min(band.top, rects[i].top);
    band.bottom = Math.max(band.bottom, rects[i].bottom);
    band.right = Math.max(band.right, rects[i].right);
  }
  return band;
}

/**
 * The tail of a pin level with `line`: in the room after the text, out into
 * the pane's padding if it must, else the margin beside `box`, right then
 * left, inside `view`. Each is tried centred on the line, then standing on
 * its foot, then hanging from its top, since a pin taller than a line would
 * otherwise reach the lines around it. Null when none is free.
 */
function pinBesideLine(line: { top: number; bottom: number }, box: Edges, view: Edges, text: readonly Edges[], m: PinMeasure | null): Point | null {
  const levels = [(line.top + line.bottom) / 2 + PIN_SIZE / 2, line.bottom, line.top + PIN_SIZE];
  // The floor keeps a pin whole while its line shows at the view's top edge.
  // A line scrolled out above that edge takes its pin with it, or the pins
  // of everything read past would gather beside whatever line is there.
  const floor = line.bottom > view.top ? view.top + PIN_SIZE : -Infinity;
  if (m) m.lineBottom = line.bottom;
  for (let l = 0; l < levels.length; l++) {
    if (m) {
      m.floor = Math.min(m.floor, levels[l]);
      if (levels[l] < floor) m.clamped = true;
    }
    levels[l] = Math.max(floor, levels[l]);
  }
  const fits = function (x: number, y: number): boolean {
    return x >= view.left && x + PIN_SIZE <= view.right && !pinCovers(x, y, text);
  };
  for (let i = 0; i < levels.length; i++) {
    let end = -Infinity;
    for (let j = 0; j < text.length; j++) {
      if (text[j].top < levels[i] && text[j].bottom > levels[i] - PIN_SIZE) end = Math.max(end, text[j].right);
    }
    if (end > -Infinity && fits(end + PIN_GAP, levels[i])) return { x: end + PIN_GAP, y: levels[i] };
  }
  const sides = [box.right + PIN_GAP, box.left - PIN_GAP - PIN_SIZE];
  for (let k = 0; k < sides.length; k++) {
    for (let n = 0; n < levels.length; n++) if (fits(sides[k], levels[n])) return { x: sides[k], y: levels[n] };
  }
  return null;
}

function placeWordsPin(range: Range, root: Element, view: Edges, pass: PinPass | null, m: PinMeasure | null): Point | null {
  const rects = pinRangeRects(range);
  if (!rects.length) return null;
  let last = rects[0];
  for (let i = 1; i < rects.length; i++) {
    if (rects[i].bottom > last.bottom || (rects[i].bottom === last.bottom && rects[i].right > last.right)) last = rects[i];
  }
  const band = pinLineBand(rects, last);
  const block = pinBlockOf(range.endContainer, root, m);
  const around = pinTextAround(block, pass);
  if (around) pinNote(m, around);
  const placed = pinBesideLine(band, block.getBoundingClientRect(), view, around ? around.rects : rects, m);
  // Nowhere free: just past the words, kept in view.
  return placed || { x: Math.min(band.right + PIN_GAP, view.right - PIN_SIZE), y: (band.top + band.bottom) / 2 + PIN_SIZE / 2 };
}

function placeBlockPin(spots: readonly Element[], bounds: Edges, at: Point, view: Edges, pass: PinPass | null, m: PinMeasure | null): Point | null {
  let length = 0;
  for (let i = 0; i < spots.length; i++) length += pinTextLength(spots[i]);
  if (length > PIN_MAX_OWN) return null;
  let own: DOMRect[];
  if (spots.length === 1) {
    const text = pinTextOf(spots[0], pass);
    pinNote(m, text);
    own = text.rects;
  } else {
    own = [];
    for (let j = 0; j < spots.length; j++) {
      const text = pinTextOf(spots[j], pass);
      pinNote(m, text);
      pinPushAll(own, text.rects);
    }
  }
  if (m) pinAtBand(m, at, own);
  if (!pinCovers(at.x, at.y, own)) return null;
  let first = own[0];
  for (let n = 1; n < own.length; n++) if (own[n].top < first.top) first = own[n];
  // One spot is a block, and its neighbours' words count too; never null, the block being short enough.
  const around = spots.length === 1 ? pinTextAround(spots[0], pass) : null;
  if (around) pinNote(m, around);
  return pinBesideLine(pinLineBand(own, first), bounds, view, around ? around.rects : own, m);
}

/** Where the pin on selected words goes, in client coordinates, or null when the words show nowhere. */
export function wordsPin(range: Range, root: Element, view: Edges): Point | null {
  return placeWordsPin(range, root, view, null, null);
}

/**
 * Where the pin on a block goes once `at`, where it stands by default, would
 * cover the block's own text; null keeps it at `at`.
 */
export function blockPin(spots: readonly Element[], bounds: Edges, at: Point, view: Edges): Point | null {
  return placeBlockPin(spots, bounds, at, view, null, null);
}

/** The pin `memo` keeps for `spot` now; undefined where it must be placed afresh. */
function pinRecall(memo: PinMemo | null, spot: Edges, view: Edges, at: Point | null): Point | null | undefined {
  if (!memo) return undefined;
  if (spot.left - view.left !== memo.left || spot.right - spot.left !== memo.width || spot.bottom - spot.top !== memo.height) return undefined;
  if (view.right - view.left !== memo.span) return undefined;
  // The floor would move it: only while its line still shows and a level tried lies above the floor.
  if (spot.top + memo.lineBottom > view.top && spot.top + memo.floor < view.top + PIN_SIZE) return undefined;
  if (at) {
    const y = at.y - spot.top;
    if (at.x - spot.left !== memo.atX) return undefined;
    if (memo.covered ? !(y > memo.atLow && y < memo.atHigh) : y < memo.atLow || y > memo.atHigh) return undefined;
  }
  for (let i = 0; i < memo.scrollers.length; i++) {
    const el = memo.scrollers[i];
    if (el.scrollLeft !== memo.scrolls[2 * i] || el.scrollTop !== memo.scrolls[2 * i + 1]) return undefined;
  }
  for (let j = 0; j < memo.parts.length; j++) {
    const r = memo.parts[j].getBoundingClientRect();
    if (r.left - spot.left !== memo.partAt[2 * j] || r.top - spot.top !== memo.partAt[2 * j + 1]) return undefined;
  }
  return memo.moved ? { x: spot.left + memo.dx, y: spot.top + memo.dy } : null;
}

/** What keeps a pin just placed for later frames; null where its place depends on the view's top or on text that may move apart. */
function pinMemoOf(spot: Edges, view: Edges, at: Point | null, placed: Point | null, m: PinMeasure, spots: readonly Element[]): PinMemo | null {
  if (m.clamped || m.pinned) return null;
  const scrolls: number[] = [];
  for (let i = 0; i < m.scrollers.length; i++) scrolls.push(m.scrollers[i].scrollLeft, m.scrollers[i].scrollTop);
  const parts: Element[] = [];
  const partAt: number[] = [];
  for (let j = 0; spots.length > 1 && j < spots.length; j++) {
    const r = spots[j].getBoundingClientRect();
    parts.push(spots[j]);
    partAt.push(r.left - spot.left, r.top - spot.top);
  }
  return {
    left: spot.left - view.left, width: spot.right - spot.left, height: spot.bottom - spot.top, span: view.right - view.left,
    moved: !!placed, dx: placed ? placed.x - spot.left : 0, dy: placed ? placed.y - spot.top : 0,
    floor: m.floor - spot.top, lineBottom: m.lineBottom - spot.top,
    atX: at ? at.x - spot.left : 0, covered: m.covered, atLow: m.atLow - spot.top, atHigh: m.atHigh - spot.top,
    scrollers: m.scrollers, scrolls: scrolls, parts: parts, partAt: partAt,
  };
}

/**
 * `wordsPin` for a report: kept from `memo` while it holds (see `PinMemo`),
 * else placed with the text `pass` has measured. `spot` is the words' box,
 * unrounded, which a kept pin moves with.
 */
export function keptWordsPin(range: Range, root: Element, view: Edges, spot: Edges, pass: PinPass, memo: PinMemo | null): PinPlaced {
  const again = pinRecall(memo, spot, view, null);
  if (again !== undefined) return { at: again, memo: memo };
  const m = pinMeasure();
  const at = placeWordsPin(range, root, view, pass, m);
  return { at: at, memo: pinMemoOf(spot, view, null, at, m, []) };
}

/** `blockPin` for a report, as `keptWordsPin`; `spot` is the spots' box unrounded, where `bounds` may be rounded. */
export function keptBlockPin(
  spots: readonly Element[], bounds: Edges, at: Point, view: Edges, spot: Edges, pass: PinPass, memo: PinMemo | null,
): PinPlaced {
  const again = pinRecall(memo, spot, view, at);
  if (again !== undefined) return { at: again, memo: memo };
  const m = pinMeasure();
  const placed = placeBlockPin(spots, bounds, at, view, pass, m);
  return { at: placed, memo: pinMemoOf(spot, view, at, placed, m, spots) };
}
