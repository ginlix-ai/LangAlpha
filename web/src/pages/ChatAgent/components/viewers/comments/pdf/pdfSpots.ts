/**
 * Comments on a PDF's words. A page draws its words in a text layer over its
 * canvas, and only pages near the viewport are drawn at all, so a comment is
 * saved by its page and its words, and found again on that page once its
 * text layer is drawn; until then it waits in the page's slot (`parked`).
 * A drag off the words draws a box on the page instead (`pdfRegions`).
 */
import type { TextAnchor } from '@/types/artifacts';
import { MAX_SELECTION, type Nearness, type TextSource, clip, findQuote, squash, textSource, wordsAround } from '../spotKit';
import { type Reading, placeFraction } from '../pageText';
import type { Found, SpotStrategy } from '../spotStrategy';
import { regionSpots, type RegionSurfaces } from '../region/regionSpots';
import { seenText } from './seenText';

/**
 * `seenText` as the quote search applies it, a character at a time. A text
 * the whole of which `seenText` leaves as it is has no character it changes:
 * each change it makes (a NUL dropped, a radical read as its ideograph, a run
 * of compatibility forms normalized, which a normalized run can never equal)
 * changes the whole text too.
 */
const SEEN: Reading = Object.assign((text: string) => seenText(text), { same: (text: string) => seenText(text) === text });

const PAGE = '.pdf-page[data-pdf-slot]';
const LAYER = '.textLayer';

/** The page an element is on, 1-based, with its text layer; null outside a page's words. */
function pageOf(node: Node): { page: number; layer: Element } | null {
  const el = node instanceof Element ? node : node.parentElement;
  const slot = el?.closest(PAGE);
  const layer = slot?.querySelector(LAYER);
  const index = Number(slot?.getAttribute('data-pdf-slot'));
  return slot && layer && Number.isInteger(index) ? { page: index + 1, layer } : null;
}

/** react-pdf appends this once pdf.js has laid out every word of the layer. */
function drawn(layer: Element | null): layer is Element {
  return !!layer?.querySelector(':scope > .endOfContent');
}

/**
 * A character of a script written without spaces between its words: Han,
 * kana, CJK punctuation and full-width forms (CSS Text's East Asian wide
 * characters, Hangul left out, since Korean spaces its words).
 */
const UNSPACED = /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}\u3000-\u303f\u30fc\uff00-\uffef]/u;

/**
 * What a line break between `before` and `after` reads as: nothing between
 * two characters of a script with no spaces between its words, as CSS
 * collapses a segment break there, else a space. Each side's own character
 * decides, so a break between Han and Latin is a space.
 */
function lineBreak(before: string, after: string): string {
  const last = String.fromCodePoint(before.codePointAt(before.length - 1) ?? 32);
  const tail = before.length > 1 && /[\udc00-\udfff]/.test(last) ? before.slice(-2) : last;
  const first = String.fromCodePoint(after.codePointAt(0) ?? 32);
  return UNSPACED.test(tail) && UNSPACED.test(first) ? '' : ' ';
}

/**
 * The words in a range as a reader reads them (`seenText`). pdf.js ends a
 * line with a `<br>` rather than a space, which `Range.toString` drops, so
 * two lines' words would run together in what the agent reads; between two
 * Chinese or Japanese characters the break is no space at all. A quote saved
 * with a space there is found all the same, since quotes are compared with
 * every space dropped (`normal`).
 */
export function wordsIn(range: Range): string {
  const root = range.commonAncestorContainer;
  if (root instanceof Text) return seenText(root.data.slice(range.startOffset, range.endOffset));
  // Text read so far, and a break waiting for the text after it.
  let out = '';
  let broken = false;
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT);
  for (let n = walker.nextNode(); n; n = walker.nextNode()) {
    if (!range.intersectsNode(n)) continue;
    if (n instanceof Text) {
      const from = n === range.startContainer ? range.startOffset : 0;
      const to = n === range.endContainer ? range.endOffset : n.data.length;
      const text = seenText(n.data.slice(from, to));
      if (!text) continue;
      if (broken) out += lineBreak(out, text);
      broken = false;
      out += text;
    } else if ((n as Element).localName === 'br') {
      broken = true;
    }
  }
  // A break with nothing after it still ends the line.
  return broken ? `${out} ` : out;
}

/** `range` kept to one layer's words: a selection running onto the next page is its first page's. */
function within(range: Range, layer: Element): Range {
  const all = document.createRange();
  all.selectNodeContents(layer);
  const r = range.cloneRange();
  if (r.compareBoundaryPoints(Range.START_TO_START, all) < 0) r.setStart(all.startContainer, all.startOffset);
  if (r.compareBoundaryPoints(Range.END_TO_END, all) > 0) r.setEnd(all.endContainer, all.endOffset);
  return r;
}

type PagePlace = NonNullable<TextAnchor['rect']>;

/**
 * Where `range` sits on its page, as fractions of the page's text layer,
 * which covers the page at any zoom: a page of the same line over and over
 * tells its lines apart by this alone. Null for a layer not laid out.
 */
export function placeOn(range: Range, layer: Element): PagePlace | null {
  // A document with no layout (a test's) has no box for a range.
  if (typeof range.getBoundingClientRect !== 'function') return null;
  const page = layer.getBoundingClientRect();
  const r = range.getBoundingClientRect();
  if (!page.width || !page.height || (!r.width && !r.height)) return null;
  const x = placeFraction((r.left - page.left) / page.width);
  const y = placeFraction((r.top - page.top) / page.height);
  return { x, y, w: placeFraction(Math.min(r.width / page.width, 1 - x)), h: placeFraction(Math.min(r.height / page.height, 1 - y)) };
}

/** How far a place on the page is from where words sat: their top-left corners apart, as fractions of the page. */
function awayFrom(was: PagePlace, layer: Element): Nearness {
  return ({ range }) => {
    const now = placeOn(range(), layer);
    return now ? Math.hypot(now.x - was.x, now.y - was.y) : Infinity;
  };
}

export function pdfSpots(root: Element): SpotStrategy {
  // Each text layer read once in a pass of finds, however many comments are on its page.
  const sources = new Map<Element, TextSource>();
  const sourceOf = (layer: Element): TextSource => {
    let source = sources.get(layer);
    if (!source) {
      source = textSource(layer, { reading: SEEN });
      sources.set(layer, source);
    }
    return source;
  };

  const findWords = (anchor: TextAnchor): Found | null => {
    if (!anchor.quote || !anchor.page) return null;
    // No page laid out yet: the document is loading, or loading a newer copy.
    if (!root.querySelector(PAGE)) return { parked: root };
    const slot = root.querySelector(`.pdf-page[data-pdf-slot="${anchor.page - 1}"]`);
    if (!slot) return null;
    const layer = slot.querySelector(LAYER);
    if (!drawn(layer)) return { parked: slot };
    const range = findQuote(sourceOf(layer), anchor, anchor.rect && awayFrom(anchor.rect, layer));
    return range && { range };
  };

  return {
    // Words across pages are a comment on the page they start on: its page, and the words, box and mark there.
    clip(range) {
      const on = pageOf(range.startContainer);
      return on && root.contains(on.layer) ? within(range, on.layer) : range;
    },

    words(range) {
      const on = pageOf(range.startContainer);
      if (!on || !root.contains(on.layer)) return null;
      const kept = within(range, on.layer);
      const quote = squash(wordsIn(kept));
      if (!quote) return null;
      const rect = placeOn(kept, on.layer);
      return {
        kind: 'text',
        page: on.page,
        quote: clip(quote, MAX_SELECTION),
        ...wordsAround(kept, on.layer, on.layer, wordsIn),
        ...(rect ? { rect } : {}),
      };
    },

    find(anchor) {
      return anchor.kind === 'text' ? findWords(anchor) : null;
    },

    // A layer's text may change between passes, as pdf.js lays it out.
    fresh() {
      sources.clear();
    },
    settled() {
      sources.clear();
    },
  };
}

/** A PDF's pages as surfaces a box is drawn on: each page's slot, which has the page's size before the page is drawn. */
function pdfSurfaces(root: Element): RegionSurfaces {
  return {
    points: false,
    // By the slots' boxes: the point a drag began at may have scrolled out of view, where nothing is under it.
    at(x, y) {
      for (const slot of root.querySelectorAll<HTMLElement>(PAGE)) {
        const r = slot.getBoundingClientRect();
        if (x < r.left || x > r.right || y < r.top || y > r.bottom) continue;
        const index = Number(slot.getAttribute('data-pdf-slot'));
        return Number.isInteger(index) ? { el: slot, page: index + 1 } : null;
      }
      return null;
    },
    of(page) {
      if (!page) return null;
      if (!root.querySelector(PAGE)) return { parked: root };
      const slot = root.querySelector(`.pdf-page[data-pdf-slot="${page - 1}"]`);
      return slot instanceof HTMLElement ? { el: slot, page } : null;
    },
  };
}

/** A PDF's comments: words selected on a page, or boxes drawn on it. */
export function pdfRegions(root: Element): SpotStrategy {
  return regionSpots(pdfSurfaces(root), pdfSpots(root));
}
