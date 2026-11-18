/**
 * Comments on a PDF's words. A page draws its words in a text layer over its
 * canvas, and only pages near the viewport are drawn at all, so a comment is
 * saved by its page and its words, and found again on that page once its
 * text layer is drawn; until then it waits in the page's slot (`parked`).
 * A drag off the words draws a box on the page instead (`pdfRegions`).
 */
import type { TextAnchor } from '@/types/artifacts';
import { MAX_SELECTION, clip, findQuote, squash, wordsAround } from '../spotKit';
import type { Found, SpotStrategy } from '../spotStrategy';
import { regionSpots, type RegionSurfaces } from '../region/regionSpots';

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
 * The words in a range as a reader reads them. pdf.js ends a line with a
 * `<br>` rather than a space, which `Range.toString` drops, so two lines'
 * words would run together in what the agent reads.
 */
export function wordsIn(range: Range): string {
  const root = range.commonAncestorContainer;
  if (root instanceof Text) return root.data.slice(range.startOffset, range.endOffset);
  const out: string[] = [];
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT | NodeFilter.SHOW_ELEMENT);
  for (let n = walker.nextNode(); n; n = walker.nextNode()) {
    if (!range.intersectsNode(n)) continue;
    if (n instanceof Text) {
      const from = n === range.startContainer ? range.startOffset : 0;
      const to = n === range.endContainer ? range.endOffset : n.data.length;
      out.push(n.data.slice(from, to));
    } else if ((n as Element).localName === 'br') {
      out.push(' ');
    }
  }
  return out.join('');
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

export function pdfSpots(root: Element): SpotStrategy {
  const findWords = (anchor: TextAnchor): Found | null => {
    if (!anchor.quote || !anchor.page) return null;
    // No page laid out yet: the document is loading, or loading a newer copy.
    if (!root.querySelector(PAGE)) return { parked: root };
    const slot = root.querySelector(`.pdf-page[data-pdf-slot="${anchor.page - 1}"]`);
    if (!slot) return null;
    const layer = slot.querySelector(LAYER);
    if (!drawn(layer)) return { parked: slot };
    const range = findQuote(layer, anchor);
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
      return { kind: 'text', page: on.page, quote: clip(quote, MAX_SELECTION), ...wordsAround(kept, on.layer, on.layer, wordsIn) };
    },

    find(anchor) {
      return anchor.kind === 'text' ? findWords(anchor) : null;
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
