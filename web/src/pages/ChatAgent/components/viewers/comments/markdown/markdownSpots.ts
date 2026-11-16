/**
 * Comments on a rendered Markdown file. A spot is a block the renderer marked
 * with its lines in the file (`rehypeSourceLines`): a heading, a paragraph,
 * a list item, a table row, a quote or a code block. A block is saved by its
 * lines and its text, and found again by its text, its lines telling apart
 * blocks that read the same: an edit above it moves its lines, not its words.
 */
import {
  MAX_LABEL, MAX_SELECTION, clip, laidOutBox, linesAnchor, linesText, nearestByQuote, normal, overlaps, parseLines, quoteFinder,
  squash, wordsAround,
} from '../spotKit';
import type { Edges, Found, SpotStrategy } from '../spotStrategy';
import { SOURCE_LINES_ATTR } from './sourceLines';

const BLOCK = `[${SOURCE_LINES_ATTR}]`;
const HEADINGS = 'h1,h2,h3,h4,h5,h6';
/** Text the reader does not see: KaTeX's MathML copy of each formula. */
const UNSEEN = '.katex-mathml';

function blockOf(node: Node): Element | null {
  return (node instanceof Element ? node : node.parentElement)?.closest(BLOCK) ?? null;
}

/** What `el` reads, without a formula's hidden MathML copy. */
function seenText(el: Node): string {
  if (!(el instanceof Element) || !el.querySelector(UNSEEN)) return el.textContent ?? '';
  const copy = el.cloneNode(true) as Element;
  for (const unseen of copy.querySelectorAll(UNSEEN)) unseen.remove();
  return copy.textContent ?? '';
}

/** A row's cells, as the agent reads the row: its source's cells, apart. */
function cellsOf(row: Element): string[] {
  return [...row.children].map((cell) => squash(seenText(cell)));
}

/** A block's text as the comment quotes it: a row by its cells, an item without the items under it, code without its chrome. */
export function blockText(block: Element): string {
  if (block.localName === 'tr') return cellsOf(block).join(' | ');
  if (block.localName === 'li') {
    const own = [...block.childNodes].filter((n) => !(n instanceof Element && (n.localName === 'ul' || n.localName === 'ol')));
    return squash(own.map(seenText).join(' '));
  }
  // A code block is drawn in a wrapper with its language and copy button; its words are the code's.
  if (block.localName === 'pre' || block.localName === 'div') return squash((block.querySelector('code') ?? block).textContent);
  return squash(seenText(block));
}

/** What a range holds as the reader sees it, without a formula's hidden MathML copy. */
function shownText(range: Range): string {
  const copy = range.cloneContents();
  for (const unseen of copy.querySelectorAll(UNSEEN)) unseen.remove();
  return copy.textContent ?? '';
}

/** Whether `spot` is in a list under the item `part`, whose words leave it out. */
function underItem(part: Element, spot: Element): boolean {
  const list = spot.closest('ul, ol');
  return part.localName === 'li' && !!list && part.contains(list);
}

/** Whether `box` touches an item's own words, not those of the items under it. */
function ownWordsIn(item: Element, box: Edges): boolean {
  if (item.localName !== 'li') return false;
  return [...item.childNodes].some((node) => {
    if (node instanceof Element && (node.localName === 'ul' || node.localName === 'ol')) return false;
    const range = document.createRange();
    range.selectNodeContents(node);
    return [...range.getClientRects()].some((r) => overlaps(r, box));
  });
}

export function markdownSpots(root: Element): SpotStrategy {
  const blocks = () => [...root.querySelectorAll(BLOCK)];
  const startOf = (el: Element | null) => parseLines(el?.getAttribute(SOURCE_LINES_ATTR))?.[0] ?? 0;

  /** The heading nearest above `el`: a block is named by the section it is in, a heading by itself. */
  const headingAbove = (el: Element): string | null => {
    if (/^h[1-6]$/.test(el.localName)) return squash(el.textContent) || null;
    let found: Element | null = null;
    for (const h of root.querySelectorAll(HEADINGS)) {
      if (el.compareDocumentPosition(h) & Node.DOCUMENT_POSITION_PRECEDING) found = h;
      else break;
    }
    return found ? squash(found.textContent) || null : null;
  };

  const labelOf = (spot: Element): string | null => {
    const row = spot.closest('tr');
    const label = row && root.contains(row) ? (cellsOf(row)[0] ?? '') : headingAbove(spot);
    return label ? clip(label, MAX_LABEL) : null;
  };

  /**
   * The block a stored one is now (`nearestByQuote`), its lines telling
   * apart blocks that read the same. A block with no words is found by its
   * lines alone.
   */
  // The blocks and their words, read once a pass of lookups (`fresh`), as an area looks for each of its parts.
  let read: Element[] | null = null;
  const words = new Map<Element, string>();
  const quotes = quoteFinder(root, UNSEEN);
  const findBlock = (stored: { lines?: string; quote?: string }): Element | null => {
    const all = (read ??= blocks());
    if (!stored.quote) return all.find((el) => el.getAttribute(SOURCE_LINES_ATTR) === stored.lines) ?? null;
    return nearestByQuote(all, { lines: stored.lines, quote: stored.quote }, startOf, (el) => {
      let text = words.get(el);
      if (text === undefined) words.set(el, (text = normal(blockText(el))));
      return [text, 0, text.length];
    });
  };

  return {
    spots: {
      spotAt(target) {
        const block = target.closest(BLOCK);
        return block && root.contains(block) ? block : null;
      },

      // The innermost blocks it touches: a quote's paragraphs rather than the
      // quote around them. An item whose own words it touches stays beside
      // the items under it, which its words leave out (`blockText`).
      spotsIn(box) {
        const touched = blocks().filter((el) => overlaps(el.getBoundingClientRect(), box));
        return touched.filter((el) => !touched.some((other) => other !== el && el.contains(other)) || ownWordsIn(el, box));
      },

      holds: (part, spot) => part === spot || (part.contains(spot) && !underItem(part, spot)),

      labelOf,

      anchorOf(spots) {
        return linesAnchor(spots, (spot) => ({ lines: spot.getAttribute(SOURCE_LINES_ATTR) ?? '', label: labelOf(spot), quote: blockText(spot) }));
      },

      rectsOf: laidOutBox,
    },

    words(range) {
      const quote = squash(shownText(range));
      const start = blockOf(range.startContainer);
      const end = blockOf(range.endContainer) ?? start;
      if (!quote || !start || !root.contains(start)) return null;
      const first = parseLines(start.getAttribute(SOURCE_LINES_ATTR));
      const last = end ? parseLines(end.getAttribute(SOURCE_LINES_ATTR)) : null;
      const label = labelOf(start);
      return {
        kind: 'text',
        quote: clip(quote, MAX_SELECTION),
        ...(first ? { lines: linesText(first[0], Math.max(first[1], last?.[1] ?? first[1])) } : {}),
        ...(label ? { label } : {}),
        ...wordsAround(range, start === end ? start : root, start === end ? start : root, shownText),
      };
    },

    fresh() {
      read = null;
      words.clear();
      quotes.fresh();
    },

    find(anchor): Found | null {
      if (anchor.kind === 'block') {
        const spot = findBlock(anchor);
        return spot ? { parts: [[spot]] } : null;
      }
      if (anchor.kind === 'area') {
        const parts = anchor.elements.map((e) => {
          const spot = e.lines ? findBlock(e) : null;
          return spot && [spot];
        });
        return parts.some(Boolean) ? { parts } : null;
      }
      if (anchor.kind === 'text') {
        const range = quotes.find(anchor, (node) => startOf(blockOf(node)));
        return range ? { range } : null;
      }
      return null;
    },
  };
}

/** The file's title, which a comment keeps: its first top heading. */
export function markdownTitle(root: Element): string | null {
  const h1 = root.querySelector('h1');
  return h1 ? squash(h1.textContent) || null : null;
}
