/**
 * What the viewers' strategies share: text read and quotes found as a framed
 * page's bridge reads and finds them (`pageText.ts`, which both run), and
 * parts saved by their lines in the file, as a Markdown block and a run of
 * code are.
 */
import type { AreaAnchor, BlockAnchor, TextAnchor } from '@/types/artifacts';
import { MAX_AREA_COUNT, MAX_AREA_ELEMENTS } from '../html/pageProtocol';
import { type Nearness, type Reading, type TextIndex, clip, findQuoteIn, indexText, keptHead, normal, searchedQuote, sharedHead, sharedTail } from './pageText';
import type { Edges } from './spotStrategy';

export { clip, normal, squash, trimmed, wholeWords, wordsAround, type Nearness } from './pageText';

/** The comment API's limits on what an anchor quotes, or tighter where a shorter value reads better. */
export const MAX_LABEL = 120;
export const MAX_QUOTE = 300;
export const MAX_SELECTION = 2000;
const MAX_AREA_QUOTE = 160;

// --- Text ---

/** Cut to `limit` at a word, the ellipsis included; text with no space late enough is cut where it reaches it. */
export function clipWords(text: string, limit: number): string {
  if (text.length <= limit) return text;
  const room = limit - 1;
  let cut = keptHead(text, room);
  if (text.charAt(room) !== ' ') {
    const space = cut.lastIndexOf(' ');
    if (space >= limit / 3) cut = cut.slice(0, space);
  }
  return `${cut.replace(/[\s,.;:-]+$/, '') || cut}…`;
}

/** A stored quote up to where it was cut for storage. */
function quoteHead(quote: string): string {
  return quote.endsWith('…') ? quote.slice(0, -1) : quote;
}

// --- Finding words again ---

/**
 * The text quotes are looked for in, and how it reads: `reading` applies to
 * the quotes as to the text, so both sides of a comparison read alike. The
 * text is indexed at the first lookup and kept until `fresh`, so a strategy
 * that looks for many anchors a pass reads it once.
 */
export interface TextSource {
  /** How the text reads, which applies to the quotes looked for in it too. */
  reading?: Reading;
  index: () => TextIndex;
  fresh: () => void;
}

/** `root`'s text as `normal` reads it, leaving out what `skip` matches (text the reader does not see). */
export function textSource(root: Element, { skip, reading }: { skip?: string; reading?: Reading } = {}): TextSource {
  let index: TextIndex | null = null;
  const keep = (node: Text) => !skip || !node.parentElement?.closest(skip);
  return {
    reading,
    index: () => (index ??= indexText(root, keep, reading)),
    fresh: () => {
      index = null;
    },
  };
}

/** How far a place's lines are from the anchor's old first line, `lineOf` reading a text node's line. */
export function nearLines(anchor: TextAnchor, lineOf: (node: Text) => number): Nearness {
  const was = parseLines(anchor.lines)?.[0] ?? 0;
  return ({ node }) => Math.abs(lineOf(node) - was);
}

/**
 * Where a text anchor's words are in `source`: of the places they occur, the
 * one the stored words around fit best, a tie going to the nearest by
 * `nearness` (`findQuoteIn`).
 */
export function findQuote(source: TextSource, anchor: TextAnchor, nearness?: Nearness): Range | null {
  if (!anchor.quote) return null;
  return findQuoteIn(source.index, searchedQuote(anchor.quote, MAX_SELECTION), anchor.prefix, anchor.suffix, { reading: source.reading, nearness });
}

// --- Parts saved by their lines ---

/** Lines in the file as an anchor writes them, "7" or "14-15", 1-based. */
export function linesText(first: number, last: number): string {
  return first >= last ? String(first) : `${first}-${last}`;
}

/** The first and last line `value` names, or null for anything else. */
export function parseLines(value: string | null | undefined): [number, number] | null {
  const m = value ? /^(\d+)(?:-(\d+))?$/.exec(value) : null;
  if (!m) return null;
  const first = Number(m[1]);
  return [first, m[2] ? Number(m[2]) : first];
}

/** A part as an anchor saves it by its lines: where it is, what names it, and its words. */
export interface LinesPart {
  lines: string;
  label: string | null;
  quote: string;
}

/**
 * The anchor of parts saved by their lines: one is a block, quoted up to
 * the API's limit; several are an area, each quoted shorter at a word, up
 * to the API's limit listed and the rest counted.
 */
export function linesAnchor<T>(groups: readonly T[], read: (group: T) => LinesPart): BlockAnchor | AreaAnchor | null {
  const part = (group: T, cut: (quote: string) => string) => {
    const { lines, label, quote } = read(group);
    return { lines, ...(label ? { label } : {}), ...(quote ? { quote: cut(quote) } : {}) };
  };
  if (groups.length === 1) {
    const one = part(groups[0], (q) => clip(q, MAX_QUOTE));
    return one.lines ? { kind: 'block', ...one } : null;
  }
  if (!groups.length) return null;
  const elements = groups.slice(0, MAX_AREA_ELEMENTS).map((g) => part(g, (q) => clipWords(q, MAX_AREA_QUOTE)));
  const labels = [...new Set(elements.flatMap((e) => (e.label ? [e.label] : [])))];
  const label = clipWords(labels.join(', '), MAX_LABEL);
  return { kind: 'area', elements, count: Math.min(groups.length, MAX_AREA_COUNT), ...(label ? { label } : {}) };
}

/**
 * The candidate a stored part is now: of those that read as its quote, the
 * one nearest its old lines; failing any, one that keeps the quote's opening
 * or closing words, an edit of it. A candidate's words are `text` from
 * `from` to `to` as `normal` reads them, so a run of lines is read in place.
 */
export function nearestByQuote<T>(
  candidates: readonly T[],
  stored: { lines?: string; quote: string },
  lineOf: (candidate: T) => number,
  wordsOf: (candidate: T) => [text: string, from: number, to: number],
): T | null {
  const was = parseLines(stored.lines)?.[0] ?? 0;
  const whole = !stored.quote.endsWith('…');
  const q = normal(quoteHead(stored.quote));
  const need = Math.min(8, Math.ceil(q.length / 2));
  const nearer = (c: T, best: T | null) => best === null || Math.abs(lineOf(c) - was) < Math.abs(lineOf(best) - was);
  let same: T | null = null;
  let edited: T | null = null;
  for (const c of candidates) {
    const [text, from, to] = wordsOf(c);
    const length = to - from;
    if ((whole ? length === q.length : length >= q.length) && text.startsWith(q, from)) {
      if (nearer(c, same)) same = c;
    } else if (same === null && (sharedHead(text, from, to, q) >= need || (whole && sharedTail(text, from, to, q) >= need)) && nearer(c, edited)) {
      edited = c;
    }
  }
  return same ?? edited;
}

// --- Boxes ---

export function overlaps(r: DOMRect, box: Edges): boolean {
  return r.left < box.right && r.right > box.left && r.top < box.bottom && r.bottom > box.top;
}

/** A spot's own box, while it is laid out. */
export function laidOutBox(spot: Element): DOMRect[] {
  return spot.getClientRects().length ? [spot.getBoundingClientRect()] : [];
}

// --- Marks laid over the content ---

/** Marks what the page lays over the content itself, which a change to the content is not. */
export const OWN_ATTR = 'data-comment-own';
/** The layer the marks sit in, and each mark: the same rules let a picture's boxes and a chart's marks take clicks. */
const LAYER_CLASS = 'comment-regions';
const MARK_CLASS = 'comment-region';
/** How large a point is outlined and pinned, in pixels. */
const POINT_PX = 14;

/** Whether a change the content observer saw is only the page's own marks being laid or moved. */
export function ownChange(record: MutationRecord): boolean {
  const own = (n: Node) => n instanceof Element && !!n.closest(`[${OWN_ATTR}]`);
  if (record.type !== 'childList') return own(record.target);
  const nodes = [...record.addedNodes, ...record.removedNodes];
  return nodes.length > 0 && nodes.every((n) => n instanceof Element && n.hasAttribute(OWN_ATTR));
}

/** Where a mark sits in the layer, as CSS lengths; a point's width and height are dropped. */
export interface MarkPlace {
  left: string;
  top: string;
  width: string;
  height: string;
}

/** The value a mark holds in `data-<field>`, read off the element that marks it. */
export function markValue<V>(spot: Element, field: string): V | null {
  const raw = spot instanceof HTMLElement ? spot.dataset[field] : undefined;
  if (!raw) return null;
  try {
    return JSON.parse(raw) as V;
  } catch {
    return null;
  }
}

/**
 * The elements marking a viewer's drawn spots (boxes on a picture, marks on a
 * chart), each holding its value as JSON in `data-<field>`. They are laid as
 * anchors are found and spots drawn, and the end of a pass of finds lets go
 * of the ones it did not name. Only its own are picked: the content can
 * outlive the page that laid marks on it (a chart kept while another is
 * shown), and another page's marks hold another document's values.
 */
export function laidMarks<V>({ field, keyOf, isPoint }: {
  field: string;
  keyOf: (value: V) => string;
  isPoint: (value: V) => boolean;
}) {
  const laid = new Map<string, { el: HTMLElement; pass: number }>();
  const own = new WeakSet<Element>();
  let pass = 0;

  const layerOf = (host: Element): Element => {
    const found = [...host.children].find((c) => c.classList.contains(LAYER_CLASS));
    if (found) return found;
    const layer = document.createElement('div');
    layer.className = LAYER_CLASS;
    layer.setAttribute(OWN_ATTR, '');
    host.appendChild(layer);
    return layer;
  };

  return {
    /** The element marking `value` at `place` in `host`, laid there if it is not; kept through this pass and `keepFor` more. */
    lay(host: Element, value: V, place: MarkPlace, keepFor = 0): HTMLElement {
      const key = keyOf(value);
      const had = laid.get(key);
      const point = isPoint(value);
      let el = had?.el;
      if (!el || !host.contains(el)) {
        el?.remove();
        el = document.createElement('div');
        el.className = MARK_CLASS;
        el.setAttribute(OWN_ATTR, '');
        el.dataset[field] = JSON.stringify(value);
        el.dataset.point = point ? 'true' : 'false';
        own.add(el);
        layerOf(host).appendChild(el);
      }
      Object.assign(el.style, { ...place, ...(point ? { width: '0', height: '0' } : {}) });
      laid.set(key, { el, pass: Math.max(pass + keepFor, had?.pass ?? 0) });
      return el;
    },
    /** The mark of these a click on `target` is on, if any. */
    spotAt(target: Element): Element | null {
      const spot = target.closest(`.${MARK_CLASS}`);
      return spot && own.has(spot) ? spot : null;
    },
    valueOf: (spot: Element): V | null => markValue<V>(spot, field),
    rectsOf(spot: Element): DOMRect[] {
      if (!spot.isConnected) return [];
      const r = spot.getBoundingClientRect();
      if (spot instanceof HTMLElement && spot.dataset.point === 'true') {
        // About its centre: while the add key is held a point takes clicks around it, and its box grows.
        const half = POINT_PX / 2;
        return [new DOMRect(r.left + r.width / 2 - half, r.top + r.height / 2 - half, POINT_PX, POINT_PX)];
      }
      return [r];
    },
    fresh() {
      pass += 1;
    },
    settled() {
      for (const [key, { el, pass: kept }] of laid) {
        if (kept >= pass) continue;
        el.remove();
        laid.delete(key);
      }
    },
    /** Takes every mark out: the page that laid them is gone. */
    dispose() {
      for (const { el } of laid.values()) el.remove();
      laid.clear();
    },
  };
}
