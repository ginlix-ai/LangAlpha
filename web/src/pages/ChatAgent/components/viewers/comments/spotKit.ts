/**
 * What the viewers' strategies share: text read as the page's bridge reads
 * it (`comments-anchors.js`), so a quote is found the same way in either,
 * and parts saved by their lines in the file, as a Markdown block and a run
 * of code are.
 */
import type { AreaAnchor, BlockAnchor, TextAnchor } from '@/types/artifacts';
import { MAX_AREA_COUNT, MAX_AREA_ELEMENTS } from '../html/pageProtocol';
import type { Edges } from './spotStrategy';

/** The comment API's limits on what an anchor quotes, or tighter where a shorter value reads better. */
export const MAX_LABEL = 120;
export const MAX_QUOTE = 300;
export const MAX_SELECTION = 2000;
const MAX_AREA_QUOTE = 160;
const MAX_AROUND = 60;

// --- Text ---

export function squash(text: unknown): string {
  return String(text ?? '').replace(/\s+/g, ' ').trim();
}

/**
 * A space as the page script counts one: every space `\s` matches, as
 * `squash` turns each into one when a quote is saved, and zero-width ones,
 * so a quote and the text it is looked for in drop the same characters.
 */
function isSpace(code: number): boolean {
  return code <= 32 || code === 160 || code === 0x1680 || code === 0x2028 || code === 0x2029 || code === 0x202f ||
    code === 0x205f || code === 0x3000 || code === 0xfeff || (code >= 0x2000 && code <= 0x200b);
}

/** Never ends on half a surrogate pair, which the server cannot store alone. */
function keptHead(text: string, length: number): string {
  const last = text.charCodeAt(length - 1);
  return text.slice(0, last >= 0xd800 && last <= 0xdbff ? length - 1 : length);
}

function keptTail(text: string, length: number): string {
  const from = Math.max(0, text.length - length);
  const first = text.charCodeAt(from);
  return text.slice(first >= 0xdc00 && first <= 0xdfff ? from + 1 : from);
}

function headOf(text: string, length: number): string {
  return text.length > length ? keptHead(text, length) : text;
}

/** Cut to `limit`, the ellipsis included. */
export function clip(text: string, limit: number): string {
  return text.length > limit ? `${keptHead(text, limit - 1)}…` : text;
}

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

/**
 * One character in lower case, as quotes and the text they are looked for in
 * both fold it: one at a time, so a final sigma or a dotted I folds the same
 * in a quote as on the page, and kept as it is where it would grow.
 */
function fold(ch: string): string {
  const lower = ch.toLowerCase();
  return lower.length === 1 ? lower : ch;
}

/** Lower case with every space dropped, as quotes are compared: the rendered text spaces what the source does not. */
export function normal(text: unknown): string {
  const s = String(text ?? '');
  let out = '';
  for (let i = 0; i < s.length; i++) if (!isSpace(s.charCodeAt(i))) out += fold(s.charAt(i));
  return out;
}

/** A stored quote up to where it was cut for storage. */
function quoteHead(quote: string): string {
  return quote.endsWith('…') ? quote.slice(0, -1) : quote;
}

/** How many characters `q` shares with the start of `text` between `from` and `to`. */
function sharedHead(text: string, from: number, to: number, q: string): number {
  let n = 0;
  while (from + n < to && n < q.length && text.charAt(from + n) === q.charAt(n)) n++;
  return n;
}

/** How many characters `q` shares with the end of `text` between `from` and `to`. */
function sharedTail(text: string, from: number, to: number, q: string): number {
  let n = 0;
  while (to - 1 - n >= from && n < q.length && text.charAt(to - 1 - n) === q.charAt(q.length - 1 - n)) n++;
  return n;
}

// Letters of a word in scripts that space their words; CJK is left out,
// since each of its characters already stands alone.
const WORD = /[0-9A-Za-z_'’À-ɏͰ-ϿЀ-ӿ]/;
const SNAP = 40;

function isWord(ch: string): boolean {
  return !!ch && WORD.test(ch);
}

/**
 * The range grown to whole words where it starts or ends inside one, so a
 * quote never opens on a fragment. A copy: the reader's selection stays.
 */
export function wholeWords(live: Range): Range {
  const range = live.cloneRange();
  let node = range.startContainer;
  let at = range.startOffset;
  if (node instanceof Text && isWord(node.data.charAt(at - 1)) && isWord(node.data.charAt(at))) {
    const first = Math.max(0, at - SNAP);
    while (at > first && isWord(node.data.charAt(at - 1))) at--;
    range.setStart(node, at);
  }
  node = range.endContainer;
  at = range.endOffset;
  if (node instanceof Text && isWord(node.data.charAt(at - 1)) && isWord(node.data.charAt(at))) {
    const last = Math.min(node.data.length, at + SNAP);
    while (at < last && isWord(node.data.charAt(at))) at++;
    range.setEnd(node, at);
  }
  return range;
}

/** The range without the spaces at either end, as the anchor finds it again, so the mark drawn at once is the one that stays. */
export function trimmed(range: Range): Range {
  const r = range.cloneRange();
  if (r.startContainer instanceof Text) {
    const data = r.startContainer.data;
    let at = r.startOffset;
    while (at < data.length && isSpace(data.charCodeAt(at))) at++;
    r.setStart(r.startContainer, at);
  }
  if (r.endContainer instanceof Text) {
    const data = r.endContainer.data;
    let at = r.endOffset;
    while (at > 0 && isSpace(data.charCodeAt(at - 1))) at--;
    if (r.endContainer !== r.startContainer || at > r.startOffset) r.setEnd(r.endContainer, at);
  }
  return r;
}

/**
 * The words before and after a range, from the start of `start` to the end
 * of `end`, which find it again among the same words elsewhere. `read` reads
 * a range as the viewer shows it.
 */
export function wordsAround(
  range: Range, start: Node, end: Node = start, read: (r: Range) => string = (r) => r.toString(),
): { prefix?: string; suffix?: string } {
  try {
    const before = document.createRange();
    before.setStart(start, 0);
    before.setEnd(range.startContainer, range.startOffset);
    const after = document.createRange();
    after.setStart(range.endContainer, range.endOffset);
    after.setEnd(end, end.childNodes.length);
    const prefix = keptTail(squash(read(before)), MAX_AROUND);
    const suffix = headOf(squash(read(after)), MAX_AROUND);
    return { ...(prefix ? { prefix } : {}), ...(suffix ? { suffix } : {}) };
  } catch {
    // The selection reached outside them; its words alone still find it.
    return {};
  }
}

// --- Finding words again ---

/** The content's text without its spaces, each character mapped back to the text node and offset it came from. */
interface TextIndex {
  text: string;
  nodes: Text[];
  nodeAt: number[];
  offsetAt: number[];
}

/** `root`'s text as `normal` reads it, leaving out what `skip` matches (text the reader does not see). */
function textIndex(root: Element, skip?: string): TextIndex {
  const chars: string[] = [];
  const nodes: Text[] = [];
  const nodeAt: number[] = [];
  const offsetAt: number[] = [];
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode() as Text | null; node; node = walker.nextNode() as Text | null) {
    if (skip && node.parentElement?.closest(skip)) continue;
    const at = nodes.length;
    nodes.push(node);
    const data = node.data;
    for (let i = 0; i < data.length; i++) {
      if (isSpace(data.charCodeAt(i))) continue;
      chars.push(fold(data.charAt(i)));
      nodeAt.push(at);
      offsetAt.push(i);
    }
  }
  return { text: chars.join(''), nodes, nodeAt, offsetAt };
}

/**
 * Where a text anchor's words are in `root`: of the places they occur, the
 * one the stored words around fit best, a tie going to the one nearest the
 * anchor's old lines where `lineOf` reads a text node's line.
 */
export function findQuote(
  root: Element,
  anchor: TextAnchor,
  { skip, lineOf, index: read }: { skip?: string; lineOf?: (node: Text) => number; index?: () => TextIndex } = {},
): Range | null {
  if (!anchor.quote) return null;
  const q = normal(anchor.quote.length >= MAX_SELECTION - 1 ? quoteHead(anchor.quote) : anchor.quote);
  if (!q) return null;
  const index = read ? read() : textIndex(root, skip);
  const p = normal(anchor.prefix);
  const s = normal(anchor.suffix);
  const was = parseLines(anchor.lines)?.[0] ?? 0;
  const off = (at: number) => (lineOf ? Math.abs(lineOf(index.nodes[index.nodeAt[at]]) - was) : 0);
  let best: { at: number; score: number } | null = null;
  // Where the old lines are known, every place is weighed, the one they name
  // among them; elsewhere the first 200, which the words around tell apart.
  const most = lineOf ? Infinity : 200;
  for (let at = index.text.indexOf(q), tries = 0; at !== -1 && tries < most; at = index.text.indexOf(q, at + 1), tries++) {
    const score = sharedTail(index.text, Math.max(0, at - p.length), at, p) + sharedHead(index.text, at + q.length, at + q.length + s.length, s);
    if (!best || score > best.score || (score === best.score && off(at) < off(best.at))) best = { at, score };
  }
  if (!best) return null;
  const last = best.at + q.length - 1;
  const range = document.createRange();
  range.setStart(index.nodes[index.nodeAt[best.at]], index.offsetAt[best.at]);
  range.setEnd(index.nodes[index.nodeAt[last]], index.offsetAt[last] + 1);
  return range;
}

/** `findQuote` in `root`, its text read once until `fresh`, for a strategy that looks for many anchors a pass. */
export function quoteFinder(root: Element, skip: string) {
  let index: TextIndex | null = null;
  const read = () => (index ??= textIndex(root, skip));
  return {
    find: (anchor: TextAnchor, lineOf: (node: Text) => number) => findQuote(root, anchor, { skip, lineOf, index: read }),
    fresh: () => {
      index = null;
    },
  };
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
