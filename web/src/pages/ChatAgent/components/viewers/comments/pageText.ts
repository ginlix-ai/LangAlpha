/**
 * How a comment's quoted words are found again in a document's text: the
 * same code in the app's viewers (`spotKit.ts`) and in a framed page, whose
 * bridge part `comments-shared.generated.js` is compiled from this file by
 * `src/server/preview_proxy/bridge/generate-shared.mjs`. Hence its shape: no
 * imports, and only syntax that compiles to the bridge's ES5 without helpers
 * (no spread, optional chaining, `??` or `for...of`). Every top-level name is
 * a name in the bridge's one scope too.
 */

/**
 * A space as `\s` counts one, as a quote is squashed when saved, and the
 * zero-width ones, so a quote and the text it is looked for in drop the same
 * characters.
 */
export function isSpace(code: number): boolean {
  return code <= 32 || code === 160 || code === 0x1680 || code === 0x2028 || code === 0x2029 || code === 0x202f ||
    code === 0x205f || code === 0x3000 || code === 0xfeff || (code >= 0x2000 && code <= 0x200b);
}

/** Every run of spaces as one, none at either end: a quote as it is saved. */
export function squash(text: unknown): string {
  return String(text === undefined || text === null ? '' : text).replace(/\s+/g, ' ').trim();
}

/** The first `length` characters, never ending on half a surrogate pair, which the server cannot store alone. */
export function keptHead(text: string, length: number): string {
  const last = text.charCodeAt(length - 1);
  return text.slice(0, last >= 0xd800 && last <= 0xdbff ? length - 1 : length);
}

/** The last `length` characters, never starting on half a surrogate pair. */
export function keptTail(text: string, length: number): string {
  const from = Math.max(0, text.length - length);
  const first = text.charCodeAt(from);
  return text.slice(first >= 0xdc00 && first <= 0xdfff ? from + 1 : from);
}

/** Cut to `limit`, the ellipsis included. */
export function clip(text: string, limit: number): string {
  return text.length > limit ? keptHead(text, limit - 1) + '\u2026' : text;
}

/** A fraction of what a spot sits on, kept to 0..1 and four places. */
export function placeFraction(n: number): number {
  return Math.round(Math.min(1, Math.max(0, n)) * 10000) / 10000;
}

// Letters of a word in scripts that space their words; CJK is left out,
// since each of its characters already stands alone.
const WORD = /[0-9A-Za-z_'\u2019\u00c0-\u024f\u0370-\u03ff\u0400-\u04ff]/;
/** The furthest a selection is grown to reach a word's end. */
const SNAP = 40;

function isWord(ch: string): boolean {
  return !!ch && WORD.test(ch);
}

/**
 * The range grown to whole words where it starts or ends inside one, so a
 * quote never opens on a fragment ("arter ended"). A copy: the reader's own
 * selection stays as they made it.
 */
export function wholeWords(live: Range): Range {
  const range = live.cloneRange();
  let node = range.startContainer as Text;
  let at = range.startOffset;
  if (node.nodeType === 3 && isWord(node.data.charAt(at - 1)) && isWord(node.data.charAt(at))) {
    const first = Math.max(0, at - SNAP);
    while (at > first && isWord(node.data.charAt(at - 1))) at--;
    range.setStart(node, at);
  }
  node = range.endContainer as Text;
  at = range.endOffset;
  if (node.nodeType === 3 && isWord(node.data.charAt(at - 1)) && isWord(node.data.charAt(at))) {
    const last = Math.min(node.data.length, at + SNAP);
    while (at < last && isWord(node.data.charAt(at))) at++;
    range.setEnd(node, at);
  }
  return range;
}

/** The range without the spaces at either end, as the anchor finds it again, so the mark drawn at once is the one that stays. */
export function trimmed(range: Range): Range {
  const r = range.cloneRange();
  let node = r.startContainer as Text;
  let at = r.startOffset;
  if (node.nodeType === 3) {
    while (at < node.data.length && isSpace(node.data.charCodeAt(at))) at++;
    r.setStart(node, at);
  }
  node = r.endContainer as Text;
  at = r.endOffset;
  if (node.nodeType === 3) {
    while (at > 0 && isSpace(node.data.charCodeAt(at - 1))) at--;
    if (node !== r.startContainer || at > r.startOffset) r.setEnd(node, at);
  }
  return r;
}

/** Whether two ranges share any of their words; ranges in different trees share none. */
export function rangesOverlap(a: Range, b: Range): boolean {
  try {
    return b.comparePoint(a.endContainer, a.endOffset) >= 0 && b.comparePoint(a.startContainer, a.startOffset) <= 0;
  } catch {
    return false;
  }
}

/** How much of the words before and after a quote is kept to find it again. */
const MAX_AROUND = 60;

/**
 * The words before and after a range, from the start of `start` to the end
 * of `end`, which find it again among the same words elsewhere. `read` reads
 * a range as the viewer shows it.
 */
export function wordsAround(
  range: Range, start: Node, end?: Node, read?: (r: Range) => string,
): { prefix?: string; suffix?: string } {
  const last = end || start;
  const readOf = read || function (r: Range) { return r.toString(); };
  const out: { prefix?: string; suffix?: string } = {};
  try {
    const before = document.createRange();
    before.setStart(start, 0);
    before.setEnd(range.startContainer, range.startOffset);
    const after = document.createRange();
    after.setStart(range.endContainer, range.endOffset);
    after.setEnd(last, last.childNodes.length);
    const prefix = keptTail(squash(readOf(before)), MAX_AROUND);
    const suffix = keptHead(squash(readOf(after)), MAX_AROUND);
    if (prefix) out.prefix = prefix;
    if (suffix) out.suffix = suffix;
  } catch {
    // The selection reached outside them; its words alone still find it.
  }
  return out;
}

/**
 * One character in lower case, as quotes and the text they are looked for in
 * both fold it: one at a time, so a final sigma or a dotted I folds the same
 * in a quote as on the page, and kept as it is where it would grow.
 */
export function fold(ch: string): string {
  const lower = ch.toLowerCase();
  return lower.length === 1 ? lower : ch;
}

/**
 * How a viewer reads its own text, where what it holds is not what the
 * reader sees (a PDF's, `pdf/seenText.ts`). Applied a character at a time to
 * the quote and the content alike, so a quote saved before or after the
 * reading was taken matches the content either way.
 */
export interface Reading {
  (text: string): string;
  /**
   * Whether every character of `text` reads as itself, asked once for a
   * whole text, which then skips the reading: most of a document's text is
   * read as it is held, and a character at a time is the dear way to find so.
   */
  same?: (text: string) => boolean;
}

/** Each character of `s` as `reading` reads it, folded, every space dropped; `each` takes the characters and where they came from. */
export function readChars(s: string, reading: Reading | undefined, each: (ch: string, from: number) => void): void {
  const own = reading && !(reading.same && reading.same(s)) ? reading : undefined;
  for (let i = 0; i < s.length; i++) {
    if (isSpace(s.charCodeAt(i))) continue;
    const read = own ? own(s.charAt(i)) : s.charAt(i);
    for (let j = 0; j < read.length; j++) if (!isSpace(read.charCodeAt(j))) each(fold(read.charAt(j)), i);
  }
}

/**
 * Lower case with every space dropped, as quotes are compared: the rendered
 * text spaces table cells and capitalises with CSS, and the source does
 * neither.
 */
export function normal(text: unknown, reading?: Reading): string {
  let out = '';
  readChars(text === undefined || text === null ? '' : String(text), reading, function (ch) {
    out += ch;
  });
  return out;
}

/** The content's text as `normal` reads it, each character mapped back to the text node and offset it came from. */
export interface TextIndex {
  text: string;
  nodes: Text[];
  nodeAt: number[];
  offsetAt: number[];
}

/** The text under `root` that `keep` takes, as `normal` reads it with `reading`. */
export function indexText(root: Node, keep: (node: Text) => boolean, reading?: Reading): TextIndex {
  const chars: string[] = [];
  const nodes: Text[] = [];
  const nodeAt: number[] = [];
  const offsetAt: number[] = [];
  // 4: NodeFilter.SHOW_TEXT.
  const walker = document.createTreeWalker(root, 4);
  let at = 0;
  const add = function (ch: string, i: number) {
    chars.push(ch);
    nodeAt.push(at);
    offsetAt.push(i);
  };
  for (let node = walker.nextNode() as Text | null; node; node = walker.nextNode() as Text | null) {
    if (!keep(node)) continue;
    at = nodes.length;
    nodes.push(node);
    readChars(node.data, reading, add);
  }
  return { text: chars.join(''), nodes: nodes, nodeAt: nodeAt, offsetAt: offsetAt };
}

/** The range over `length` indexed characters from `at`. */
export function rangeAt(index: TextIndex, at: number, length: number): Range {
  const last = at + length - 1;
  const range = document.createRange();
  range.setStart(index.nodes[index.nodeAt[at]], index.offsetAt[at]);
  range.setEnd(index.nodes[index.nodeAt[last]], index.offsetAt[last] + 1);
  return range;
}

/** How many characters `q` shares with the start of `text` between `from` and `to`. */
export function sharedHead(text: string, from: number, to: number, q: string): number {
  let n = 0;
  while (from + n < to && n < q.length && text.charAt(from + n) === q.charAt(n)) n++;
  return n;
}

/** How many characters `q` shares with the end of `text` between `from` and `to`. */
export function sharedTail(text: string, from: number, to: number, q: string): number {
  let n = 0;
  while (to - 1 - n >= from && n < q.length && text.charAt(to - 1 - n) === q.charAt(q.length - 1 - n)) n++;
  return n;
}

/**
 * The words of a quote to look for: a selection past `cap` was stored cut,
 * with an ellipsis, so only its opening words can be found; a quote that
 * merely ends in an ellipsis is looked for as it stands.
 */
export function searchedQuote(quote: string, cap: number): string {
  return quote.length >= cap - 1 && quote.charAt(quote.length - 1) === '\u2026' ? quote.slice(0, -1) : quote;
}

/** A place a quote occurs: the text node it starts in, and its range, made when asked. */
export interface QuotePlace {
  node: Text;
  range: () => Range;
}

/** How far a place is from where the anchor's words were; lower is nearer. */
export type Nearness = (place: QuotePlace) => number;

/** How a quote is looked for, beyond its words. */
export interface QuoteSearch {
  /** How the text reads, applied to the quote and the words around it alike. */
  reading?: Reading;
  /** Tells apart places the words around tie at; given, every place is weighed. */
  nearness?: Nearness;
  /** Only places that start inside it. */
  within?: Node | null;
  /**
   * The fewest characters a quote may have to be looked for: a framed page,
   * whose text is anyone's, asks two, since one character is found anywhere.
   */
  shortest?: number;
}

/** Without a nearness, the places weighed: the words around tell the first ones apart. */
const QUOTE_TRIES = 200;
/** Of the places the words around tie at, the most a nearness measures: past them, the earlier place stands. */
const QUOTE_MEASURED = 500;

/**
 * Where `quote` is in the indexed text: of the places it occurs, the one the
 * stored words around (`prefix`, `suffix`) fit best. With a nearness a tie
 * goes to the nearest; only tied places are measured, the words around
 * ranking first, so an edit that leaves them telling places apart is found by
 * them. `index` is read only once the quote is worth looking for.
 */
export function findQuoteIn(
  index: () => TextIndex | null, quote: string, prefix: unknown, suffix: unknown, how: QuoteSearch,
): Range | null {
  const q = normal(quote, how.reading);
  if (q.length < (how.shortest || 1)) return null;
  const read = index();
  if (!read) return null;
  const text = read.text;
  const p = normal(prefix, how.reading);
  const s = normal(suffix, how.reading);
  const nearness = how.nearness;
  const within = how.within;
  const measured = new Map<number, number>();
  const far = function (at: number): number {
    let d = measured.get(at);
    if (d === undefined) {
      if (!nearness || measured.size >= QUOTE_MEASURED) return Infinity;
      d = nearness({ node: read.nodes[read.nodeAt[at]], range: function () { return rangeAt(read, at, q.length); } });
      measured.set(at, d);
    }
    return d;
  };
  let best = -1;
  let bestScore = -1;
  const most = nearness ? Infinity : QUOTE_TRIES;
  for (let at = text.indexOf(q), tries = 0; at !== -1 && tries < most; at = text.indexOf(q, at + 1), tries++) {
    if (within && !within.contains(read.nodes[read.nodeAt[at]])) continue;
    const score = sharedTail(text, Math.max(0, at - p.length), at, p) + sharedHead(text, at + q.length, at + q.length + s.length, s);
    if (score > bestScore || (score === bestScore && far(at) < far(best))) {
      best = at;
      bestScore = score;
    }
  }
  return best < 0 ? null : rangeAt(read, best, q.length);
}
