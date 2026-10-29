/**
 * Holds the tail of a streaming markdown block to the shape it will settle in.
 *
 * Markdown read mid-construct often means something else for a tick: `Label:`
 * over a lone `-` is a setext heading, `**bold` shows its asterisks, a header
 * row without its delimiter row is a paragraph, an open `$$` hands half a
 * formula to KaTeX. Each draws a shape the next tick throws away, and
 * everything below it moves twice. So the block still being written renders
 * repaired: what cannot take its final shape yet is held back, and what can is
 * closed.
 *
 * The repair only cuts the block short and appends to it, so every character
 * it shows sits at its source offset: the reveal fade's marks and the live
 * fence are offsets into the block as written. The one character it hides in
 * front of shown text, the `[` of a link still being written, is overwritten
 * in place by a word joiner, which draws nothing and adds no break.
 *
 * Only the line being typed is read for inline constructs. An emphasis or a
 * code span left open on a finished line may be literal, and closing it would
 * draw a shape the settled text never has.
 */
import { scanFences } from './markdownBlocks';

export interface StreamingTail {
  /** The block as it should render. */
  text: string;
  /** How much of the block `text` shows; what follows it is held back. */
  shown: number;
}

const WORD_JOINER = '\u2060';
// How far past a `$` its line can run without closing it before the `$` reads
// as a stray one (`$(0.12)`, `$-denominated`) rather than a formula still
// being typed. Held longer, it stalls the rest of a paragraph, which a reply
// usually writes as one line, until the line ends.
const OPEN_MATH_MAX = 64;
// A display-math opener that its own line does not close: `$$` after any
// quote or list markers, or `\[` alone, as LaTeX display is written.
const DISPLAY_MATH_RE = /^[ \t]*(?:(?:>|[-*+]|\d{1,9}[.)])[ \t]*)*(\$\$|\\\[(?=[ \t]*$))/;
// A line that is still only block markers: a list item or quote with nothing
// in it, a heading's hashes, a setext underline or thematic break, the digits
// of an ordered marker before its dot. Rendered, each changes the line above
// it or draws an empty box for a tick.
const MARKER_ONLY_RE = /^[ \t]*(?:(?:>|[-*+]|\d{1,9}[.)])[ \t]*)*(?:\[[ xX]\][ \t]*)?(?:#{1,6}|\d{1,9}|=+)?[ \t]*$/;
// Where a line's inline content starts: past its quote and list markers, a
// heading's hashes, a task box.
const LINE_PREFIX_RE = /^[ \t]*(?:(?:>|[-*+](?=[ \t])|\d{1,9}[.)](?=[ \t]))[ \t]*)*(?:#{1,6}(?=[ \t]|$)[ \t]*)?(?:\[[ xX]\](?=[ \t]))?/;
// A list item's task box still being typed, which lands as a checkbox.
const TASK_BOX_SO_FAR_RE = /^[ \t]*(?:>[ \t]*)*(?:[-*+]|\d{1,9}[.)])[ \t]+\[(?:[ xX]\]?)?$/;
const CLOSING_LINE_RE = /^ {0,3}(?:`{3,}|~{3,}|\$\$)[ \t]*$/;
const TABLE_LINE_RE = /^ {0,3}\|/;
const TABLE_ROW_RE = /^( {0,3})\|(.*)\|[ \t]*\r?$/;
const DELIMITER_CELL_RE = /^[ \t]*:?-+:?[ \t]*$/;
const URL_START_RE = /^(?:https?:\/\/|www\.)/i;
// GFM leaves these off the end of an autolink literal.
const URL_TRAILING_RE = /[?!.,:*_~]/;
const HTML_TAG_RE = /^<\/?[a-z][a-z0-9-]*(?=[\s/>]|$)/;
const WS_RE = /\s/;
const ALNUM_RE = /[\p{L}\p{N}]/u;
const LATIN_ALNUM_RE = /[A-Za-z0-9]/;
const PUNCT_RE = /[\p{P}\p{S}]/u;

/**
 * Repairs `block`, the last block of a reply still streaming in. Settled text
 * must never come through here: a construct left open at the end of a
 * finished reply is meant literally.
 */
export function repairStreamingTail(block: string): StreamingTail {
  const held = holdBackBlocks(block);
  if (held.code) return { text: block.slice(0, held.end), shown: held.end };

  let end = held.end;
  let text = block.slice(0, end);
  let suffix = '';
  let partialRow: string | null = null;
  const lineStart = text.lastIndexOf('\n') + 1;
  if (lineStart < text.length) {
    const line = text.slice(lineStart);
    if (CLOSING_LINE_RE.test(line)) {
      // `holdBackBlocks` has held every opener, so this line closes a fence
      // or display math, and takes its final shape as it is.
    } else if (TASK_BOX_SO_FAR_RE.test(line)) {
      end = lineStart;
    } else if (TABLE_LINE_RE.test(line)) {
      // A row being typed reflows its table on every cell; it lands whole.
      partialRow = line;
      end = lineStart;
    } else {
      const repaired = repairLine(line);
      let shown = line.slice(0, repaired.cut);
      for (const at of repaired.joiners) shown = shown.slice(0, at) + WORD_JOINER + shown.slice(at + 1);
      if (MARKER_ONLY_RE.test(shown.replaceAll(WORD_JOINER, ''))) {
        end = lineStart;
      } else {
        end = lineStart + repaired.cut;
        text = text.slice(0, lineStart) + shown;
        suffix = repaired.closers;
      }
    }
  }
  if (end <= lineStart) {
    text = block.slice(0, end);
    suffix = provisionalDelimiter(text, partialRow);
  }
  return { text: text + suffix, shown: end };
}

/**
 * Where the block stops showing at block level, and whether it ends in code.
 * Display math still open is held from its opener, since KaTeX would draw
 * half a formula or an error. A fence opener whose info string is still
 * typing is held, and so is a closing fence being typed, which draws as one
 * more line of code until it is long enough to close. Inside an open fence
 * nothing else is read: it is code.
 */
function holdBackBlocks(block: string): { end: number; code: boolean } {
  // Cast because TypeScript does not see the callback assign it.
  let math = null as { at: number; close: string } | null;
  const fence = scanFences(block, (body, offset) => {
    if (math !== null) {
      if (body.includes(math.close)) math = null;
      return true;
    }
    const display = DISPLAY_MATH_RE.exec(body);
    const close = display?.[1] === '$$' ? '$$' : '\\]';
    if (display && !body.slice(display[0].length).includes(close)) math = { at: offset, close };
    return false;
  });
  if (math !== null) return { end: math.at, code: false };
  if (fence === null) return { end: block.length, code: false };
  // The opener is the last line and has no newline yet.
  if (!block.includes('\n', fence.at)) return { end: fence.at, code: false };
  const lineStart = block.lastIndexOf('\n') + 1;
  const last = block.slice(lineStart);
  const fenceChar = fence.opener[0];
  const closing = last.trimStart();
  if (last && last.length - closing.length <= 3 && closing.replace(/[ \t]+$/, '').split('').every((c) => c === fenceChar)) {
    return { end: lineStart, code: true };
  }
  return { end: block.length, code: true };
}

interface Delimiter { kind: 'delimiter'; ch: string; len: number; at: number }
interface Bracket { kind: 'bracket'; at: number; image: boolean; cite: boolean }
type Opener = Delimiter | Bracket;

function runLength(s: string, i: number): number {
  let j = i;
  while (s[j] === s[i]) j++;
  return j - i;
}

function lastIndex<T>(xs: readonly T[], test: (x: T) => boolean): number {
  for (let k = xs.length - 1; k >= 0; k--) if (test(xs[k])) return k;
  return -1;
}

// Whether `line` closes, from `from` on, inline math opened by `len` dollars.
function closesMath(line: string, from: number, len: number): boolean {
  for (let i = from; i < line.length;) {
    if (line[i] === '\\') { i += 2; continue; }
    if (line[i] !== '$') { i++; continue; }
    const run = runLength(line, i);
    if (run === len) return true;
    i += run;
  }
  return false;
}

// Whether a destination typed so far can still be a citation's web address.
const citationUrlSoFar = (dest: string): boolean =>
  /^https?:\/\//i.test(dest) || 'https://'.startsWith(dest.toLowerCase()) || 'http://'.startsWith(dest.toLowerCase());

/**
 * The line being typed, read for inline constructs left open at its end.
 * `cut` is where it stops showing, `joiners` the `[` it overwrites, and
 * `closers` what to append so the rest takes its final shape.
 *
 * A code span or emphasis still open is closed. A delimiter with nothing after
 * it yet is held, and so is a closer half typed (`**bold*`), which would
 * otherwise draw as one asterisk and italics. Inline math is held whole until
 * it closes, or until it has run on too long to be a formula. A link still being written shows its label as plain text, which
 * a file link keeps, and holds its destination; an image or a citation, whose
 * final shape is not text at all, is held from its first character, and so is
 * a citation whose closing parenthesis has not arrived. Emphasis is closed
 * only where it could open, so `2*3` or `snake_case` never turns into italics.
 */
function repairLine(line: string): { cut: number; joiners: number[]; closers: string } {
  const n = line.length;
  const stack: Opener[] = [];
  let code: { at: number; len: number } | null = null;
  let math: { at: number; len: number } | null = null;
  let link: { bracket: Bracket; at: number; depth: number } | null = null;
  let citation: { from: number; to: number } | null = null;
  let cut = n;
  let i = LINE_PREFIX_RE.exec(line)?.[0].length ?? 0;
  const from = i;

  scan: while (i < n) {
    const c = line[i];
    if (code !== null) {
      if (c !== '`') { i++; continue; }
      const run = runLength(line, i);
      if (run === code.len) code = null;
      else if (i + run === n && run < code.len) { cut = i; break; }
      i += run;
      continue;
    }
    if (math !== null) {
      if (c === '\\') { i += 2; continue; }
      if (c !== '$') { i++; continue; }
      const run = runLength(line, i);
      if (run === math.len) math = null;
      i += run;
      continue;
    }
    if (link !== null) {
      if (c === '\\') { i += 2; continue; }
      if (c === '(') link.depth++;
      else if (c === ')' && link.depth-- === 0) {
        if (link.bracket.cite && /^https?:\/\//i.test(line.slice(link.at + 2, i))) citation = { from: link.bracket.at - 1, to: i + 1 };
        link = null;
      }
      i++;
      continue;
    }
    switch (c) {
      case '\\': {
        // Half an escape, or LaTeX the delimiter pass has not seen closed yet.
        if (i + 1 === n || line[i + 1] === '(' || line[i + 1] === '[') { cut = i; break scan; }
        i += 2;
        continue;
      }
      case '`': {
        const run = runLength(line, i);
        code = { at: i, len: run };
        i += run;
        continue;
      }
      case '$': {
        const run = runLength(line, i);
        const after = line[i + run];
        if (after === undefined) { cut = i; break scan; }
        if (run <= 2 && !WS_RE.test(after) && (n - i <= OPEN_MATH_MAX || closesMath(line, i + run, run))) {
          math = { at: i, len: run };
        }
        i += run;
        continue;
      }
      case '<': {
        const tag = HTML_TAG_RE.exec(line.slice(i));
        if (!tag) {
          // A tag's first characters.
          if (/^<\/?$/.test(line.slice(i))) { cut = i; break scan; }
          i++;
          continue;
        }
        const close = line.indexOf('>', i + tag[0].length);
        if (close < 0) { cut = i; break scan; }
        i = close + 1;
        continue;
      }
      case '(':
      case '!': {
        // Possibly the first character of a citation or an image, which the
        // next one settles.
        if (i + 1 === n && (i === from || WS_RE.test(line[i - 1]))) { cut = i; break scan; }
        i++;
        continue;
      }
      case '[': {
        const before = line[i - 1];
        // `array[0` is not a link; `是[链接` is.
        if (i === from || before === undefined || !LATIN_ALNUM_RE.test(before)) {
          stack.push({ kind: 'bracket', at: i, image: before === '!', cite: before === '(' });
        }
        i++;
        continue;
      }
      case ']': {
        const k = lastIndex(stack, (o) => o.kind === 'bracket');
        if (k < 0) { i++; continue; }
        const bracket = stack[k] as Bracket;
        // What opened inside the label and never closed there is literal.
        stack.length = k;
        if (line[i + 1] === '(' || i + 1 === n) { link = { bracket, at: i, depth: 0 }; i += 2; continue; }
        i++;
        continue;
      }
      case '*':
      case '_':
      case '~': {
        const run = runLength(line, i);
        const before = line[i - 1];
        const after = line[i + run];
        // A single tilde is literal (`singleTilde: false`), or the first of two.
        if (c === '~' && run !== 2) {
          if (after === undefined && run === 1) { cut = i; break scan; }
          i += run;
          continue;
        }
        const closes = i > from && !WS_RE.test(before) && (c !== '_' || after === undefined || !ALNUM_RE.test(after));
        if (closes) {
          const k = lastIndex(stack, (o) => o.kind === 'bracket' || o.ch === c);
          const open = k >= 0 ? stack[k] : null;
          if (open?.kind === 'delimiter') {
            if (run < open.len && after === undefined) { cut = i; break scan; }
            stack.length = k;
            i += run;
            continue;
          }
        }
        const edge = i === from || WS_RE.test(before) || PUNCT_RE.test(before);
        const leftFlanking = after !== undefined && !WS_RE.test(after) && (edge || !PUNCT_RE.test(after));
        // Inside a word only `*` opens, as CommonMark has it, which bold
        // between CJK characters depends on; after a Latin letter or digit it
        // is far likelier arithmetic (`2*3`, `2**3`).
        const opens = leftFlanking && (edge || (c === '*' && !LATIN_ALNUM_RE.test(before)));
        if (opens) {
          stack.push({ kind: 'delimiter', ch: c, len: run, at: i });
          i += run;
          continue;
        }
        if (after === undefined) { cut = i; break scan; }
        i += run;
        continue;
      }
      default: {
        // An address is opaque: its underscores and asterisks open nothing.
        if ((c === 'h' || c === 'H' || c === 'w' || c === 'W') && URL_START_RE.test(line.slice(i, i + 8))
          && (i === from || /[\s(*_~]/.test(line[i - 1]))) {
          let j = i;
          while (j < n && !/[\s<[\]]/.test(line[j])) j++;
          while (j > i && URL_TRAILING_RE.test(line[j - 1])) j--;
          i = Math.max(j, i + 1);
          continue;
        }
        i++;
      }
    }
  }

  let codeCloser = '';
  const joiners: number[] = [];
  if (code !== null) {
    if (cut <= code.at + code.len) cut = code.at;
    else codeCloser = '`'.repeat(code.len);
  } else if (math !== null) {
    cut = Math.min(cut, math.at);
  } else if (link !== null) {
    const { bracket } = link;
    if (bracket.image || (bracket.cite && citationUrlSoFar(line.slice(link.at + 2, cut)))) cut = Math.min(cut, bracket.at - 1);
    else {
      cut = Math.min(cut, link.at);
      if (bracket.at + 1 < cut) joiners.push(bracket.at);
      else cut = bracket.at;
    }
  } else if (citation !== null && citation.to === n) {
    cut = citation.from;
  }
  // A label with no `]` yet: the same reading as a link still being written.
  for (const o of stack) {
    if (o.kind !== 'bracket' || o.at >= cut) continue;
    if (o.image || o.cite) { cut = o.at - 1; break; }
    if (o.at + 1 < cut) joiners.push(o.at);
    else { cut = o.at; break; }
  }

  const open = stack.filter((o): o is Delimiter => o.kind === 'delimiter' && o.at < cut);
  // Behind a code span's closer, an emphasis closer follows punctuation, which
  // closes whatever precedes it; on bare text it has to touch the last word.
  if (open.length && !codeCloser) {
    // A closer after whitespace closes nothing.
    while (cut > from && WS_RE.test(line[cut - 1])) cut--;
    // An opener with nothing after it opens nothing visible.
    while (open.length && open[open.length - 1].at + open[open.length - 1].len >= cut) {
      cut = open.pop()!.at;
      while (cut > from && WS_RE.test(line[cut - 1])) cut--;
    }
  }
  let closers = codeCloser;
  for (let k = open.length - 1; k >= 0; k--) closers += open[k].ch.repeat(open[k].len);
  return { cut, joiners: joiners.filter((at) => at < cut), closers };
}

/**
 * A delimiter row for a header row that has none yet, so the table draws as a
 * table from its first row instead of as a paragraph for a line. The
 * alignment comes from the delimiter row being typed, where there is one.
 */
function provisionalDelimiter(text: string, partialRow: string | null): string {
  if (!text.endsWith('\n')) return '';
  // The last two lines only, read in place: the block can be the whole reply.
  const end = text.length - 1;
  const lastStart = text.lastIndexOf('\n', end - 1) + 1;
  const header = TABLE_ROW_RE.exec(text.slice(lastStart, end));
  if (!header) return '';
  // Another row above it: the table already has its delimiter, or never will.
  if (lastStart > 0 && TABLE_LINE_RE.test(text.slice(text.lastIndexOf('\n', lastStart - 2) + 1, lastStart - 1))) return '';
  const cells = header[2].split(/(?<!\\)\|/);
  if (cells.length < 2) return '';
  const typed = (partialRow ?? '').trim().replace(/^\|/, '').split('|');
  const align = (k: number) => (typed[k] !== undefined && DELIMITER_CELL_RE.test(typed[k]) ? typed[k].trim() : '---');
  return `${header[1]}|${cells.map((_, k) => align(k)).join('|')}|`;
}
