/**
 * Splits a markdown string into prose and code regions.
 *
 * The `Markdown` pipeline rewrites the whole string before parsing — citation
 * bubbles, the currency guard, LaTeX delimiters. Inside code, markdown stops
 * interpreting escapes and raw HTML, so those rewrites are not harmless there:
 * an injected `\` or `<cite-bubble>` reaches the screen verbatim, and the code
 * block's Copy button hands back text that no longer parses as what it claims
 * to be. Anything that rewrites prose must run through one of the helpers here.
 */

interface Span {
  text: string;
  code: boolean;
}

/** Opening fence: up to 3 spaces of indent, then 3+ backticks or tildes. */
export const FENCE_OPEN_RE = /^ {0,3}(`{3,}|~{3,})/;
/** Closing fence: same character as the opener, at least as long, nothing else. */
const FENCE_CLOSE_RE = /^ {0,3}(`{3,}|~{3,})[ \t]*$/;

export function closesFence(line: string, opener: string): boolean {
  // The terminator has to come off first: JavaScript's `$` does not match ahead
  // of a trailing newline, so testing the raw line never matches.
  const match = FENCE_CLOSE_RE.exec(line.replace(/\r?\n$/, ''));
  return (
    match !== null &&
    match[1][0] === opener[0] &&
    match[1].length >= opener.length
  );
}

/** Appends `text`, merging into the previous span when the kind matches. */
function push(spans: Span[], text: string, code: boolean): void {
  if (!text) return;
  const last = spans[spans.length - 1];
  if (last && last.code === code) last.text += text;
  else spans.push({ text, code });
}

/**
 * Cuts `content` at fenced-code boundaries. Fence lines belong to the code
 * span, and an unterminated fence runs to the end — matching how the markdown
 * parser downstream will read it.
 */
function splitFences(content: string): Span[] {
  // Lookbehind keeps the terminator on each line, so joining the spans
  // reproduces the input byte for byte.
  const lines = content.split(/(?<=\n)/);
  const spans: Span[] = [];
  let opener: string | null = null;

  for (const line of lines) {
    if (opener === null) {
      const open = FENCE_OPEN_RE.exec(line);
      if (open) {
        opener = open[1];
        push(spans, line, true);
      } else {
        push(spans, line, false);
      }
      continue;
    }
    push(spans, line, true);
    if (closesFence(line, opener)) opener = null;
  }
  return spans;
}

/** Index of the next run of exactly `length` backticks at or after `from`. */
function findClosingRun(text: string, from: number, length: number): number {
  let i = from;
  while (i < text.length) {
    if (text[i] !== '`') {
      i += 1;
      continue;
    }
    let run = 0;
    while (text[i + run] === '`') run += 1;
    if (run === length) return i;
    i += run;
  }
  return -1;
}

/**
 * Cuts one fence-free span at inline-code boundaries. A code span opens with a
 * run of N backticks and closes with a run of exactly N; an unmatched run is
 * literal text, not an opener.
 *
 * `isOpaque` picks which code spans the caller must not rewrite. The rest are
 * marked as prose, so `push` merges them back into the text around them and a
 * caller that protects only some spans still hands its transform whole lines.
 */
function splitInlineCode(
  text: string,
  isOpaque: (span: string) => boolean = () => true
): Span[] {
  const spans: Span[] = [];
  let i = 0;
  while (i < text.length) {
    if (text[i] !== '`') {
      const next = text.indexOf('`', i);
      const stop = next === -1 ? text.length : next;
      push(spans, text.slice(i, stop), false);
      i = stop;
      continue;
    }
    let run = 0;
    while (text[i + run] === '`') run += 1;
    const close = findClosingRun(text, i + run, run);
    if (close === -1) {
      push(spans, text.slice(i, i + run), false);
      i += run;
      continue;
    }
    const span = text.slice(i, close + run);
    push(spans, span, isOpaque(span));
    i = close + run;
  }
  return spans;
}

const crossesLines = (span: string): boolean => span.includes('\n');

/**
 * Applies `transform` to everything outside fenced code blocks.
 *
 * The weakest guard here: it leaves inline code exposed. Line-structural
 * rewrites want `mapOutsideMultilineCode` and character-level ones want
 * `mapOutsideCode` — reach for this only where inline code is the point, as
 * `normalizeFileRefs` does when it unwraps backticks around a file reference.
 */
export function mapOutsideFences(
  content: string,
  transform: (prose: string) => string
): string {
  if (!content || typeof content !== 'string') return content;
  return splitFences(content)
    .map((span) => (span.code ? span.text : transform(span.text)))
    .join('');
}

/**
 * Applies `transform` to everything outside fenced blocks *and* inline code
 * spans. Use for character-level rewrites that would be visible corruption
 * inside any code — escaping, tag injection, delimiter substitution.
 */
export function mapOutsideCode(
  content: string,
  transform: (prose: string) => string
): string {
  return mapOutsideFences(content, (prose) =>
    splitInlineCode(prose)
      .map((span) => (span.code ? span.text : transform(span.text)))
      .join('')
  );
}

/**
 * Applies `transform` to everything outside fenced blocks and *line-crossing*
 * inline code spans. Use for line-structural rewrites — table repair, prose
 * reflow.
 *
 * A code span may cross line endings (CommonMark folds them to spaces), so a
 * line-oriented pass can walk into one and rewrite lines that were never
 * markdown. Single-line spans stay in the text the transform sees, because
 * they sit inside the very lines it exists to repair: hiding `` `AAPL` ``
 * would take its table row with it.
 */
export function mapOutsideMultilineCode(
  content: string,
  transform: (prose: string) => string
): string {
  return mapOutsideFences(content, (prose) =>
    splitInlineCode(prose, crossesLines)
      .map((span) => (span.code ? span.text : transform(span.text)))
      .join('')
  );
}
