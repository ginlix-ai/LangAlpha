import { normalizeFileRefs } from './normalizeFileRefs';
import { mapOutsideCode, mapOutsideMultilineCode } from './markdownSegments';

/**
 * Fix malformed GFM tables so remark-gfm can parse them.
 *
 * Common LLM mistakes:
 *  - Separator row has fewer columns than the header row
 *  - Data rows have fewer/more columns than the header
 *  - Merged/mangled cells like "|---|------ 1 | ..."
 *
 * Strategy: detect table blocks (consecutive lines starting/ending with |),
 * count header columns, then rebuild the separator and pad/trim data rows.
 */
function fixMarkdownTables(content: string): string {
  if (!content || typeof content !== 'string') return content;

  const lines = content.split('\n');
  const result: string[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    // Detect a potential table header: must have at least 2 pipes and look like "| ... | ... |"
    if (trimmed.startsWith('|') && trimmed.endsWith('|') && (trimmed.match(/\|/g) || []).length >= 3) {
      const headerCols = trimmed.split('|').slice(1, -1); // cells between outer pipes
      const colCount = headerCols.length;

      // Check if next line is a separator (pipes + dashes/colons/spaces only)
      if (i + 1 < lines.length && /^\|[\s\-:|]+\|$/.test(lines[i + 1].trim())) {
        const sepCols = lines[i + 1].trim().split('|').slice(1, -1).length;

        // If separator column count doesn't match header, rebuild it
        if (sepCols !== colCount) {
          result.push(line);
          result.push('|' + Array(colCount).fill('---').join('|') + '|');
          i += 2;
        } else {
          // Separator is fine, push both
          result.push(line);
          result.push(lines[i + 1]);
          i += 2;
        }

        // Now process data rows: pad or trim cells to match colCount
        while (i < lines.length) {
          const row = lines[i].trim();
          if (!row.startsWith('|')) break; // end of table

          const cells = row.split('|');
          // Split gives ['', cell1, cell2, ..., ''] for "|a|b|"
          // Handle malformed rows that don't end with |
          const hasTrailingPipe = row.endsWith('|');
          const inner = hasTrailingPipe ? cells.slice(1, -1) : cells.slice(1);

          if (inner.length === colCount) {
            result.push(lines[i]); // row is fine
          } else if (inner.length < colCount) {
            // Pad with empty cells
            const padded = [...inner, ...Array(colCount - inner.length).fill(' ')];
            result.push('|' + padded.join('|') + '|');
          } else {
            // Too many cells -- trim to colCount
            result.push('|' + inner.slice(0, colCount).join('|') + '|');
          }
          i++;
        }
        continue;
      }
    }

    result.push(line);
    i++;
  }

  return result.join('\n');
}

/**
 * Strip YAML front matter (--- delimited block at start of content).
 * Without this, `---` renders as <hr> and YAML fields as plain text.
 */
function stripFrontMatter(content: string): string {
  if (!content || typeof content !== 'string') return content;
  if (!content.startsWith('---\n') && !content.startsWith('---\r\n')) return content;
  const end = content.indexOf('\n---', 3);
  if (end === -1) return content;
  // Skip past closing "---" and optional newline
  const rest = content.slice(end + 4);
  return rest.startsWith('\n') ? rest.slice(1) : rest.startsWith('\r\n') ? rest.slice(2) : rest;
}

// A cashtag: `$AAPL`, `$BRK.B`. Followed by a dollar, `^`, `_`, `{`, `(` or a
// backslash, it is the start of a formula (`$X$`, `$P(A)$`, `$X_t$`) instead.
const TICKER = String.raw`[A-Z]{1,5}(?:\.[A-Z]{1,2})?(?![\p{L}\p{N}$^_{(\\])`;
const TICKER_DOLLAR_RE = new RegExp(String.raw`(?<![\\$])\$(?=${TICKER})`, 'gu');
const OPENS_TICKER_RE = new RegExp(String.raw`^\$${TICKER}`, 'u');

/**
 * Escape dollar signs used as currency ($129.82, $/bbl, in millions ($)) or in
 * front of a ticker ($AAPL, $BRK.B) so remark-math does not treat them as
 * inline-math delimiters. remark-math pairs any two dollars, so `$AAPL and
 * $MSFT` would set "AAPL and " as a formula. $$ (display math) is left alone.
 *
 * Capitals after a dollar also start formulas (`$N = 5$`), so a ticker-shaped
 * one stays math when the next dollar in its paragraph closes it: touching
 * the text before it, not just after an opening bracket or quote (the dollar
 * in `($x$)` opens a formula of its own), and not itself opening a ticker.
 */
function escapeCurrencyDollars(content: string): string {
  if (!content || typeof content !== 'string') return content;
  // A dollar standing for the unit (`85 $/bbl`, `($)`) opens nothing either;
  // after a word it closes a formula, as in `($x$)`.
  const currency = content
    .replace(/(?<!\$)\$(?!\$)(?=[-+]?\d)/g, '\\$')
    .replace(/(?<=^|[\s(])\$(?=[/)])/gm, '\\$');
  return currency.replace(TICKER_DOLLAR_RE, (dollar, at: number) => (closesInlineMath(currency, at + 1) ? dollar : '\\$'));
}

// The next dollar is found first and only the text up to it searched for a
// blank line, so a list with a cashtag on every row stays one pass.
function closesInlineMath(content: string, from: number): boolean {
  for (let i = content.indexOf('$', from); i >= 0; i = content.indexOf('$', i + 1)) {
    if (content[i - 1] === '\\') continue;
    if (content[i + 1] === '$') { i++; continue; }
    if (/\n[ \t]*\n/.test(content.slice(from, i))) return false;
    return !/[\s([{"“]/.test(content[i - 1]) && !OPENS_TICKER_RE.test(content.slice(i, i + 12));
  }
  return false;
}

/**
 * Normalize LaTeX delimiters for remark-math compatibility.
 *
 * LLMs often emit \[...\] (display) and \(...\) (inline) notation,
 * but remark-math only recognizes $$...$$ and $...$ delimiters.
 */
function normalizeLatexDelimiters(content: string): string {
  if (!content || typeof content !== 'string') return content;

  // Convert display math: \[...\] -> $$...$$
  // Match \[ ... \] allowing newlines in between
  content = content.replace(/\\\[([\s\S]*?)\\\]/g, (_, math) => `$$${math}$$`);

  // Convert inline math: \(...\) -> $...$
  content = content.replace(/\\\((.*?)\\\)/g, (_, math) => `$${math}$`);

  return content;
}

// A whole line holding one `$$...$$` equation. The body has no unescaped `$`,
// so two equations on one line, or `$$` closing mid-line, never match.
const ONE_LINE_DISPLAY_MATH_RE = /^( {0,3})\$\$((?:[^$\\\n]|\\.)+)\$\$[ \t]*$/gm;

/**
 * Give display math written on a single line a block of its own.
 *
 * remark-math only opens a display block on a `$$` line by itself, so
 * `$$x$$` (and `\[x\]`, which normalizeLatexDelimiters has already turned into
 * it) parses as inline math even when it is the whole line. The equation moves
 * onto its own line between fences; `$$x$$` inside a sentence stays inline,
 * and an equation still streaming in has no closing `$$` to match yet.
 */
function blockOneLineDisplayMath(content: string): string {
  if (!content || typeof content !== 'string') return content;
  return content.replace(ONE_LINE_DISPLAY_MATH_RE, (line, indent: string, body: string) =>
    body.trim() ? `${indent}$$\n${indent}${body.trim()}\n${indent}$$` : line,
  );
}

/**
 * Drop the row break LLMs leave after the last row of a numbered environment.
 *
 * LaTeX, and KaTeX since 0.18, read that `\\` as opening one more row and
 * number it, so the block grows an empty line tagged with the next equation
 * number. The unnumbered forms (`aligned`, `align*`, matrices) draw nothing
 * for it, so only the three numbered ones are touched.
 */
function dropTrailingRowBreaks(content: string): string {
  if (!content || typeof content !== 'string') return content;
  return content.replace(/\\\\\s*(?=\\end\{(?:align|gather|alignat)\})/g, '');
}

/**
 * Convert inline citation patterns ([label](url)) into <cite-bubble> HTML tags.
 * rehype-raw will parse these into the AST and the CitationBubble component renders them.
 */
export function escapeHtmlAttr(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
}

export function transformCitationBubbles(content: string): string {
  if (!content || typeof content !== 'string') return content;
  return content.replace(
    /\(\[([^\]]+)\]\((https?:\/\/[^)]+)\)\)/g,
    (_, label, url) => {
      // Encode $ as %24 so escapeCurrencyDollars won't mangle URLs (e.g. ?price=$100)
      const safeUrl = url.replace(/\$/g, '%24');
      return `<cite-bubble label="${escapeHtmlAttr(label)}" href="${escapeHtmlAttr(safeUrl)}"></cite-bubble>`;
    }
  );
}

/**
 * The text the markdown parser reads. Every pass rewrites prose before it gets
 * there, so each one has to say how much of the string it may touch. Inside code,
 * markdown stops interpreting escapes and raw HTML: a rewrite that lands there is
 * visible corruption and makes the Copy button return unparseable text.
 */
export function prepareMarkdownSource(content: string, rootDir: Parameters<typeof normalizeFileRefs>[1], otherRoots: Parameters<typeof normalizeFileRefs>[2]): string {
  // Whole-string by design: front matter is anchored at the start, and
  // normalizeFileRefs deliberately unwraps backticks around file refs.
  const base = normalizeFileRefs(stripFrontMatter(content), rootDir, otherRoots);
  // Table repair is line-structural, so it reads whole lines and has to stay
  // out of the code spans that own one: fenced, or inline across a break.
  const tables = mapOutsideMultilineCode(base, fixMarkdownTables);
  // These inject characters and tags, which is corruption inside any code.
  const prose = mapOutsideCode(tables, (text) =>
    dropTrailingRowBreaks(normalizeLatexDelimiters(escapeCurrencyDollars(transformCitationBubbles(text))))
  );
  // Line-structural again, and after the delimiter pass so `\[x\]` arrives
  // already spelled `$$x$$`.
  return mapOutsideMultilineCode(prose, blockOneLineDisplayMath);
}
