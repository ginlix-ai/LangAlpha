/**
 * Splits a markdown document into independently renderable blocks.
 *
 * A streaming reply re-renders on every typewriter tick, and the parser, the
 * syntax highlighter and React all pay for the whole document each time. Cut
 * at blank lines the parser would also stop at, and everything but the last
 * block becomes a fixed string the renderer can memoize.
 *
 * Correctness over granularity: a cut in the wrong place changes the rendered
 * output, a missed cut only costs memoization. So a blank line inside a fence,
 * a display-math block, or a loose list never splits, an indented line after a
 * blank line is treated as a continuation, and a document that uses constructs
 * with cross-block meaning (link reference definitions, footnotes, block-level
 * raw HTML) is rendered whole.
 *
 * That last bail is decided per call against the text so far, so a reply that
 * streams its first raw HTML tag or reference definition collapses from many
 * blocks to one at that tick and remounts once. It stays whole from there on,
 * since the text only grows.
 */
import { FENCE_OPEN_RE, closesFence } from './markdownSegments';

const BLANK_RE = /^[ \t]*$/;
const LIST_ITEM_RE = /^ {0,3}(?:[-*+]|\d{1,9}[.)])(?:[ \t]|$)/;
const INDENTED_RE = /^[ \t]/;
/**
 * Block-quote and list-item markers a line may open with. A definition keeps
 * its document-wide scope inside those containers, so they are stripped
 * before the cross-block test rather than hiding it.
 */
const CONTAINER_PREFIX_RE = /^(?: {0,3}(?:>|[-*+]|\d{1,9}[.)])(?:[ \t]|$))+/;
/**
 * A line opening a construct whose meaning depends on other blocks: render
 * the whole document. The tag list is CommonMark's block-HTML set plus the
 * containers the sanitizer lets through beyond it (picture, svg, math),
 * whose children may sit past a blank line. Tested per line outside fences and display math, where
 * the same text is literal, with any container prefix removed first.
 */
const CROSS_BLOCK_RE = /^ {0,3}(?:\[[^\]]+\]:|<(?:!--|\/?(?:address|article|aside|blockquote|body|caption|center|col|colgroup|dd|details|dialog|dir|div|dl|dt|fieldset|figcaption|figure|footer|form|frame|frameset|h[1-6]|head|header|hr|html|iframe|legend|li|link|main|math|menu|nav|noframes|ol|optgroup|option|p|param|picture|pre|script|section|source|style|summary|svg|table|tbody|td|textarea|tfoot|th|thead|title|tr|track|ul)\b))/i;

/**
 * Returns the blocks in order; joining them reproduces the input exactly.
 * Trailing blank lines belong to the block before them.
 */
export function splitMarkdownBlocks(content: string): string[] {
  if (!content) return [content];

  const lines = content.split(/(?<=\n)/);
  const blocks: string[] = [];
  let current = '';
  let fence: string | null = null;
  let inMath = false;
  let pendingBlank = false;
  // A list item anywhere in the block keeps its list open to the block's end
  // (a lazy or indented line never closes one), so a following item after a
  // blank line is the same loose list even when the block opened with a
  // paragraph or heading the item interrupted.
  let listOpen = false;

  for (const line of lines) {
    const body = line.replace(/\r?\n$/, '');

    if (fence !== null) {
      current += line;
      if (closesFence(line, fence)) fence = null;
      continue;
    }
    if (inMath) {
      current += line;
      if ((body.match(/\$\$/g) || []).length % 2 === 1) inMath = false;
      continue;
    }

    if (BLANK_RE.test(body)) {
      current += line;
      pendingBlank = current.trim().length > 0;
      continue;
    }
    if (CROSS_BLOCK_RE.test(body.replace(CONTAINER_PREFIX_RE, ''))) return [content];

    if (pendingBlank) {
      pendingBlank = false;
      const continues = INDENTED_RE.test(body) || (listOpen && LIST_ITEM_RE.test(body));
      if (!continues) {
        blocks.push(current);
        current = '';
        listOpen = false;
      }
    }

    current += line;
    if (LIST_ITEM_RE.test(body)) listOpen = true;
    const open = FENCE_OPEN_RE.exec(line);
    if (open) fence = open[1];
    else if ((body.match(/\$\$/g) || []).length % 2 === 1) inMath = true;
  }

  if (current) blocks.push(current);
  return blocks.length ? blocks : [content];
}
