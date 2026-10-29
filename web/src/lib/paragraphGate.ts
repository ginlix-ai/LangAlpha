import { fromMarkdown } from 'mdast-util-from-markdown';
import { mathFromMarkdown } from 'mdast-util-math';
import { math } from 'micromark-extension-math';

/** Chars of unbroken tail we will hold back before giving up and showing it.
 *  A model that answers in one long paragraph, or opens a fence and writes for
 *  a minute, would otherwise stream into a blank bubble. */
const MAX_HELD_CHARS = 2000;

interface Positioned {
  type: string;
  position?: { start: { offset?: number }; end: { offset?: number } };
  children?: Positioned[];
}

/** Every span of the source that is code, at any nesting depth.
 *
 *  A blank line inside one of these is content, not a paragraph break, and
 *  cutting there hands the renderer an unterminated block. Nesting is what a
 *  hand-written scan kept getting wrong: a fence inside a list item or a quote
 *  is written past the three-space limit its own rule is measured against, and
 *  the indent that proves it is a container is unreadable without tracking the
 *  container. The parser already tracks them, so the ranges come back right
 *  for a fence, an indented block, and either one nested in anything else.
 */
function codeRanges(root: Positioned): Array<[number, number]> {
  const ranges: Array<[number, number]> = [];
  const walk = (nodes: Positioned[]): void => {
    for (const node of nodes) {
      const start = node.position?.start.offset;
      const end = node.position?.end.offset;
      // A code or display-math block has no block-level children to descend into.
      if ((node.type === 'code' || node.type === 'math') && start !== undefined && end !== undefined) {
        ranges.push([start, end]);
      }
      else if (node.children) walk(node.children);
    }
  };
  walk(root.children ?? []);
  return ranges;
}

/** A line with its blockquote markers removed.
 *
 *  CommonMark separates two paragraphs inside a blockquote with a marker-only
 *  line (`>` or `> `), and a reply that opens with a quote has no boundary
 *  anywhere in it until that line is read as the break it is. */
function unquoted(line: string): string {
  let content = line;
  for (let match = /^ {0,3}> ?/.exec(content); match; match = /^ {0,3}> ?/.exec(content)) {
    content = content.slice(match[0].length);
  }
  return content;
}

// One streaming block is asked about several times per render (its body, its
// wrapper, the bubble's waiting indicator), always with the same string, so the
// last answer is kept rather than parsing the whole reply again for each.
let last: { text: string; maxHeld: number; prefix: string } | null = null;

/**
 * The part of a streaming reply that is safe to paint: everything up to the
 * last blank line that is not inside a code block.
 *
 * Holding the tail is the whole point of paragraph mode, but a blank line
 * *inside* a block is ordinary code, and cutting there would hand the markdown
 * renderer an unterminated block and make it flicker between code and prose on
 * every chunk. The block boundaries come from the same parser the renderer
 * runs on the result, so what counts as safe here is decided by the thing that
 * has to render it rather than by a second, approximate reading of the syntax.
 * Past `maxHeld` the gate opens rather than sit on a paragraph the reader is
 * waiting for.
 */
export function visibleParagraphPrefix(text: string, maxHeld = MAX_HELD_CHARS): string {
  if (!text) return '';
  if (last && last.text === text && last.maxHeld === maxHeld) return last.prefix;
  const prefix = computePrefix(text, maxHeld);
  last = { text, maxHeld, prefix };
  return prefix;
}

function computePrefix(text: string, maxHeld: number): string {
  // Ranges come back in source order and lines are read in order, so one
  // cursor walks both instead of testing every line against every block.
  // The renderer reads display math too (remark-math), and rewrites `\[...\]`
  // to `$$...$$` before it does. That rewrite is the same length, so the
  // offsets of this parse still index the original text.
  const tree = fromMarkdown(text.replace(/\\\[([\s\S]*?)\\\]/g, (_, body: string) => `$$${body}$$`), {
    extensions: [math()],
    mdastExtensions: [mathFromMarkdown()],
  });
  const ranges = codeRanges(tree as unknown as Positioned);
  let next = 0;

  let boundary = -1; // index just past the newline that ends the last safe blank line
  let pos = 0;
  for (;;) {
    const newline = text.indexOf('\n', pos);
    if (newline === -1) break;
    while (next < ranges.length && ranges[next][1] <= pos) next++;
    const insideCode = next < ranges.length && pos > ranges[next][0] && pos < ranges[next][1];
    if (!insideCode && unquoted(text.slice(pos, newline)).trim() === '') boundary = newline + 1;
    pos = newline + 1;
  }

  if (boundary < 0) return text.length > maxHeld ? text : '';
  return text.length - boundary > maxHeld ? text : text.slice(0, boundary);
}
