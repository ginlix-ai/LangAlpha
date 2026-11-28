/**
 * The file's own line numbers on rendered Markdown blocks, which a comment on
 * a block names to the agent. The renderer parses a prepared copy of the
 * file block by block, and says which line of the file each parsed line came
 * from (`Markdown`'s `rehypeForBlock`).
 */
import type { Element, Root } from 'hast';
import { linesText } from '../spotKit';

/** The attribute a block's lines are written to, "7" or "14-15", the file's 1-based lines. */
export const SOURCE_LINES_ATTR = 'data-source-lines';

/** The blocks a comment can pick: what a reader calls a paragraph, a heading, an item, a row, a quote or a code block. */
const BLOCK_TAGS = new Set(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'tr', 'blockquote', 'pre']);

/** A block's lines, or null for an element that is no block or has no position. */
function blockLines(el: Element, fileLine: (line: number) => number): string | null {
  const start = el.position?.start.line;
  let end = el.position?.end.line;
  if (!BLOCK_TAGS.has(el.tagName) || !start || !end) return null;
  // An item's own lines stop where a list inside it starts: its sub-items are blocks of their own.
  if (el.tagName === 'li') {
    const sub = el.children.find((c): c is Element => c.type === 'element' && (c.tagName === 'ul' || c.tagName === 'ol'));
    const subStart = sub?.position?.start.line;
    if (subStart && subStart > start) end = subStart - 1;
  }
  return linesText(fileLine(start), fileLine(end));
}

/**
 * The rehype pass that writes each block's lines in the file, `fileLine`
 * mapping a line of the parsed text to the file's (both 1-based). Run after
 * the sanitizer, which keeps positions and would drop the attribute.
 */
export function rehypeSourceLines(fileLine: (line: number) => number) {
  const visit = (node: Root | Element, inBlock: boolean) => {
    node.children.forEach((child, i) => {
      // Words no block holds (the text of a raw HTML block, whose tag the
      // sanitizer dropped or that no reader calls a block) still have their
      // lines: they go on a span around the words, past the line breaks on
      // either side, so words picked there are saved by where they are.
      if (child.type === 'text') {
        const at = child.position;
        if (inBlock || !at || !child.value.trim()) return;
        const lead = (/^\s*/.exec(child.value)?.[0].split('\n').length ?? 1) - 1;
        const trail = (/\s*$/.exec(child.value)?.[0].split('\n').length ?? 1) - 1;
        const dataSourceLines = linesText(fileLine(at.start.line + lead), fileLine(at.end.line - trail));
        node.children[i] = { type: 'element', tagName: 'span', properties: { dataSourceLines }, children: [child] };
        return;
      }
      if (child.type !== 'element') return;
      const dataSourceLines = blockLines(child, fileLine);
      if (dataSourceLines) {
        // A code block draws its own frame, which takes no attribute: the lines go on a box around it.
        if (child.tagName === 'pre') node.children[i] = { type: 'element', tagName: 'div', properties: { dataSourceLines }, children: [child] };
        else child.properties = { ...child.properties, dataSourceLines };
      }
      visit(child, inBlock || dataSourceLines !== null);
    });
  };
  return (tree: Root) => visit(tree, false);
}
