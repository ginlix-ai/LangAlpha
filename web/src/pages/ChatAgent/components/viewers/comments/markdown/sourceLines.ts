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

/**
 * The rehype pass that writes each block's lines in the file, `fileLine`
 * mapping a line of the parsed text to the file's (both 1-based). Run after
 * the sanitizer, which keeps positions and would drop the attribute.
 */
export function rehypeSourceLines(fileLine: (line: number) => number) {
  const visit = (node: Root | Element) => {
    node.children.forEach((child, i) => {
      if (child.type !== 'element') return;
      const start = child.position?.start.line;
      let end = child.position?.end.line;
      if (BLOCK_TAGS.has(child.tagName) && start && end) {
        // An item's own lines stop where a list inside it starts: its sub-items are blocks of their own.
        if (child.tagName === 'li') {
          const sub = child.children.find((c): c is Element => c.type === 'element' && (c.tagName === 'ul' || c.tagName === 'ol'));
          const subStart = sub?.position?.start.line;
          if (subStart && subStart > start) end = subStart - 1;
        }
        const dataSourceLines = linesText(fileLine(start), fileLine(end));
        // A code block draws its own frame, which takes no attribute: the lines go on a box around it.
        if (child.tagName === 'pre') node.children[i] = { type: 'element', tagName: 'div', properties: { dataSourceLines }, children: [child] };
        else child.properties = { ...child.properties, dataSourceLines };
      }
      visit(child);
    });
  };
  return (tree: Root) => visit(tree);
}
