import type { Element, ElementContent, Root, Text } from 'hast';
import { matchSpans } from '@/lib/textMatch';
import { OPAQUE, rewriteChildren } from './hast';

// KaTeX's visible half is the renderer's own markup, drawn from the source
// it was given; a mark inside it would break the layout it computed.
const isOpaque = (el: Element): boolean => {
  const cls = el.properties.className;
  return OPAQUE.has(el.tagName) || (Array.isArray(cls) && cls.some((c) => typeof c === 'string' && c.startsWith('katex')));
};

function mark(node: Text, words: readonly string[]): ElementContent[] | null {
  const spans = matchSpans(node.value, words);
  if (spans.length === 0) return null;
  const pieces: ElementContent[] = [];
  let at = 0;
  for (const [from, to] of spans) {
    if (from > at) pieces.push({ type: 'text', value: node.value.slice(at, from) });
    pieces.push({ type: 'element', tagName: 'mark', properties: { dataMatch: '' }, children: [{ type: 'text', value: node.value.slice(from, to) }] });
    at = to;
  }
  if (at < node.value.length) pieces.push({ type: 'text', value: node.value.slice(at) });
  return pieces;
}

/**
 * Marks `words` where they appear in the text, case folded as the search
 * folds them, as `<mark>` elements the tree itself carries: React renders
 * them, so nothing edits the DOM behind its back. Each carries `data-match`,
 * which the sanitizer strips from an authored `<mark>`, so a reader can tell
 * a match from the text's own highlight.
 */
export function rehypeMarkWords(words: readonly string[]) {
  const walk = (parent: Root | Element) =>
    rewriteChildren(parent, (child) => {
      if (child.type === 'text') return mark(child, words);
      if (child.type === 'element' && !isOpaque(child)) walk(child);
      return null;
    });
  return (tree: Root) => {
    if (words.length) walk(tree);
  };
}
