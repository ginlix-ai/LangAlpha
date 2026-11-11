import type { Element, ElementContent, Root } from 'hast';
import { OPAQUE, rewriteChildren } from '@/pages/ChatAgent/utils/hast';

const standIn = (className: string, children: ElementContent[]): Element[] =>
  children.length === 0 ? [] : [{ type: 'element', tagName: 'span', properties: { className: [className] }, children }];

const textOf = (value: unknown): ElementContent[] => (typeof value === 'string' && value.trim() ? [{ type: 'text', value }] : []);

function rework(el: Element): Element[] | null {
  switch (el.tagName) {
    case 'img':
      return standIn('command-palette-pane-alt', textOf(el.properties.alt));
    case 'a':
      walk(el);
      return standIn('command-palette-pane-link', el.children);
    case 'cite-bubble':
      return standIn('command-palette-pane-cite', textOf(el.properties.label));
    default:
      // Into an <svg> too: the parser keeps an <a> inside one, and the
      // sanitizer lets it through with its href.
      if (!OPAQUE.has(el.tagName) || el.tagName === 'svg') walk(el);
      return null;
  }
}

function walk(parent: Root | Element) {
  rewriteChildren(parent, (child) => (child.type === 'element' ? rework(child) : null));
}

/**
 * A turn as the palette's preview pane shows it, run after the sanitizer. A
 * link is its text: the pane is a glance while the arrows move, and a click
 * there would leave the palette. An image is its alt text, since a
 * workspace's files are read through a workspace the pane has none of. A
 * cited source is its label.
 */
export function rehypePreview() {
  return (tree: Root) => walk(tree);
}
