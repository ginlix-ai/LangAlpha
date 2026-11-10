/** Helpers shared by the rehype passes that rework a rendered turn's tree. */
import type { Element, ElementContent, Root, RootContent } from 'hast';

/**
 * Never descended into by a pass over prose: a fence's <pre> is drawn from
 * its text (CodeBlock reads its children as a string, so an element inside
 * would print as an object), and the rest hold none.
 */
export const OPAQUE: ReadonlySet<string> = new Set(['pre', 'script', 'style', 'svg', 'math', 'textarea']);

/** What `rewriteChildren` gets back for a child: its replacements, or null to keep it. */
export type Rewrite = (child: RootContent, index: number) => ElementContent[] | null;

function rewritten<C extends RootContent>(children: C[], fn: (child: C, index: number) => C[] | null): C[] {
  let out: C[] | null = null;
  children.forEach((child, i) => {
    const replaced = fn(child, i);
    if (replaced && !out) out = children.slice(0, i);
    if (out) out.push(...(replaced ?? [child]));
  });
  return out ?? children;
}

/**
 * Replaces each child of `parent` `fn` returns replacements for, in one
 * pass. The array is copied only from the first replacement on, so a parent
 * with nothing to change keeps its own.
 */
export function rewriteChildren(parent: Root | Element, fn: Rewrite): void {
  if (parent.type === 'root') parent.children = rewritten(parent.children, fn);
  else parent.children = rewritten(parent.children, fn);
}
