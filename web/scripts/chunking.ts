import type { Rolldown } from 'vite';

// Vendors get a pinned chunk so app deploys don't re-invalidate them.
//
// The priorities are load-bearing. A group also captures every
// dependency of what it matches, so recharts would pull React,
// `clsx` and `use-sync-external-store` into vendor-charts, and the
// entry would then have to preload that whole chunk to reach them.
// That is how 170 kB of charts sat on the critical path for five
// months. The eager groups claim first so the lazy vendors stay lazy.
// Enforced by scripts/check-critical-path.mjs.
//
// The `$initial` group is everything the entry reaches statically.
// Without it Rolldown splits the eager code shared with lazy routes
// into ~100 small common chunks, each one more request on first load.
// It ranks above the lazy vendors so sharing a dependency with them
// can never make them eager.

// Shared by the entry and the lazy vendors.
const EAGER_SHARED = new Set(['clsx', 'use-sync-external-store']);
const REACT = new Set(['react', 'react-dom', 'react-router', ...EAGER_SHARED]);
const MARKDOWN = new Set([
  'react-markdown', 'remark-gfm', 'remark-math', 'remark-cjk-friendly',
  'rehype-katex', 'rehype-raw', 'katex',
]);
const CHARTS = new Set(['recharts', 'lightweight-charts']);

/** The npm package a module id belongs to, or null for app source. */
function packageOf(id: string): string | null {
  const tail = id.split(/[\\/]node_modules[\\/]/);
  if (tail.length < 2) return null;
  const [scope, name] = tail[tail.length - 1].split(/[\\/]/);
  return scope.startsWith('@') ? `${scope}/${name}` : scope;
}

const vendorGroup = (
  name: string,
  priority: number,
  matches: (pkg: string) => boolean,
): Rolldown.CodeSplittingGroup => ({
  name,
  priority,
  test: (id: string) => {
    const pkg = packageOf(id);
    return pkg !== null && matches(pkg);
  },
});

export const criticalPathGroups: Rolldown.CodeSplittingGroup[] = [
  vendorGroup('vendor-react', 50, (pkg) => REACT.has(pkg)),
  vendorGroup('vendor-motion', 40, (pkg) => pkg === 'framer-motion'),
  vendorGroup('vendor-dnd', 30, (pkg) => pkg.startsWith('@dnd-kit/')),
  { name: 'index', priority: 25, tags: ['$initial'] },
  vendorGroup('vendor-markdown', 20, (pkg) => MARKDOWN.has(pkg)),
  vendorGroup('vendor-charts', 10, (pkg) => CHARTS.has(pkg)),
];
