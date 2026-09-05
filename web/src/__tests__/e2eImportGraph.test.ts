import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync, existsSync, statSync } from 'node:fs';
import path from 'node:path';

const WEB = path.resolve(__dirname, '../..');
const SRC = path.join(WEB, 'src');

/**
 * Playwright transforms a spec and everything it imports with its own pipeline,
 * which has no CSS loader. So a spec that reaches app source pays for that
 * module's whole import graph, and a stylesheet anywhere in it is a parse error
 * that takes down the entire e2e run at collection, not one test.
 *
 * `printPageStyle.ts` reached `lib/shellPdf.ts`, which imports printExport.css
 * for its side effect. That is why the page geometry lives alone in
 * `lib/pageGeometry.ts` now, and why this walks the graph rather than pinning
 * that one file: the next import added anywhere along the chain is what breaks
 * it, and a red e2e job says only "Unexpected token".
 */
const EXTS = ['', '.ts', '.tsx', '.js', '.jsx', '/index.ts', '/index.tsx'];

function resolveSpecifier(spec: string, fromFile: string): string | null {
  let base: string;
  if (spec.startsWith('@/')) base = path.join(SRC, spec.slice(2));
  else if (spec.startsWith('.')) base = path.resolve(path.dirname(fromFile), spec);
  else return null; // bare package: Playwright leaves node_modules alone
  for (const ext of EXTS) {
    const candidate = base + ext;
    if (existsSync(candidate) && statSync(candidate).isFile()) return candidate;
  }
  return null;
}

// `export ... from` as well as `import`: a re-export is followed at transform
// time exactly like an import, and reading only one of the two would let the
// chain be rebuilt through the other with the guard still green.
const SPECIFIER = /^\s*(?:import|export)\s[^'"]*?from\s*['"]([^'"]+)['"]|^\s*import\s*['"]([^'"]+)['"]/gm;

function importsOf(file: string): string[] {
  const text = readFileSync(file, 'utf8');
  return [...text.matchAll(SPECIFIER)].map((m) => m[1] ?? m[2]);
}

describe('what an e2e spec drags in when it imports app source', () => {
  it('never reaches a stylesheet', () => {
    const specs = readdirSync(path.join(WEB, 'e2e'))
      .filter((f) => f.endsWith('.spec.js'))
      .map((f) => path.join(WEB, 'e2e', f));

    const offenders: string[] = [];
    const seen = new Set<string>();
    const walk = (file: string, trail: string[]) => {
      if (seen.has(file)) return;
      seen.add(file);
      for (const spec of importsOf(file)) {
        if (/\.(css|scss|sass|less)$/.test(spec)) {
          offenders.push([...trail, file, spec].map((p) => path.relative(WEB, p)).join(' -> '));
          continue;
        }
        const next = resolveSpecifier(spec, file);
        if (next) walk(next, [...trail, file]);
      }
    };
    for (const spec of specs) walk(spec, []);

    expect(specs.length).toBeGreaterThan(0);
    expect(offenders).toEqual([]);
  });
});
