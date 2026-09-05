import { readdirSync, readFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * A reference to a `--color-*` name no theme declares silently renders as
 * nothing — or as its inline fallback, which then never tracks the theme.
 * Fifteen of those had accumulated before this test existed.
 */

const SRC = resolve(__dirname, '../..');
const TOKENS = join(SRC, 'styles/tokens.css');

/**
 * `--color-*` names stamped from JS or by a library rather than declared in
 * tokens.css. Empty today: the other JS-stamped custom properties in the app
 * (`--app-font-scale` from index.html, `--sidebar-width`, Radix's
 * `--radix-*`) are not `--color-*` and so fall outside the scan already.
 */
const JS_STAMPED = new Set<string>();
const JS_STAMPED_PREFIXES = ['--radix-'];

const SOURCE_EXTENSIONS = ['.ts', '.tsx', '.js', '.jsx', '.css'];

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== 'node_modules') walk(path, out);
    } else if (SOURCE_EXTENSIONS.some((ext) => entry.name.endsWith(ext))) {
      out.push(path);
    }
  }
  return out;
}

function isAllowlisted(name: string): boolean {
  return JS_STAMPED.has(name) || JS_STAMPED_PREFIXES.some((p) => name.startsWith(p));
}

describe('color token references', () => {
  const declared = new Set(
    [...readFileSync(TOKENS, 'utf8').matchAll(/(--color-[\w-]+)\s*:/g)].map((m) => m[1]),
  );

  it('declares tokens in both themes', () => {
    // Guards the parse itself: a rename that broke the regex would otherwise
    // make every reference "undeclared" or every one "declared".
    expect(declared.size).toBeGreaterThan(50);
    expect(declared.has('--color-bg-page')).toBe(true);
    expect(declared.has('--color-bg-popover')).toBe(true);
  });

  it('has no reference to an undeclared --color-* token', () => {
    const orphans: string[] = [];
    for (const file of walk(SRC)) {
      if (file === TOKENS) continue;
      const lines = readFileSync(file, 'utf8').split('\n');
      lines.forEach((line, i) => {
        for (const match of line.matchAll(/var\((--color-[\w-]+)/g)) {
          const name = match[1];
          if (declared.has(name) || isAllowlisted(name)) continue;
          orphans.push(`${relative(SRC, file)}:${i + 1} → ${name}`);
        }
      });
    }
    expect(orphans).toEqual([]);
  });
});
