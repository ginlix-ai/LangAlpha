// @vitest-environment node
import { readdirSync, readFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * The motion vocabulary is `lib/motion.ts`, mirrored for CSS as `--motion-*`
 * and `--ease-*` in tokens.css. A stylesheet that writes its own duration or
 * curve starts a second vocabulary: before this test there were eight
 * durations for "a small thing appears" and two entrance curves. ESLint's
 * `no-restricted-syntax` holds the same line for the scripts.
 */

const SRC = resolve(__dirname, '../..');

/** The two stylesheets that define the vocabulary rather than use it. */
const DEFINES = new Set(['styles/tokens.css', 'styles/animations.css']);

/** A comment carrying this, on the line or ending on the line before, exempts
 *  that line, and says why: `/* motion-exempt: the loop's period *\/`. */
const EXEMPT = 'motion-exempt:';

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== 'node_modules') walk(path, out);
    } else if (entry.name.endsWith('.css')) {
      out.push(path);
    }
  }
  return out;
}

const lineAt = (text: string, index: number) => text.slice(0, index).split('\n').length;

/** Every violation in one stylesheet, as `line → what`. */
function motionViolations(css: string): string[] {
  // Comments blanked to spaces, so indices and line numbers still line up.
  const exemptLines = new Set<number>();
  const code = css.replace(/\/\*[\s\S]*?\*\//g, (comment, index: number) => {
    if (comment.includes(EXEMPT)) {
      const end = lineAt(css, index + comment.length);
      exemptLines.add(end);
      exemptLines.add(end + 1);
    }
    return comment.replace(/[^\n]/g, ' ');
  });
  const found: string[] = [];
  const report = (index: number, what: string) => {
    const line = lineAt(code, index);
    if (!exemptLines.has(line)) found.push(`${line} → ${what}`);
  };

  for (const m of code.matchAll(/cubic-bezier\(/g)) report(m.index, 'cubic-bezier()');

  const declaration = /(?<![\w-])(transition|animation)(-duration|-delay|-timing-function)?\s*:\s*([^;{}]*)/g;
  for (const m of code.matchAll(declaration)) {
    // A token (`var(--motion-quick)`) is the point; a fallback inside one
    // is still a literal, so only a bare reference is set aside.
    const value = m[3].replace(/var\(\s*--[\w-]+\s*\)/g, '');
    for (const t of value.matchAll(/(?<![\w.-])(\d*\.?\d+)(m?s)\b/g)) {
      if (Number(t[1]) !== 0) report(m.index, `${m[1]}${m[2] ?? ''}: ${t[0]}`);
    }
    for (const k of value.matchAll(/(?<![\w-])ease(-in-out|-in|-out)?(?![\w-])/g)) {
      report(m.index, `${m[1]}${m[2] ?? ''}: ${k[0]}`);
    }
  }
  return found;
}

describe('motion tokens', () => {
  it('reads a stylesheet the way the rule means', () => {
    // Guards the parse: a regex that matched nothing would pass every file.
    expect(motionViolations('a { transition: opacity 150ms ease; }')).toEqual([
      '1 → transition: 150ms',
      '1 → transition: ease',
    ]);
    expect(motionViolations('a {\n  animation:\n    spin 1.2s cubic-bezier(0.4, 0, 0.2, 1) infinite;\n}')).toEqual([
      '3 → cubic-bezier()',
      '2 → animation: 1.2s',
    ]);
    expect(motionViolations('a { transition: opacity var(--motion-quick) var(--ease-out), scale 0s; }')).toEqual([]);
    expect(motionViolations('a { transition-delay: var(--x, 80ms); }')).toEqual(['1 → transition-delay: 80ms']);
    expect(motionViolations('/* motion-exempt: a loop period */\na { animation: spin 1s linear infinite; }')).toEqual([]);
    expect(motionViolations('a { animation: spin 1s linear infinite; } /* motion-exempt: loop */')).toEqual([]);
    expect(motionViolations('a { --pill-transition: 1s; transition-property: opacity; }')).toEqual([]);
  });

  it('keeps durations and curves in tokens.css', () => {
    const offenders: string[] = [];
    for (const file of walk(SRC)) {
      const rel = relative(SRC, file);
      if (DEFINES.has(rel)) continue;
      for (const v of motionViolations(readFileSync(file, 'utf8'))) offenders.push(`${rel}:${v}`);
    }
    expect(offenders).toEqual([]);
  });

  it('mirrors lib/motion.ts in tokens.css', async () => {
    const { DURATION, EASE_OUT, EASE_REVEAL, EASE_COLLAPSE, STAGGER, PRESS_SCALE } = await import('@/lib/motion');
    const tokens = readFileSync(join(SRC, 'styles/tokens.css'), 'utf8');
    const declared = (name: string) => tokens.match(new RegExp(`${name}:\\s*([^;]+);`))?.[1].trim();
    for (const [name, seconds] of Object.entries(DURATION)) {
      expect(declared(`--motion-${name}`), name).toBe(`${Math.round(seconds * 1000)}ms`);
    }
    expect(declared('--motion-stagger')).toBe(`${Math.round(STAGGER * 1000)}ms`);
    expect(declared('--press-scale')).toBe(String(PRESS_SCALE));
    const bezier = (curve: readonly number[]) => `cubic-bezier(${curve.join(', ')})`;
    expect(declared('--ease-out')).toBe(bezier(EASE_OUT));
    expect(declared('--ease-reveal')).toBe(bezier(EASE_REVEAL));
    expect(declared('--ease-collapse')).toBe(bezier(EASE_COLLAPSE));
  });
});
