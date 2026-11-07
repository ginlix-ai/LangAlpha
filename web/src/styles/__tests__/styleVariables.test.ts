// @vitest-environment node
import { readdirSync, readFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * A card style variable is declared only under the styles that set it
 * (styles/cardStyle.css). Read elsewhere with no fallback it computes to
 * `unset`, and that still wins the cascade over every lower rule, so the
 * element silently loses its own value. This finds which styles declare each
 * variable from the style blocks themselves, then holds every read under
 * src to a fallback unless the rule it sits in is scoped to styles that all
 * declare it.
 */

const SRC = resolve(__dirname, '../..');
type Style = 'terminal' | 'soft' | 'balanced';
const ALL: Style[] = ['terminal', 'soft', 'balanced'];

/** The style scopes a rule can open with (cardStyle.css's shorthands). */
const SCOPES: Record<string, Style[]> = {
  ':root:not([data-card-style])': ['terminal'],
  ":root[data-card-style='soft']": ['soft'],
  ":root[data-card-style='balanced']": ['balanced'],
  ":root:not([data-card-style='balanced'])": ['terminal', 'soft'],
  ":root:is([data-card-style='soft'], [data-card-style='balanced'])": ['soft', 'balanced'],
};

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== 'node_modules' && entry.name !== '__tests__') walk(path, out);
    } else if (/\.(css|ts|tsx)$/.test(entry.name)) {
      out.push(path);
    }
  }
  return out;
}

/** Splits at commas outside parentheses and brackets. */
function splitList(text: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < text.length; i++) {
    if ('(['.includes(text[i])) depth++;
    else if (')]'.includes(text[i])) depth--;
    else if (text[i] === ',' && depth === 0) {
      parts.push(text.slice(start, i).trim());
      start = i + 1;
    }
  }
  parts.push(text.slice(start).trim());
  return parts;
}

/** The styles one selector applies in, and whether it is the scope alone. */
function scopeOf(selector: string): { styles: Style[]; bare: boolean } {
  const compound = selector.match(/^:root(?:\[[^\]]*\]|:[\w-]+\((?:[^()]|\([^()]*\))*\))*/)?.[0] ?? '';
  const key = compound.replace(/\[data-theme=['"]?\w+['"]?\]/, '');
  if (!key.includes('data-card-style')) return { styles: ALL, bare: selector === compound };
  const styles = SCOPES[key];
  if (!styles) throw new Error(`unknown style scope: ${compound}`);
  return { styles, bare: selector === compound };
}

interface Rule {
  selectors: string[];
  declarations: string[];
  line: number;
}

/** Every block that holds declarations, with the selectors around it. */
function parseCss(text: string): Rule[] {
  const css = text.replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, ' '));
  const rules: Rule[] = [];
  const stack: { prelude: string; declarations: string[]; line: number }[] = [];
  let buffer = '';
  let line = 1;
  for (const ch of css) {
    if (ch === '\n') line++;
    if (ch === '{') {
      stack.push({ prelude: buffer.trim(), declarations: [], line });
      buffer = '';
    } else if (ch === ';' || ch === '}') {
      if (buffer.trim() && stack.length) stack[stack.length - 1].declarations.push(buffer.trim());
      buffer = '';
      if (ch === '}') {
        const block = stack.pop();
        if (block) {
          const selectors = [...stack.map((b) => b.prelude), block.prelude].filter((p) => !p.startsWith('@'));
          rules.push({ selectors, declarations: block.declarations, line: block.line });
        }
      }
    } else {
      buffer += ch;
    }
  }
  return rules;
}

/** The styles a rule applies in: each selector list's union, nested ones intersected. */
function stylesOf(rule: Rule): Style[] {
  return rule.selectors.reduce<Style[]>((styles, list) => {
    const own = new Set(splitList(list).flatMap((s) => scopeOf(s).styles));
    return styles.filter((s) => own.has(s));
  }, ALL);
}

const files = walk(SRC);
const sheets = files.filter((f) => f.endsWith('.css')).map((f) => ({ file: f, rules: parseCss(readFileSync(f, 'utf8')) }));

/** Which styles declare each variable that some style declares on the root. */
function styleDeclarations(): Map<string, Set<Style>> {
  const declared = new Map<string, Set<Style>>();
  for (const { rules } of sheets) {
    for (const rule of rules) {
      if (rule.selectors.length !== 1) continue;
      for (const selector of splitList(rule.selectors[0])) {
        const { styles, bare } = scopeOf(selector);
        if (!bare || !selector.startsWith(':root')) continue;
        for (const decl of rule.declarations) {
          const name = decl.match(/^(--[\w-]+)\s*:/)?.[1];
          if (!name) continue;
          const set = declared.get(name) ?? new Set<Style>();
          styles.forEach((s) => set.add(s));
          declared.set(name, set);
        }
      }
    }
  }
  return declared;
}

const declared = styleDeclarations();
/** Declared by some styles and not others: the ones a bare read can miss. */
const styleOnly = new Map([...declared].filter(([, styles]) => styles.size < ALL.length));

const READ = /var\(\s*(--[\w-]+)\s*(,?)/g;
// Tailwind's shorthand for a read: `bg-(--x)`, `font-(family-name:--x)`.
const TAILWIND_READ = /-\((?:[\w-]+:)?(--[\w-]+)\s*(,?)/g;
// A custom property set from a read (`[--x:var(--y)]`, `'--x': 'var(--y)'`):
// the read makes it invalid where the variable is undeclared, which hands its
// own readers their fallbacks.
const SETS_PROPERTY = /(?:\[--[\w-]+:|['"]--[\w-]+['"]\s*:\s*['"`])$/;

describe('card style variables', () => {
  it('finds the styles each variable is declared in', () => {
    // Guards the parse itself: a broken scope table or block parse would
    // otherwise pass every read.
    expect([...declared.get('--cs-face') ?? []].sort()).toEqual(['soft', 'terminal']);
    expect([...declared.get('--color-shell-label') ?? []]).toEqual(['terminal']);
    expect([...declared.get('--term-mark') ?? []].sort()).toEqual(['balanced', 'soft']);
    expect(styleOnly.has('--term-case')).toBe(false);
  });

  it('reads a variable without a fallback only where every style in scope declares it', () => {
    const bare: string[] = [];
    const check = (name: string, fallback: string, where: string, styles: Style[]) => {
      const declaredIn = styleOnly.get(name);
      if (!declaredIn || fallback) return;
      if (styles.every((s) => declaredIn.has(s))) return;
      bare.push(`${where} → ${name} (declared in ${[...declaredIn].join(', ')})`);
    };
    for (const { file, rules } of sheets) {
      for (const rule of rules) {
        const styles = stylesOf(rule);
        for (const decl of rule.declarations) {
          if (decl.startsWith('--')) continue;
          for (const [, name, fallback] of decl.matchAll(READ)) {
            check(name, fallback, `${relative(SRC, file)}:${rule.line}`, styles);
          }
        }
      }
    }
    for (const file of files.filter((f) => !f.endsWith('.css'))) {
      readFileSync(file, 'utf8').split('\n').forEach((line, i) => {
        for (const pattern of [READ, TAILWIND_READ]) {
          for (const match of line.matchAll(pattern)) {
            if (SETS_PROPERTY.test(line.slice(0, match.index))) continue;
            check(match[1], match[2], `${relative(SRC, file)}:${i + 1}`, ALL);
          }
        }
      });
    }
    expect(bare).toEqual([]);
  });
});
