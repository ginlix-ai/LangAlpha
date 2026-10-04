// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import enUS from '../en-US.json';
import zhCN from '../zh-CN.json';

const REPO_ROOT = resolve(__dirname, '..', '..', '..');
// Sweep the whole source tree: any statically-written locale key anywhere in
// the app must resolve in both catalogs. (This started Dashboard-only; the
// chat surface grew its own keys and drifted silently.)
const SRC_DIR = resolve(REPO_ROOT, 'src');

// A captured string counts as a locale key only when its first segment is a
// real top-level catalog namespace — that keeps dotted non-keys the regexes
// can also match ('settings.json', module paths) out of the report.
const NAMESPACES = new Set(Object.keys(enUS as Record<string, unknown>));
function isLocaleKey(candidate: string): boolean {
  const dot = candidate.indexOf('.');
  return dot > 0 && NAMESPACES.has(candidate.slice(0, dot));
}

// Match `t('foo.bar.baz')` and `t("foo.bar.baz")` — the second arg form for
// interpolation is fine because we only capture the first quoted argument.
const T_CALL = /\bt\(\s*['"]([a-zA-Z0-9_.]+)['"]/g;
// Match `titleKey: 'foo.bar'` / `descriptionKey: 'foo.bar'` / etc. — keys
// stored in widget definitions, STATUS_UI tables, and PresetMeta. Catches our
// static metadata references that aren't wrapped in t(). A bare `key:` is a
// locale key too (freshness specs, table columns); a React `key` never names
// a catalog namespace, so `isLocaleKey` keeps it out.
const KEY_PROP = /\b(?:key|titleKey|descriptionKey|nameKey|tagKey|bestForKey|labelKey|blurbKey|messageKey|noteKey|bodyKey|descKey)\s*[:=]\s*['"]([a-zA-Z0-9_.]+)['"]/g;
// Bare quoted keys held in const maps and passed to a helper rather than to
// `t()` directly: SOURCE_KEY / BUCKET_KEY on the dashboard, the tab-label map
// and the skill-action failure helper under plugins, and the probe verdict
// tables under mcp, and the status, group and delivery tables under
// automation. Scoped to those namespaces rather than swept tree-wide,
// because `isLocaleKey` only checks
// the first segment and plenty of dotted non-keys (module paths, filenames)
// would otherwise qualify. The cost of being in this list is that a namespace
// here may not also be used for storage keys or other dotted identifiers --
// see `plugins:deckExpanded`, which uses a colon for exactly that reason.
const KEY_VALUE = /['"]((?:dashboard|plugins|orders|mcp|automation|timezone)\.[a-zA-Z0-9_.]+)['"]/g;

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      if (entry === '__tests__' || entry === 'node_modules') continue;
      walk(full, out);
      continue;
    }
    if (entry.endsWith('.ts') || entry.endsWith('.tsx')) {
      out.push(full);
    }
  }
  return out;
}

function lookup(obj: unknown, key: string): unknown {
  return key.split('.').reduce<unknown>((acc, part) => {
    if (acc && typeof acc === 'object' && part in (acc as object)) {
      return (acc as Record<string, unknown>)[part];
    }
    return undefined;
  }, obj);
}

// i18next falls back to plural-suffixed variants (`_one` / `_other` /
// `_zero`) when the bare key is absent and the call passes `count`, and to
// `_ordinal_*` ones when it also passes `ordinal: true`. A test that only
// checks the bare key would falsely flag those plural-only entries.
const PLURAL_SUFFIXES = ['', '_one', '_other', '_zero', '_two', '_few', '_many', '_ordinal_other'];
function resolveAnyVariant(obj: unknown, key: string): boolean {
  for (const suffix of PLURAL_SUFFIXES) {
    if (typeof lookup(obj, key + suffix) === 'string') return true;
  }
  return false;
}

function collectKeys(): { keys: Set<string>; perFile: Map<string, string[]> } {
  const files = walk(SRC_DIR);
  const keys = new Set<string>();
  const perFile = new Map<string, string[]>();
  for (const file of files) {
    const src = readFileSync(file, 'utf8');
    const found: string[] = [];
    for (const re of [T_CALL, KEY_PROP, KEY_VALUE]) {
      re.lastIndex = 0;
      let m: RegExpExecArray | null;
      while ((m = re.exec(src)) !== null) {
        if (isLocaleKey(m[1])) {
          keys.add(m[1]);
          found.push(m[1]);
        }
      }
    }
    if (found.length > 0) perFile.set(file, found);
  }
  return { keys, perFile };
}

// ── Interpolation ─────────────────────────────────────────
//
// A `{{var}}` the call does not pass renders literally ("No quote for
// {{symbol}}"), and nothing else catches it: the key resolves, the types
// pass. So for each `t('key', …)` whose options are an object literal, every
// variable in the key's strings (each plural variant, both catalogs) has to
// be among the names the literal passes. A call whose options are a variable
// or carry a spread is skipped, since what it passes is not in the text.

const VAR = /\{\{\s*-?\s*([A-Za-z_$][\w$]*)/g;

function stringVariants(obj: unknown, key: string): string[] {
  return PLURAL_SUFFIXES.map((suffix) => lookup(obj, key + suffix)).filter((v): v is string => typeof v === 'string');
}

/** The source from `start` (an opening bracket) to its match, skipping
 *  brackets inside string and template literals. */
function balanced(src: string, start: number): string | null {
  const open = src[start];
  const close = open === '{' ? '}' : ')';
  let depth = 0;
  let quote: string | null = null;
  for (let i = start; i < src.length; i++) {
    const c = src[i];
    if (quote) {
      if (c === '\\') i++;
      else if (c === quote) quote = null;
      continue;
    }
    if (c === "'" || c === '"' || c === '`') quote = c;
    else if (c === '{' || c === '(' || c === '[') depth++;
    else if (c === '}' || c === ')' || c === ']') {
      depth--;
      if (depth === 0) return c === close ? src.slice(start + 1, i) : null;
    }
  }
  return null;
}

/** The top-level property names of an object literal's body, or null when a
 *  spread makes them unknowable. */
function literalNames(body: string): Set<string> | null {
  const names = new Set<string>();
  let depth = 0;
  let quote: string | null = null;
  let part = '';
  const parts: string[] = [];
  for (let i = 0; i < body.length; i++) {
    const c = body[i];
    if (quote) {
      part += c;
      if (c === '\\') part += body[++i] ?? '';
      else if (c === quote) quote = null;
      continue;
    }
    if (c === "'" || c === '"' || c === '`') quote = c;
    else if (c === '{' || c === '(' || c === '[') depth++;
    else if (c === '}' || c === ')' || c === ']') depth--;
    if (c === ',' && depth === 0) {
      parts.push(part);
      part = '';
    } else part += c;
  }
  parts.push(part);
  for (const raw of parts) {
    const p = raw.trim();
    if (!p) continue;
    if (p.startsWith('...')) return null;
    const m = /^(?:['"]([^'"]+)['"]|([A-Za-z_$][\w$]*))\s*(?::|$)/.exec(p);
    if (m) names.add(m[1] ?? m[2]);
  }
  return names;
}

interface TCallSite {
  key: string;
  file: string;
  /** Names the options literal passes; empty when there are no options. */
  passed: Set<string>;
}

function collectCallSites(): TCallSite[] {
  const sites: TCallSite[] = [];
  const call = /\bt\(\s*['"]([a-zA-Z0-9_.]+)['"]\s*/g;
  for (const file of walk(SRC_DIR)) {
    const src = readFileSync(file, 'utf8');
    call.lastIndex = 0;
    let m: RegExpExecArray | null;
    while ((m = call.exec(src)) !== null) {
      if (!isLocaleKey(m[1])) continue;
      let i = call.lastIndex;
      let passed: Set<string> | null = new Set();
      if (src[i] === ',') {
        i++;
        while (/\s/.test(src[i] ?? '')) i++;
        if (src[i] !== '{') continue; // options held in a variable
        const body = balanced(src, i);
        if (body === null) continue;
        passed = literalNames(body);
      } else if (src[i] !== ')') {
        continue;
      }
      if (passed) sites.push({ key: m[1], file: file.replace(REPO_ROOT + '/', ''), passed });
    }
  }
  return sites;
}

describe('locale interpolation (src-wide)', () => {
  const sites = collectCallSites();

  it('finds call sites to check (sanity check)', () => {
    expect(sites.filter((s) => s.passed.size > 0).length).toBeGreaterThan(50);
  });

  it('every {{variable}} in a key is passed where the key is used', () => {
    const offenders: string[] = [];
    for (const site of sites) {
      for (const [name, catalog] of [['en-US', enUS], ['zh-CN', zhCN]] as const) {
        const missing = new Set<string>();
        for (const text of stringVariants(catalog, site.key)) {
          VAR.lastIndex = 0;
          let v: RegExpExecArray | null;
          while ((v = VAR.exec(text)) !== null) if (!site.passed.has(v[1])) missing.add(v[1]);
        }
        if (missing.size) offenders.push(`  - ${site.key} (${name}) needs ${[...missing].join(', ')} in ${site.file}`);
      }
    }
    if (offenders.length > 0) throw new Error(`Interpolation variables not passed (${offenders.length}):\n${offenders.join('\n')}`);
  });
});

describe('locale key parity (src-wide)', () => {
  const { keys, perFile } = collectKeys();

  it('discovers a non-trivial number of keys (sanity check)', () => {
    expect(keys.size).toBeGreaterThan(200);
  });

  it('every referenced key resolves in en-US.json', () => {
    const missing: { key: string; files: string[] }[] = [];
    for (const key of keys) {
      if (resolveAnyVariant(enUS, key)) continue;
      const where: string[] = [];
      for (const [file, fileKeys] of perFile) {
        if (fileKeys.includes(key)) where.push(file.replace(REPO_ROOT + '/', ''));
      }
      missing.push({ key, files: where });
    }
    if (missing.length > 0) {
      const report = missing.map((m) => `  - ${m.key}\n    referenced in: ${m.files.join(', ')}`).join('\n');
      throw new Error(`Missing en-US keys (${missing.length}):\n${report}`);
    }
  });

  it('every referenced key resolves in zh-CN.json', () => {
    const missing: string[] = [];
    for (const key of keys) {
      if (!resolveAnyVariant(zhCN, key)) missing.push(key);
    }
    if (missing.length > 0) {
      throw new Error(`Missing zh-CN keys (${missing.length}):\n${missing.map((k) => '  - ' + k).join('\n')}`);
    }
  });

  it('zh-CN entries are non-empty strings', () => {
    // Keys that haven't been translated yet are flagged with the
    // `__pending: <english>` prefix (string, not object) — translators can
    // grep for `__pending:` to find work to do. We don't enforce any keys
    // are fully translated, just that the slot is non-empty.
    const offenders: string[] = [];
    for (const key of keys) {
      const value = lookup(zhCN, key);
      if (typeof value === 'string' && value.length === 0) offenders.push(key);
    }
    expect(offenders).toEqual([]);
  });
});

// ── Whole-catalog coverage ────────────────────────────────
//
// i18n.ts loads no fallback language, so a zh-CN visitor sees a raw key for
// anything zh-CN.json cannot resolve, including keys built at runtime that
// the sweep above cannot see. So every en-US key has to resolve in zh-CN the
// way i18next resolves it there. zh has one plural form, `other`, so a plural
// family is covered by its `_other` variant or by the bare key.

function leaves(obj: unknown, prefix = ''): string[] {
  if (!obj || typeof obj !== 'object') return [prefix];
  return Object.entries(obj as Record<string, unknown>).flatMap(([k, v]) => leaves(v, prefix ? `${prefix}.${k}` : k));
}

const PLURAL = /^(.*?)(_ordinal)?_(zero|one|two|few|many|other)$/;

describe('zh-CN covers the whole en-US catalog', () => {
  it('every en-US key resolves in zh-CN', () => {
    const missing = leaves(enUS).filter((key) => {
      const plural = PLURAL.exec(key);
      if (!plural) return typeof lookup(zhCN, key) !== 'string';
      const [, base, ordinal = ''] = plural;
      return typeof lookup(zhCN, `${base}${ordinal}_other`) !== 'string' && typeof lookup(zhCN, base) !== 'string';
    });
    if (missing.length > 0) {
      throw new Error(`en-US keys zh-CN cannot resolve (${missing.length}):\n${missing.map((k) => '  - ' + k).join('\n')}`);
    }
  });
});

// ── Trans slots ───────────────────────────────────────────
//
// `<Trans>` fills a paired tag with the catalog's content between the tags, so
// `<kbd></kbd>` renders its component empty and drops the children the call
// site gave it (a keycap glyph, an icon). A slot whose component carries its
// own content has to be self-closing, `<kbd/>`.
describe('Trans slots', () => {
  it('no catalog value has an empty paired tag', () => {
    const EMPTY_PAIR = /<(\w+)><\/\1>/;
    const offenders = ([['en-US', enUS], ['zh-CN', zhCN]] as const).flatMap(([name, catalog]) =>
      leaves(catalog)
        .filter((key) => EMPTY_PAIR.test(String(lookup(catalog, key))))
        .map((key) => `${name}: ${key}`),
    );
    expect(offenders).toEqual([]);
  });
});
