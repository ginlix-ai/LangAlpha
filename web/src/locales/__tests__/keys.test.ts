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
// static metadata references that aren't wrapped in t().
const KEY_PROP = /\b(?:titleKey|descriptionKey|nameKey|tagKey|bestForKey|labelKey|blurbKey)\s*[:=]\s*['"]([a-zA-Z0-9_.]+)['"]/g;
// Bare quoted keys held in const maps and passed to a helper rather than to
// `t()` directly: SOURCE_KEY / BUCKET_KEY on the dashboard, the tab-label map
// and the skill-action failure helper under plugins. Scoped to those two
// namespaces rather than swept tree-wide, because `isLocaleKey` only checks
// the first segment and plenty of dotted non-keys (module paths, filenames)
// would otherwise qualify. The cost of being in this list is that a namespace
// here may not also be used for storage keys or other dotted identifiers --
// see `plugins:deckExpanded`, which uses a colon for exactly that reason.
const KEY_VALUE = /['"]((?:dashboard|plugins)\.[a-zA-Z0-9_.]+)['"]/g;

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
// `_zero`) when the bare key is absent and the call passes `count`. A test
// that only checks the bare key would falsely flag those plural-only entries.
const PLURAL_SUFFIXES = ['', '_one', '_other', '_zero', '_two', '_few', '_many'];
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
