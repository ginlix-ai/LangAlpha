import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import enUS from '../en-US.json';
import zhCN from '../zh-CN.json';

const REPO_ROOT = resolve(__dirname, '..', '..', '..');
// Sweep the entire Dashboard surface, not just `widgets/`. The custom-mode
// shell (`DashboardCustom.tsx`) and components/ live one level up but still
// reference `dashboard.widgets.*` keys (action bar, edit-holding dialog,
// reset confirmation). Catching those requires walking the parent dir.
const WIDGETS_DIR = resolve(REPO_ROOT, 'src/pages/Dashboard');

// Match `t('foo.bar.baz')` and `t("foo.bar.baz")` — the second arg form for
// interpolation is fine because we only capture the first quoted argument.
const T_CALL = /\bt\(\s*['"]([a-zA-Z0-9_.]+)['"]/g;
// Match `titleKey: 'foo.bar'` / `descriptionKey: 'foo.bar'` / etc. — keys
// stored in widget definitions and PresetMeta. Catches our static metadata
// references that aren't wrapped in t().
const KEY_PROP = /\b(?:titleKey|descriptionKey|nameKey|tagKey|bestForKey|labelKey|blurbKey)\s*[:=]\s*['"]([a-zA-Z0-9_.]+)['"]/g;
// Same shape but for our SOURCE_KEY / BUCKET_KEY const maps. Matches any
// `dashboard.*` key — both the widgets subtree and the page-level chrome
// namespaces (layoutToggle, etc.) so missing-key regressions in either
// surface get caught.
const KEY_VALUE = /['"](dashboard\.[a-zA-Z0-9_.]+)['"]/g;

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
  const files = walk(WIDGETS_DIR);
  const keys = new Set<string>();
  const perFile = new Map<string, string[]>();
  for (const file of files) {
    const src = readFileSync(file, 'utf8');
    const found: string[] = [];
    for (const re of [T_CALL, KEY_PROP, KEY_VALUE]) {
      re.lastIndex = 0;
      let m: RegExpExecArray | null;
      while ((m = re.exec(src)) !== null) {
        if (m[1].startsWith('dashboard.')) {
          keys.add(m[1]);
          found.push(m[1]);
        }
      }
    }
    if (found.length > 0) perFile.set(file, found);
  }
  return { keys, perFile };
}

describe('dashboard widget i18n keys', () => {
  const { keys, perFile } = collectKeys();

  it('discovers a non-trivial number of keys (sanity check)', () => {
    expect(keys.size).toBeGreaterThan(50);
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
