// @vitest-environment node
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

import enUS from '@/locales/thisMacFirstRun/en-US.json';
import zhCN from '@/locales/thisMacFirstRun/zh-CN.json';

// The first run's copy sits outside the main catalogs, where
// locales/__tests__/keys.test.ts never looks, so this pins the same contract
// for it: every key the page reads is in both locales, and neither catalog
// holds copy the page does not read.
const source = readFileSync(resolve(__dirname, '..', 'ThisMacFirstRun.tsx'), 'utf8');
const read = [...new Set([...source.matchAll(/\b(?:tRun|t)\('([a-zA-Z]+)'/g)].map((m) => m[1]))].sort();

describe.each(Object.entries({ 'en-US': enUS, 'zh-CN': zhCN }))('the %s first-run copy', (_, catalog) => {
  it('holds exactly the keys the page reads', () => {
    expect(Object.keys(catalog).sort()).toEqual(read);
  });

  it('has no empty copy', () => {
    for (const [key, text] of Object.entries(catalog)) expect(text.trim(), key).not.toBe('');
  });
});
