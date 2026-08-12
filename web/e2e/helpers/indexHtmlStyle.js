import { expect } from '@playwright/test';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const WEB_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/** Read a file relative to `web/`. */
export const readWebFile = (p) => readFileSync(resolve(WEB_ROOT, p), 'utf8');

/**
 * The unscoped `color-scheme: dark` from index.html's inline <style>.
 *
 * Two specs rebuild an embedded document to check a cascade this declaration
 * drives, and both must source it from the real file — a copy that drifted
 * would leave them guarding a collision that no longer exists while still
 * passing. Fails loudly if index.html stops shipping it.
 */
export function indexHtmlInlineStyle() {
  const html = readWebFile('index.html');
  const blocks = [...html.matchAll(/<style>([\s\S]*?)<\/style>/g)].map((m) => m[1]);
  const scheme = blocks.find((b) => b.includes('color-scheme'));
  expect(scheme, 'index.html should still declare an inline color-scheme').toBeTruthy();
  expect(scheme).toMatch(/html\s*\{[^}]*color-scheme:\s*dark/);
  return scheme;
}
