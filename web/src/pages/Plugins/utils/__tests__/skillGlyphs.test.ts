// @vitest-environment node
import { readdirSync, readFileSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import { SKILL_GLYPHS, skillGlyph } from '../skillGlyphs';

/**
 * Every glyph a shipped bundle names is one the web draws. A name outside the
 * set falls back to the package's mark without a word, so a typo in a
 * `plugin.json`, or a glyph added there and not here, shows up only as a row
 * that looks like its neighbours.
 */

const PLUGINS = resolve(__dirname, '../../../../../../plugins');

type Manifest = {
  extensions?: { 'ai.langalpha'?: { skills?: Record<string, { icon?: string }> | string[] } };
};

function shippedIcons(): [string, string][] {
  const out: [string, string][] = [];
  for (const entry of readdirSync(PLUGINS, { withFileTypes: true })) {
    const path = join(PLUGINS, entry.name, 'plugin.json');
    if (!entry.isDirectory() || !existsSync(path)) continue;
    const manifest = JSON.parse(readFileSync(path, 'utf8')) as Manifest;
    const skills = manifest.extensions?.['ai.langalpha']?.skills;
    if (!skills || Array.isArray(skills)) continue;
    for (const [skill, meta] of Object.entries(skills)) {
      if (meta.icon) out.push([`${entry.name}/${skill}`, meta.icon]);
    }
  }
  return out;
}

describe('skill glyphs', () => {
  it('reads the shipped bundles', () => {
    expect(shippedIcons().length).toBeGreaterThan(0);
  });

  it('draws every glyph a shipped bundle names', () => {
    const missing = shippedIcons()
      .filter(([, icon]) => !icon.includes('.'))
      .filter(([, icon]) => !skillGlyph(icon))
      .map(([where, icon]) => `${where}: ${icon}`);
    expect(missing).toEqual([]);
  });

  it('keys each glyph by the name the manifest uses', () => {
    // lucide's own spelling, so an author can look a name up on its site.
    for (const [name, Icon] of Object.entries(SKILL_GLYPHS)) {
      const pascal = name.replace(/(^|-)([a-z0-9])/g, (_, __, c: string) => c.toUpperCase());
      expect(Icon.displayName, name).toBe(pascal);
    }
  });

  it('does not reach the prototype', () => {
    expect(skillGlyph('constructor')).toBeUndefined();
    expect(skillGlyph('toString')).toBeUndefined();
  });
});
