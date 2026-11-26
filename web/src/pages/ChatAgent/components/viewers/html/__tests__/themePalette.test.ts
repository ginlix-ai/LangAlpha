// @vitest-environment node
import { describe, expect, it } from 'vitest';

// Node, not jsdom, on purpose: the headless render page imports this builder
// with no app DOM, so importing it must read no browser global.
import { buildHtmlSrcDoc } from '../buildHtmlSrcDoc';
import { LIGHT_PALETTE, rgbaToHex, THEME_VARS } from '../frameTheme';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

// Read from disk: a CSS import, `?raw` included, is emptied under vitest.
const tokens = readFileSync(join(__dirname, '../../../../../../styles/tokens.css'), 'utf8');

/** The body of the `nth` top-level block opened by `selector {`. */
function block(selector: string, nth = 0): string {
  let at = -1;
  for (let i = 0; i <= nth; i++) at = tokens.indexOf(`${selector} {`, at + 1);
  if (at < 0) throw new Error(`no ${selector} block`);
  const open = tokens.indexOf('{', at);
  let depth = 0;
  for (let i = open; i < tokens.length; i++) {
    if (tokens[i] === '{') depth++;
    else if (tokens[i] === '}' && --depth === 0) return tokens.slice(open + 1, i);
  }
  throw new Error(`unclosed ${selector} block`);
}

function declarations(body: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const m of body.replace(/\/\*[\s\S]*?\*\//g, '').matchAll(/(--[\w-]+)\s*:\s*([^;]+);/g)) {
    out[m[1]] = m[2].trim();
  }
  return out;
}

const byte = (n: number) => Math.min(255, Math.max(0, n)).toString(16).padStart(2, '0');

/** A token value as the builder writes it: `#rrggbb`, or `#rrggbbaa` when translucent. */
function hex(value: string, scope: Record<string, string>): string {
  const v = value.replace(/var\((--[\w-]+)\)/g, (_, name: string) => scope[name]);
  let m = /^#([0-9a-f]{6})$/i.exec(v);
  if (m) return `#${m[1].toLowerCase()}`;
  // As the canvas serializes it, which is the form rgbaToHex reads.
  const rgba = rgbaToHex(v.replace(/\s*,\s*/g, ', '));
  if (rgba) return rgba;
  m = /^hsl\(([\d.]+)\s+([\d.]+)%\s+([\d.]+)%\)$/.exec(v);
  if (m) {
    const h = +m[1];
    const s = +m[2] / 100;
    const l = +m[3] / 100;
    const k = (n: number) => (n + h / 30) % 12;
    const a = s * Math.min(l, 1 - l);
    const f = (n: number) => l - a * Math.max(-1, Math.min(k(n) - 3, 9 - k(n), 1));
    return `#${[f(0), f(8), f(4)].map((x) => byte(Math.round(x * 255))).join('')}`;
  }
  throw new Error(`unparsed token value ${value}`);
}

describe('LIGHT_PALETTE', () => {
  // The light theme declares only what differs from :root; the rest is inherited.
  const root = { ...declarations(block('\n:root', 0)), ...declarations(block('\n:root', 1)) };
  const light = { ...root, ...declarations(block('[data-theme="light"]')) };

  it.each(THEME_VARS)('%s matches the light theme in tokens.css', (name) => {
    expect(LIGHT_PALETTE.vars[name]).toBe(hex(light[name], light));
  });

  it('builds a document with no DOM to read', () => {
    expect(buildHtmlSrcDoc('widget-inline', { html: '<p>x</p>', theme: LIGHT_PALETTE }))
      .toContain('--color-text-primary: #1f1f1e;');
  });
});
