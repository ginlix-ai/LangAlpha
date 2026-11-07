// @vitest-environment node
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

// Text tokens are chosen by eye and read everywhere, so a step lightened for
// looks fails WCAG across the whole app with no failure of its own to see.
const css = readFileSync(resolve(__dirname, '../tokens.css'), 'utf8');

type Rgb = [number, number, number];

function hex(value: string): Rgb {
  return [1, 3, 5].map((i) => parseInt(value.slice(i, i + 2), 16)) as Rgb;
}

function luminance(rgb: Rgb): number {
  const [r, g, b] = rgb.map((v) => {
    const s = v / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function ratio(fg: Rgb, bg: Rgb): number {
  const [a, b] = [luminance(fg), luminance(bg)];
  return (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
}

function lightToken(name: string): Rgb {
  const block = css.slice(css.indexOf('[data-theme="light"]'));
  return hex(new RegExp(`${name}:\\s*(#[0-9A-Fa-f]{6})`).exec(block)![1]);
}

function darkTextOn(name: string, bg: Rgb): Rgb {
  const m = new RegExp(`${name}:\\s*rgba\\((\\d+), (\\d+), (\\d+), ([\\d.]+)\\)`).exec(css);
  if (!m) return hex(new RegExp(`${name}:\\s*(#[0-9A-Fa-f]{6})`).exec(css)![1]);
  const a = Number(m[4]);
  return [1, 2, 3].map((i) => Number(m[i]) * a + bg[i - 1] * (1 - a)) as Rgb;
}

describe('text tokens hold 4.5:1', () => {
  const white: Rgb = [255, 255, 255];
  const canvas = lightToken('--color-bg-canvas');

  it.each(['--color-text-secondary', '--color-text-tertiary', '--color-text-quaternary', '--color-text-muted'])('%s on white', (name) => {
    expect(ratio(lightToken(name), white)).toBeGreaterThanOrEqual(4.5);
  });

  it.each(['--color-text-secondary', '--color-text-tertiary', '--color-text-muted'])('%s on the canvas', (name) => {
    expect(ratio(lightToken(name), canvas)).toBeGreaterThanOrEqual(4.5);
  });

  // hsl(220 4% 14.5%), the dark card, is the lightest ground text sits on there.
  it.each(['--color-text-secondary', '--color-text-tertiary', '--color-text-quaternary', '--color-text-muted'])('%s on the dark card', (name) => {
    const card: Rgb = [35, 36, 38];
    expect(ratio(darkTextOn(name, card), card)).toBeGreaterThanOrEqual(4.5);
  });
});
