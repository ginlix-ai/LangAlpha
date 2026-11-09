// @vitest-environment node
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, it, expect } from 'vitest';

// The stamp that decides `html.desktop-mac`, lifted out of index.html and run as
// the browser runs it. It is inline and runs before the bundle, so nothing can
// import it. The case that matters is a shell through 0.3.0 that reports version
// 0.0.0: its page was in a renderer without the window's switches, so its
// `native` is a default, and believing it put the logo under the window buttons
// of a frameless window.

const source = readFileSync(resolve(__dirname, '../../../index.html'), 'utf8');
const stamp = [...source.matchAll(/<script>([\s\S]*?)<\/script>/g)]
  .map((m) => m[1])
  .find((s) => s.includes("classList.add('desktop-mac')"));

function stamped(shell: Record<string, unknown> | undefined): boolean {
  const classes = new Set<string>();
  const document = { documentElement: { classList: { add: (c: string) => classes.add(c) } } };
  new Function('window', 'document', stamp!)({ langalphaDesktop: shell }, document);
  return classes.has('desktop-mac');
}

describe('the desktop frame stamp in index.html', () => {
  it('is found', () => {
    expect(stamp).toBeTruthy();
  });

  it('follows the shell when it answered', () => {
    expect(stamped({ version: '0.3.0', platform: 'darwin', windowChrome: 'hidden' })).toBe(true);
    expect(stamped({ version: '0.3.0', platform: 'darwin', windowChrome: 'native' })).toBe(false);
  });

  it('guesses from the platform when the shell lost its switches', () => {
    expect(stamped({ version: '0.0.0', platform: 'darwin', windowChrome: 'native' })).toBe(true);
    expect(stamped({ version: '0.0.0', platform: 'win32', windowChrome: 'native' })).toBe(false);
  });

  it('guesses from the platform on a shell too old to send a frame', () => {
    expect(stamped({ version: '0.1.0', platform: 'darwin' })).toBe(true);
  });

  it('stamps nothing in a browser', () => {
    expect(stamped(undefined)).toBe(false);
  });
});
