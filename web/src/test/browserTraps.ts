/// <reference types="vitest/globals" />
// The traps for a test file that declares `// @vitest-environment node`, armed
// by setup.ts before the file's module graph loads.
//
// A node file must never touch the browser. The dangerous failure is not a
// crash but a quiet pass: code that probes `typeof window` takes its no-DOM
// branch here, a branch production never runs, and the assertions still hold.
// So every browser global below is a trap. Reading one from app or test code
// throws where it happens and, in case something catches that, fails the file
// again in `afterAll`. The fix is to drop the file's node pragma, not to guard
// the read.

const BROWSER_GLOBALS = [
  'window',
  'document',
  'navigator',
  'location',
  'localStorage',
  'sessionStorage',
  'matchMedia',
  'getComputedStyle',
  'requestAnimationFrame',
  'customElements',
  'HTMLElement',
  'Element',
] as const;

// Vitest's own formatters and fake timers probe `window` and friends to decide
// how to print or what to patch. Those reads are the runner's, not the test's.
const RUNNER_FRAME = /[\\/]node_modules[\\/](?:vitest|@vitest[\\/][^\\/]+|chai|tinyrainbow|loupe)[\\/]|\(node:|at node:/;

const touched = new Map<string, string>();

function callerFrame(): string {
  const frames = (new Error().stack ?? '').split('\n').slice(1);
  return frames.find((f) => !f.includes('browserTraps.ts'))?.trim() ?? '<unknown>';
}

export function installBrowserTraps(): void {
  for (const name of BROWSER_GLOBALS) {
    const native = Object.getOwnPropertyDescriptor(globalThis, name);
    const read = (): unknown =>
      native && ('get' in native && native.get ? native.get.call(globalThis) : native.value);
    Object.defineProperty(globalThis, name, {
      configurable: true,
      get() {
        const frame = callerFrame();
        if (RUNNER_FRAME.test(frame)) return read();
        if (!touched.has(name)) touched.set(name, frame);
        throw new ReferenceError(
          `${name} read in a node-environment test (${frame}). ` +
            'This file uses the browser: remove its `@vitest-environment node` pragma.',
        );
      },
    });
  }

  afterAll(() => {
    if (touched.size === 0) return;
    const reads = [...touched].map(([name, frame]) => `  ${name}: ${frame}`).join('\n');
    throw new Error(
      `Browser globals were read in a node-environment test file:\n${reads}\n` +
        'Remove the `@vitest-environment node` pragma so the file runs under jsdom.',
    );
  });
}
