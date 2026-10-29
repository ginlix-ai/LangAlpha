import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { renderHook } from '@testing-library/react';
import { INTRO_VISUALS, loadIntroScenes, useIntroScenes } from '../engine/introScenes';

// Unmocked on purpose: each visual is looked up in the one module index.ts
// maps it to, and a lookup that misses is skipped rather than thrown, so a
// visual mapped to the wrong module plays an empty panel with no error.
describe('intro scene registry', () => {
  it('finds every visual in the module it is mapped to', async () => {
    await loadIntroScenes(INTRO_VISUALS);
    const { result } = renderHook(() => useIntroScenes(INTRO_VISUALS));
    const missing = INTRO_VISUALS.filter((_, i) => !result.current[i]);
    expect(missing).toEqual([]);
  });
});

// A scene is laid out in px, as its camera and pointer are, so that the
// app's font-size setting leaves the two in step. A rem in a scene's classes
// or styles would grow with the root while the pointer stays put.
describe('intro scene sources', () => {
  it('size nothing in rem', () => {
    const dir = resolve(__dirname, '..', 'engine', 'introScenes');
    const found = readdirSync(dir)
      .filter((f) => /\.(tsx?|css)$/.test(f))
      .flatMap((f) =>
        readFileSync(resolve(dir, f), 'utf8')
          .split('\n')
          .flatMap((line, i) => (/\drem\b/.test(line) ? [`${f}:${i + 1}`] : []))
      );
    expect(found).toEqual([]);
  });
});
