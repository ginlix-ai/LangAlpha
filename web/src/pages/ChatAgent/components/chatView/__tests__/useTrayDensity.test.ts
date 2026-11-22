/**
 * The layout holds while the liquid runs, either way: a layout change
 * mid-run would remount the liquid from the dock. The one measure it takes
 * is on the first frame shown, the unfold's, before the liquid has run.
 */
import { describe, it, expect, afterEach, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import type { TrayDensity } from '../trayDensity';
import { useTrayDensity } from '../useTrayDensity';

const OriginalRO = window.ResizeObserver;
let resized: (() => void) | null = null;

beforeEach(() => {
  window.ResizeObserver = class {
    constructor(cb: ResizeObserverCallback) {
      resized = () => cb([], this as unknown as ResizeObserver);
    }
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

afterEach(() => {
  window.ResizeObserver = OriginalRO;
  resized = null;
  document.body.replaceChildren();
});

function sized<T extends HTMLElement>(el: T, width: () => number): T {
  el.getBoundingClientRect = () => DOMRect.fromRect({ width: width(), height: 20 });
  return el;
}

/** A row `available` wide, and a probe with one 100px chip and a 20px Status button at each density but tight, at 40px. */
function line() {
  const box = { available: 300 };
  const row = sized(document.createElement('div'), () => box.available);
  const probe = document.createElement('div');
  for (const level of ['full', 'compact', 'tight', 'more'] as const) {
    const el = document.createElement('div');
    el.dataset.level = level;
    const ids = level === 'more' ? ['status'] : ['tasks', 'status'];
    for (const id of ids) {
      const chip = document.createElement('div');
      chip.dataset.chip = id;
      sized(chip, () => (id === 'status' ? 20 : level === 'tight' ? 40 : 100));
      el.append(chip);
    }
    probe.append(el);
  }
  document.body.append(row, probe);
  return { box, rowRef: { current: row }, probeRef: { current: probe } };
}

function renderDensity(initial: { shown: boolean; hold: boolean }) {
  const { box, rowRef, probeRef } = line();
  const hook = renderHook((opts: { shown: boolean; hold: boolean }) => useTrayDensity(rowRef, probeRef, opts), { initialProps: initial });
  return { box, hook, density: (): TrayDensity => hook.result.current.density };
}

describe('useTrayDensity: holding through the liquid', () => {
  it('measures the first frame shown even while held, as an unfold starts', () => {
    const { box, hook, density } = renderDensity({ shown: false, hold: true });
    box.available = 80;
    hook.rerender({ shown: true, hold: true });
    expect(density()).toBe('tight');
  });

  it('keeps its layout through a run, a change applying once the run lands', () => {
    const { box, hook, density } = renderDensity({ shown: false, hold: true });
    box.available = 80;
    hook.rerender({ shown: true, hold: true });
    expect(density()).toBe('tight');
    // The row grows mid-run (a price tick or a chip would resize it the same way).
    box.available = 300;
    act(() => resized?.());
    expect(density()).toBe('tight');
    hook.rerender({ shown: true, hold: false });
    expect(density()).toBe('full');
  });

  it('follows the row at once with no run', () => {
    const { box, density } = renderDensity({ shown: true, hold: false });
    expect(density()).toBe('full');
    box.available = 80;
    act(() => resized?.());
    expect(density()).toBe('tight');
  });
});
