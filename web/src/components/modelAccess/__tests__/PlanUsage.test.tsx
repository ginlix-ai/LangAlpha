/**
 * How a usage table gives way as it narrows (`useUsageSteps`): by its width,
 * whenever the table mounts, and, for a line that must stay whole, by whether
 * that line fits.
 */
import { useLayoutEffect, useState } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import { VIEW_WIDTHS, useUsageSteps, type UsageSteps, type UsageWidths } from '../usageSteps';

const h = vi.hoisted(() => ({
  width: 0,
  observers: [] as ((entries: { contentRect: { width: number } }[]) => void)[],
  seen: [] as string[],
}));

const LIMIT_LINE = '5h limit used up, resets Thu 9:00 AM';
const SHORT_LINE = 'user@example.com';
// The width each whole line needs to fit at a step, as a layout would say.
const NEEDS: Record<string, Record<string, number>> = {
  [LIMIT_LINE]: { 'every column': 760, 'no Resets': 730 },
};

class CapturingResizeObserver {
  private cb: (typeof h.observers)[number];
  constructor(cb: (typeof h.observers)[number]) {
    this.cb = cb;
    h.observers.push(cb);
  }
  observe() {}
  unobserve() {}
  disconnect() {
    h.observers.splice(h.observers.indexOf(this.cb), 1);
  }
}

const stepOf = (s: UsageSteps) => {
  if (s.columns) return s.resets === 'block' ? 'every column' : 'no Resets';
  return s.tail === 'contents' ? 'bars' : 'stacked';
};

function Table({ lines = [] }: { lines?: string[] }) {
  const [table, setTable] = useState<HTMLDivElement | null>(null);
  const steps = useUsageSteps(table, VIEW_WIDTHS);
  const step = steps ? stepOf(steps) : 'none';
  useLayoutEffect(() => {
    h.seen.push(step);
  }, [step]);
  return (
    <div ref={setTable} data-testid="table" data-step={step}>
      {lines.map((line) => <span key={line} data-usage-whole>{line}</span>)}
    </div>
  );
}

/** A table that may not be mounted when its widths are given, as Claude
 *  Code's view is while its first token is added. */
function LateTable({ widths, shown }: { widths: UsageWidths | null; shown: boolean }) {
  const [table, setTable] = useState<HTMLDivElement | null>(null);
  const steps = useUsageSteps(table, widths);
  return (
    <section data-testid="late" data-step={steps ? stepOf(steps) : 'none'}>
      {shown && <div ref={setTable} />}
    </section>
  );
}

const step = () => screen.getByTestId('table').getAttribute('data-step');
const resize = (width: number) => {
  h.width = width;
  act(() => h.observers.forEach((cb) => cb([{ contentRect: { width } }])));
};

describe('useUsageSteps', () => {
  beforeEach(() => {
    h.width = 0;
    h.observers = [];
    h.seen = [];
    vi.stubGlobal('ResizeObserver', CapturingResizeObserver);
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(() => ({ width: h.width }) as DOMRect);
    const wholeNeed = (el: Element) => {
      const need = NEEDS[el.textContent ?? '']?.[el.closest('[data-step]')?.getAttribute('data-step') ?? ''] ?? 0;
      return h.width < need;
    };
    vi.spyOn(Element.prototype, 'scrollWidth', 'get').mockImplementation(function (this: Element) {
      return wholeNeed(this) ? 200 : 100;
    });
    vi.spyOn(Element.prototype, 'clientWidth', 'get').mockImplementation(() => 100);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it.each([
    [800, 'every column'],
    [716, 'every column'],
    [715, 'no Resets'],
    [656, 'no Resets'],
    [655, 'bars'],
    [588, 'bars'],
    [587, 'stacked'],
    [300, 'stacked'],
  ])('at %i wide, the table shows %s', (width, expected) => {
    h.width = width;
    render(<Table />);
    expect(step()).toBe(expected);
  });

  it('follows the table as it is resized', () => {
    h.width = 800;
    render(<Table />);
    resize(700);
    expect(step()).toBe('no Resets');
    resize(600);
    expect(step()).toBe('bars');
    resize(500);
    expect(step()).toBe('stacked');
    resize(720);
    expect(step()).toBe('every column');
  });

  it('measures a table that mounts after its widths are given', () => {
    h.width = 600;
    const { rerender } = render(<LateTable widths={null} shown />);
    rerender(<LateTable widths={VIEW_WIDTHS} shown={false} />);
    rerender(<LateTable widths={VIEW_WIDTHS} shown />);
    expect(screen.getByTestId('late').getAttribute('data-step')).toBe('bars');
    resize(500);
    expect(screen.getByTestId('late').getAttribute('data-step')).toBe('stacked');
  });

  it('takes every column for a table that is not laid out, whose width reads 0', () => {
    h.width = 0;
    render(<Table />);
    expect(step()).toBe('every column');
  });

  it.each([
    [740, 'no Resets'],
    [720, 'bars'],
  ])('at %i wide, steps a table down for a whole line that overflows until it fits: %s', (width, expected) => {
    h.width = width;
    render(<Table lines={[LIMIT_LINE]} />);
    expect(step()).toBe(expected);
  });

  it('leaves a table at its step by width while its whole lines fit', () => {
    h.width = 720;
    render(<Table lines={[SHORT_LINE]} />);
    expect(step()).toBe('every column');
  });

  it('does not climb back at the width it gave a step up at, and only at a wider one', () => {
    h.width = 720;
    render(<Table lines={[LIMIT_LINE]} />);
    expect(h.seen).toEqual(['every column', 'no Resets', 'bars']);
    resize(720);
    resize(700);
    expect(step()).toBe('bars');
    expect(h.seen).toEqual(['every column', 'no Resets', 'bars']);
    resize(800);
    expect(step()).toBe('every column');
  });

  it('starts over once the text of its whole lines changes', () => {
    h.width = 720;
    const { rerender } = render(<Table lines={[LIMIT_LINE]} />);
    expect(step()).toBe('bars');
    rerender(<Table lines={[SHORT_LINE]} />);
    expect(step()).toBe('every column');
  });
});
