import { createContext, useLayoutEffect, useMemo, useState } from 'react';

/**
 * How a usage table gives way as it narrows, as classes for each step:
 * every column; then without Resets, whose times stay in the window cells'
 * tooltip; then no columns, each window a small bar on the detail line;
 * then, too narrow for the text beside the status and actions, those take a
 * line of their own under it.
 */
export interface UsageSteps {
  /** The rows' grid: tile, text, then 5h and 7d, then Resets, then status and actions. */
  grid: string;
  /** A window's cell and header. */
  window: string;
  /** Resets' cell and header. */
  resets: string;
  /** The header row. */
  header: string;
  /** The text column across the usage columns, for a row that has no use to show in them. */
  spanUsage: string;
  /** The bars on the detail line, once the columns are gone. */
  inline: string;
  /** A row on the grid's columns, or below that a row of its own. */
  row: string;
  /** The status and actions: two cells, or one group on a line under the text. */
  tail: string;
  /** A line under a row, from its text's start to its end. */
  below: string;
  /** Whether the columns and header row, and with it its Check now, show. */
  columns: boolean;
}

/** The widths a usage table needs for each step past stacked: the bars
 *  beside the status, the columns without Resets, then every column. The
 *  Claude Code view and the Settings summary differ, since their columns do. */
export type UsageWidths = readonly [number, number, number];
export const VIEW_WIDTHS: UsageWidths = [588, 656, 716];
export const SUMMARY_WIDTHS: UsageWidths = [598, 598, 648];

const GRIDS = [
  'grid-cols-[auto_minmax(0,1fr)_auto_auto]',
  'grid-cols-[auto_minmax(0,1fr)_auto_auto]',
  'grid-cols-[auto_minmax(0,1fr)_auto_auto_auto_auto]',
  'grid-cols-[auto_minmax(0,1fr)_auto_auto_auto_auto_auto]',
];

/** The classes of step `level`, 0 stacked to 3 every column. */
function stepsAt(level: number): UsageSteps {
  return {
    grid: GRIDS[level],
    window: level >= 2 ? 'flex' : 'hidden',
    resets: level >= 3 ? 'block' : 'hidden',
    header: level >= 2 ? 'grid' : 'hidden',
    spanUsage: level === 3 ? 'col-span-4' : level === 2 ? 'col-span-3' : '',
    inline: level >= 2 ? 'hidden' : '',
    row: level >= 1 ? 'grid grid-cols-subgrid' : '',
    tail: level >= 1 ? 'contents' : '',
    below: level >= 1 ? 'col-[2/-1]' : 'basis-full pl-10',
    columns: level >= 2,
  };
}

/**
 * The steps of the table `el`: the widest its width allows, then one
 * narrower for as long as a line that must stay whole there
 * (`data-usage-whole`, a limit line) overflows. A step given up so stays
 * given up at that width and below until those lines change, so the table
 * never flips between two steps. One not laid out (no width) takes every
 * column. Null without `widths`, for no table. The table is observed
 * whenever it mounts, which can be after `widths` are given: a table that
 * mounts later is measured then, not left at no width.
 */
export function useUsageSteps(el: HTMLElement | null, widths: UsageWidths | null): UsageSteps | null {
  const [width, setWidth] = useState(0);
  const [raised, setRaised] = useState<{ lines: string; widths: number[] } | null>(null);
  const floors = raised?.widths ?? widths;
  const level = !floors ? 0 : width === 0 ? 3 : floors.filter((w) => width >= w).length;
  useLayoutEffect(() => {
    if (!el || !widths) return;
    // Read before paint, so no frame lays the table out at no width.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setWidth(el.getBoundingClientRect().width);
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(el);
    return () => observer.disconnect();
  }, [el, widths]);
  // After every render, since any row's own can change what overflows.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useLayoutEffect(() => {
    const whole = Array.from(el?.querySelectorAll<HTMLElement>('[data-usage-whole]') ?? []);
    const lines = whole.map((line) => line.textContent).join('\n');
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (raised && raised.lines !== lines) setRaised(null);
    else if (floors && level > 0 && whole.some((line) => line.scrollWidth > line.clientWidth)) {
      setRaised({ lines, widths: floors.map((w, i) => (i >= level - 1 ? Math.max(w, width + 1) : w)) });
    }
  });
  return useMemo(() => (widths ? stepsAt(level) : null), [widths, level]);
}

/** Set by a table of rows, which then lay their use out on its columns. */
export const UsageTableContext = createContext<UsageSteps | null>(null);
