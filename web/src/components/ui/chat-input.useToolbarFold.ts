import { useLayoutEffect, useRef, useState, type ReactNode, type RefObject } from 'react';
import { useContainerWidth } from '@/hooks/useNarrowContainer';

/**
 * Progressive toolbar folding for the composer action bar.
 *
 * One registry drives everything: the items are declared once, in priority
 * order (first = last to fold), and each one knows how to render itself inline,
 * in the ⋯ overflow menu, and — via `measureOnly` — in the hidden measure row.
 * `computeFold` is pure so the fit path is unit-testable without a layout
 * engine; the hook wires it to the live measurements.
 */

export interface ToolbarItemRender {
  /** Geometry-only render for the hidden measure row: same component, no
   *  handlers, menus, portals, or aria state. */
  measureOnly?: boolean;
}

export interface ToolbarItem {
  id: string;
  /** Menu grouping. The overflow loop emits a separator between groups, so
   *  no call site ever places one by hand. */
  group: string;
  visible: boolean;
  /** Toggle is on — surfaces the ⋯ trigger dot while folded. */
  active?: boolean;
  inline: (opts: ToolbarItemRender) => ReactNode;
  menu: () => ReactNode;
}

/* Toolbar geometry (px), mirroring the composer's Tailwind classes so the fit
 * budget is spelled out instead of hidden in magic numbers. */
/** `gap-1` between action-bar controls. */
export const TOOLBAR_GAP = 4;
/** `h-8 w-8` icon buttons (attach, chart capture, mic, send, ⋯). */
export const ICON_BUTTON_W = 32;
/** `gap-2` between the left and right control groups. */
export const GROUP_GAP = 8;
/** `px-3` on the container's inner column (both sides). */
export const CONTAINER_PX = 24;
/** 1px container border per side — `getBoundingClientRect` reports border-box. */
export const CONTAINER_BORDER = 2;
/** Token-usage ring. */
export const TOKEN_RING_W = 18;

export interface FoldResult {
  inline: Set<string>;
  folded: string[];
}

/**
 * Which toolbar items keep their inline slot at `containerWidth`.
 *
 * Items are consumed in declaration order and the loop STOPS at the first one
 * that doesn't fit: priority is a real ordering, not a best-effort packing, so
 * a wide high-priority item can never be skipped over in favour of a narrow
 * low-priority one. With no measurements (jsdom, ResizeObserver unavailable)
 * everything stays inline.
 */
export function computeFold({
  containerWidth,
  widths,
  items,
  fixedLeft,
  rightReserve,
  gap = TOOLBAR_GAP,
  overflowWidth = ICON_BUTTON_W,
}: {
  containerWidth: number;
  widths: Readonly<Record<string, number>> | null;
  items: readonly { id: string; visible: boolean }[];
  /** Left chrome + container padding/border consumed before the first item. */
  fixedLeft: number;
  /** Right group (model pill, mic, send) + the gap between the two groups. */
  rightReserve: number;
  gap?: number;
  overflowWidth?: number;
}): FoldResult {
  const candidates = items.filter((i) => i.visible).map((i) => i.id);
  const measured = widths && Object.values(widths).some((w) => w > 0) ? widths : null;
  if (!containerWidth || !measured) {
    return { inline: new Set(candidates), folded: [] };
  }

  let budget = containerWidth - fixedLeft - rightReserve;
  const needAll = candidates.reduce((sum, id) => sum + (measured[id] ?? 0) + gap, 0);
  if (needAll <= budget) return { inline: new Set(candidates), folded: [] };

  budget -= gap + overflowWidth; // the ⋯ trigger itself
  const inline = new Set<string>();
  for (const id of candidates) {
    const w = (measured[id] ?? 0) + gap;
    if (w > budget) break;
    inline.add(id);
    budget -= w;
  }
  return { inline, folded: candidates.filter((id) => !inline.has(id)) };
}

const sameWidths = (a: Record<string, number>, b: Record<string, number>) =>
  Object.keys(a).length === Object.keys(b).length
  && Object.entries(b).every(([k, v]) => a[k] === v);

export function useToolbarFold({
  containerRef,
  items,
  measureKey,
  fixedLeft,
  rightReserve,
}: {
  containerRef: RefObject<HTMLElement | null>;
  /** Priority order, memoized — its identity also drives re-measurement. */
  items: ToolbarItem[];
  /** Signature of measured chrome that lives outside `items` (the model
   *  pill), so a label change there re-measures too. */
  measureKey?: string;
  fixedLeft: number;
  /** Takes the measured widths: the model pill's width is measured, not fixed. */
  rightReserve: (widths: Readonly<Record<string, number>>) => number;
}): { measureRowRef: RefObject<HTMLDivElement | null>; fold: FoldResult } {
  const measureRowRef = useRef<HTMLDivElement>(null);
  const [widths, setWidths] = useState<Record<string, number> | null>(null);
  // Floor-quantized: a budget must never be spent against width the container
  // doesn't have (rounding up by 8px is exactly how pills end up overlapping).
  const containerWidth = useContainerWidth(containerRef, 16, { round: 'floor' });

  useLayoutEffect(() => {
    const row = measureRowRef.current;
    if (!row) return;
    const next: Record<string, number> = {};
    row.querySelectorAll<HTMLElement>('[data-measure]').forEach((el) => {
      next[el.dataset.measure as string] = el.offsetWidth;
    });
    setWidths((prev) => (prev && sameWidths(prev, next) ? prev : next));
  }, [items, measureKey]);

  return {
    measureRowRef,
    fold: computeFold({
      containerWidth,
      widths,
      items,
      fixedLeft,
      rightReserve: rightReserve(widths ?? {}),
    }),
  };
}
