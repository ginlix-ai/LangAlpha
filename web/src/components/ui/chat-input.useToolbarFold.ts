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
/** `h-8 w-8` icon buttons (attach, chart capture, mic, send, ⋯) and the token-usage ring's slot. */
export const ICON_BUTTON_W = 32;
/** `gap-2` between the left and right control groups. */
export const GROUP_GAP = 8;
/** `px-3` on the container's inner column (both sides). */
export const CONTAINER_PX = 24;
/** 1px container border per side — `getBoundingClientRect` reports border-box. */
export const CONTAINER_BORDER = 2;

export interface FoldResult {
  inline: Set<string>;
  folded: string[];
  /** The model pill has no room for its reasoning level beside the name. */
  levelHidden: boolean;
  /** Even with every item in ⋯ the row cannot keep the model name at its
   *  floor, so the pill shows its logo alone rather than a sliver of name. */
  nameHidden: boolean;
}

/**
 * Which toolbar items keep their inline slot at `containerWidth`, and how
 * much of the model pill shows.
 *
 * Items are consumed in declaration order and the loop STOPS at the first one
 * that doesn't fit: priority is a real ordering, not a best-effort packing, so
 * a wide high-priority item can never be skipped over in favour of a narrow
 * low-priority one. With no measurements (jsdom, ResizeObserver unavailable)
 * everything stays inline.
 *
 * The pill gives way last, and in steps: every item folds before it loses
 * anything, then its level goes, then the name truncates to its floor, and
 * past that the name itself goes. The level goes before the name shrinks,
 * since a cut name stops saying which model runs while the picker it opens
 * names the level anyway.
 */
export function computeFold({
  containerWidth,
  widths,
  items,
  fixedLeft,
  rightReserve,
  rightFloor = rightReserve,
  status = 0,
  gap = TOOLBAR_GAP,
  overflowWidth = ICON_BUTTON_W,
}: {
  containerWidth: number;
  widths: Readonly<Record<string, number>> | null;
  items: readonly { id: string; visible: boolean }[];
  /** Left chrome + container padding/border consumed before the first item. */
  fixedLeft: number;
  /** Right group (model pill with its level, mic, send) + the gap between
   *  the two groups. */
  rightReserve: number;
  /** The same with the level gone and the model name cut to its floor. */
  rightFloor?: number;
  /** The host's status at the end of the left group. The model pill gives
   *  up the room for it, so items don't hop into ⋯ as it comes and goes;
   *  only past the name's floor does it cost them their slot. */
  status?: number;
  gap?: number;
  overflowWidth?: number;
}): FoldResult {
  const candidates = items.filter((i) => i.visible).map((i) => i.id);
  const measured = widths && Object.values(widths).some((w) => w > 0) ? widths : null;
  if (!containerWidth || !measured) {
    return { inline: new Set(candidates), folded: [], levelHidden: false, nameHidden: false };
  }

  const cost = (ids: readonly string[]) => ids.reduce((sum, id) => sum + (measured[id] ?? 0) + gap, 0);
  const trigger = gap + overflowWidth; // the ⋯ trigger itself
  const nameHidden = fixedLeft + status + rightFloor + (candidates.length > 0 ? trigger : 0) > containerWidth;
  // The row as it paints with the pill whole: whatever it lacks, the level
  // gives up first.
  const levelHidden = (inline: readonly string[]) =>
    fixedLeft + cost(inline) + (inline.length < candidates.length ? trigger : 0) + status + rightReserve > containerWidth;

  let budget = containerWidth - fixedLeft - Math.max(rightReserve, rightFloor + status);
  if (cost(candidates) <= budget) {
    return { inline: new Set(candidates), folded: [], levelHidden: levelHidden(candidates), nameHidden };
  }

  budget -= trigger;
  const inline: string[] = [];
  for (const id of candidates) {
    const w = cost([id]);
    if (w > budget) break;
    inline.push(id);
    budget -= w;
  }
  return {
    inline: new Set(inline),
    folded: candidates.filter((id) => !inline.includes(id)),
    levelHidden: levelHidden(inline),
    nameHidden,
  };
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
  rightFloor,
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
  /** `rightReserve` with no level and the model name at its floor. */
  rightFloor?: (widths: Readonly<Record<string, number>>) => number;
}): {
  measureRowRef: RefObject<HTMLDivElement | null>;
  statusRef: RefObject<HTMLSpanElement | null>;
  fold: FoldResult;
} {
  const measureRowRef = useRef<HTMLDivElement>(null);
  const statusRef = useRef<HTMLSpanElement>(null);
  const [widths, setWidths] = useState<Record<string, number> | null>(null);
  // Floor-quantized: a budget must never be spent against width the container
  // doesn't have (rounding up by 8px is exactly how pills end up overlapping).
  const containerWidth = useContainerWidth(containerRef, 16, { round: 'floor' });
  // Measured where it paints: a host's status is live, so it has no twin in
  // the measure row. The element `statusRef` names holds the gap before it,
  // so its width is the whole cost.
  const statusWidth = useContainerWidth(statusRef, 1);

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
    statusRef,
    fold: computeFold({
      containerWidth,
      widths,
      items,
      fixedLeft,
      rightReserve: rightReserve(widths ?? {}),
      rightFloor: rightFloor?.(widths ?? {}),
      status: statusWidth,
    }),
  };
}
