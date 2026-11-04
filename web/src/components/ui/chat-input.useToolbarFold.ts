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
  /** Sits in the right group beside the model pill. It folds last, once the
   *  model name has no room left to give (see `computeFold`). */
  slot?: 'right';
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
  /** Even with every item in ⋯ the row cannot keep the model name at its
   *  floor, so the pill shows its logo alone rather than a sliver of name. */
  nameHidden: boolean;
}

/**
 * Which toolbar items keep their inline slot at `containerWidth`.
 *
 * Items are consumed in declaration order and the loop STOPS at the first one
 * that doesn't fit: priority is a real ordering, not a best-effort packing, so
 * a wide high-priority item can never be skipped over in favour of a narrow
 * low-priority one. With no measurements (jsdom, ResizeObserver unavailable)
 * everything stays inline.
 *
 * Right-slot items are the last resort. The model name truncates before they
 * move, so they fold only when the row cannot hold them with every left item
 * folded and the name cut to its floor; the last declared folds first. Past
 * that, the name itself goes.
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
  items: readonly { id: string; visible: boolean; slot?: 'right' }[];
  /** Left chrome + container padding/border consumed before the first item. */
  fixedLeft: number;
  /** Right group (model pill, mic, send) + the gap between the two groups,
   *  without its right-slot items. */
  rightReserve: number;
  /** The same with the model name cut to its floor. */
  rightFloor?: number;
  /** The host's status at the end of the left group. The model name gives
   *  up the room for it, so items don't hop into ⋯ as it comes and goes;
   *  only past the name's floor does it cost them their slot. */
  status?: number;
  gap?: number;
  overflowWidth?: number;
}): FoldResult {
  const visible = items.filter((i) => i.visible);
  const candidates = visible.filter((i) => i.slot !== 'right').map((i) => i.id);
  const right = visible.filter((i) => i.slot === 'right').map((i) => i.id);
  const measured = widths && Object.values(widths).some((w) => w > 0) ? widths : null;
  if (!containerWidth || !measured) {
    return { inline: new Set([...candidates, ...right]), folded: [], nameHidden: false };
  }

  const cost = (ids: readonly string[]) => ids.reduce((sum, id) => sum + (measured[id] ?? 0) + gap, 0);
  const trigger = gap + overflowWidth; // the ⋯ trigger itself

  const rightInline = [...right];
  const floorRow = () => fixedLeft + status + rightFloor + cost(rightInline)
    + (candidates.length > 0 || rightInline.length < right.length ? trigger : 0);
  while (rightInline.length > 0 && floorRow() > containerWidth) rightInline.pop();
  const rightFolded = right.filter((id) => !rightInline.includes(id));
  // With every right-slot item gone, this is the row with everything folded.
  const nameHidden = floorRow() > containerWidth;
  // Folding last ranks a right-slot item above every left one, so once one
  // has folded they all have.
  if (rightFolded.length > 0) {
    return { inline: new Set(rightInline), folded: [...candidates, ...rightFolded], nameHidden };
  }

  // The model name gives up room for the status, but only down to its floor.
  let budget = containerWidth - fixedLeft - cost(right) - Math.max(rightReserve, rightFloor + status);
  if (cost(candidates) <= budget) return { inline: new Set([...candidates, ...right]), folded: [], nameHidden };

  budget -= trigger;
  const inline = new Set<string>(right);
  for (const id of candidates) {
    const w = cost([id]);
    if (w > budget) break;
    inline.add(id);
    budget -= w;
  }
  return { inline, folded: candidates.filter((id) => !inline.has(id)), nameHidden };
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
  /** `rightReserve` with the model name at its floor. */
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
