import { getWidget } from './WidgetRegistry';
import { BREAKPOINT_KEYS, COLS, clamp } from './gridConstants';
import type { BreakpointKey, RGLItem, WidgetInstance } from '../types';

/**
 * Keep `layouts[bp][].i` in sync with `widgets[].id`.
 * - Drops layout items whose widget no longer exists.
 * - For any widget that has no layout entry at a breakpoint, auto-places it at the bottom.
 * - Fills in min/max size constraints from widget definitions.
 */
export function reconcileLayouts(
  widgets: WidgetInstance[],
  layouts: Partial<Record<BreakpointKey, RGLItem[]>>
): Partial<Record<BreakpointKey, RGLItem[]>> {
  const widgetIds = new Set(widgets.map((w) => w.id));
  const out: Partial<Record<BreakpointKey, RGLItem[]>> = {};

  for (const bp of BREAKPOINT_KEYS) {
    const items = (layouts[bp] ?? []).filter((l) => widgetIds.has(l.i));
    const placed = new Set(items.map((l) => l.i));
    const result: RGLItem[] = items.map((l) => {
      const w = widgets.find((w) => w.id === l.i);
      if (!w) return l;
      const def = getWidget(w.type);
      if (!def) return l;
      const minW = def.minSize.w;
      const maxW = def.maxSize?.w;
      // For fit-to-content widgets, keep whatever minH/maxH was last written
      // by the fitHeight callback. Overwriting here would expand the manual
      // resize range on every reconcile and break the content-locked height.
      const minH = def.fitToContent && typeof l.minH === 'number' ? l.minH : def.minSize.h;
      const maxH = def.fitToContent && typeof l.maxH === 'number' ? l.maxH : def.maxSize?.h;
      return {
        ...l,
        w: clamp(l.w, minW, maxW),
        h: clamp(l.h, minH, maxH),
        minW,
        minH,
        maxW,
        maxH,
      };
    });

    for (const w of widgets) {
      if (placed.has(w.id)) continue;
      const def = getWidget(w.type);
      const dw = def?.defaultSize.w ?? 4;
      const dh = def?.defaultSize.h ?? 3;
      const maxY = result.reduce((m, l) => Math.max(m, l.y + l.h), 0);
      result.push({
        i: w.id,
        x: 0,
        y: maxY,
        w: dw,
        h: dh,
        minW: def?.minSize.w,
        minH: def?.minSize.h,
        maxW: def?.maxSize?.w,
        maxH: def?.maxSize?.h,
      });
    }

    out[bp] = result;
  }

  return out;
}

/**
 * Find a placement at the bottom of the grid for a new widget.
 */
export function placeAtBottom(
  layouts: Partial<Record<BreakpointKey, RGLItem[]>>,
  newId: string,
  w: number,
  h: number
): Partial<Record<BreakpointKey, RGLItem[]>> {
  const out: Partial<Record<BreakpointKey, RGLItem[]>> = {};
  for (const bp of BREAKPOINT_KEYS) {
    const items = layouts[bp] ?? [];
    const maxY = items.reduce((m, l) => Math.max(m, l.y + l.h), 0);
    out[bp] = [
      ...items,
      { i: newId, x: 0, y: maxY, w: Math.min(w, COLS), h },
    ];
  }
  return out;
}
