/**
 * Composer toolbar fit path. Previously untestable — the math lived inside the
 * component and depended on a layout engine jsdom doesn't have; `computeFold`
 * is pure, so the contract can be pinned directly:
 *  1. no measurements → everything stays inline (jsdom / pre-paint);
 *  2. items fold from the TAIL of the declared priority order;
 *  3. the ⋯ trigger pays for itself out of the budget;
 *  4. the loop breaks on the first miss — a narrow low-priority item never
 *     jumps ahead of a wide high-priority one.
 */
import { describe, it, expect } from 'vitest';
import { computeFold, TOOLBAR_GAP, ICON_BUTTON_W } from '../chat-input.useToolbarFold';

const ITEMS = [
  { id: 'mode', visible: true },
  { id: 'plan', visible: true },
  { id: 'watch', visible: true },
  { id: 'workspace', visible: true },
];

const WIDTHS = { mode: 60, plan: 60, watch: 70, workspace: 120, model: 100 };

/** Width that exactly fits `ids` inline (plus the ⋯ trigger when folding). */
const widthFor = (ids: string[], opts: { overflow?: boolean } = {}) =>
  ids.reduce((sum, id) => sum + WIDTHS[id as keyof typeof WIDTHS] + TOOLBAR_GAP, 0)
  + (opts.overflow ? TOOLBAR_GAP + ICON_BUTTON_W : 0);

const fold = (containerWidth: number, over: Partial<Parameters<typeof computeFold>[0]> = {}) =>
  computeFold({
    containerWidth,
    widths: WIDTHS,
    items: ITEMS,
    fixedLeft: 0,
    rightReserve: 0,
    ...over,
  });

describe('computeFold', () => {
  it('keeps everything inline when nothing is measured', () => {
    const r = computeFold({ containerWidth: 100, widths: null, items: ITEMS, fixedLeft: 0, rightReserve: 0 });
    expect([...r.inline].sort()).toEqual(['mode', 'plan', 'watch', 'workspace']);
    expect(r.folded).toEqual([]);
  });

  it('keeps everything inline when the container has not been measured yet', () => {
    const r = fold(0);
    expect(r.folded).toEqual([]);
  });

  it('treats an all-zero width map as unmeasured', () => {
    const r = computeFold({
      containerWidth: 100,
      widths: { mode: 0, plan: 0, watch: 0, workspace: 0 },
      items: ITEMS,
      fixedLeft: 0,
      rightReserve: 0,
    });
    expect(r.folded).toEqual([]);
  });

  it('skips invisible items entirely', () => {
    const r = computeFold({
      containerWidth: 10_000,
      widths: WIDTHS,
      items: [{ id: 'mode', visible: true }, { id: 'watch', visible: false }],
      fixedLeft: 0,
      rightReserve: 0,
    });
    expect([...r.inline]).toEqual(['mode']);
    expect(r.folded).toEqual([]);
  });

  it('keeps every item inline when they all fit — no ⋯ trigger cost', () => {
    const r = fold(widthFor(['mode', 'plan', 'watch', 'workspace']));
    expect(r.folded).toEqual([]);
  });

  it('folds the lowest-priority item first', () => {
    // One pixel short of fitting all four.
    const r = fold(widthFor(['mode', 'plan', 'watch', 'workspace']) - 1);
    expect(r.folded).toEqual(['workspace']);
    expect([...r.inline]).toEqual(['mode', 'plan', 'watch']);
  });

  it('folds from the tail as the container keeps shrinking', () => {
    expect(fold(widthFor(['mode', 'plan', 'watch'], { overflow: true })).folded).toEqual(['workspace']);
    expect(fold(widthFor(['mode', 'plan'], { overflow: true })).folded).toEqual(['watch', 'workspace']);
    expect(fold(widthFor(['mode'], { overflow: true })).folded).toEqual(['plan', 'watch', 'workspace']);
    expect(fold(widthFor([], { overflow: true })).folded).toEqual(['mode', 'plan', 'watch', 'workspace']);
  });

  it('charges the ⋯ trigger to the budget', () => {
    // Enough room for all four pills, but not for three pills + the trigger.
    const w = widthFor(['mode', 'plan', 'watch']) + TOOLBAR_GAP;
    expect(fold(w).folded).toEqual(['watch', 'workspace']);
  });

  it('subtracts fixed chrome from the budget', () => {
    const w = widthFor(['mode', 'plan'], { overflow: true });
    expect(fold(w).folded).toEqual(['watch', 'workspace']);
    expect(fold(w, { fixedLeft: 40 }).folded).toEqual(['plan', 'watch', 'workspace']);
    expect(fold(w, { rightReserve: 40 }).folded).toEqual(['plan', 'watch', 'workspace']);
  });

  it('breaks at the first miss instead of packing a narrower later item', () => {
    // Room for `mode` + a 60px pill, but `plan` here is the wide one: priority
    // wins, so `watch` does NOT get promoted into the leftover space.
    const widths = { mode: 60, plan: 200, watch: 60, workspace: 60 };
    const r = computeFold({
      containerWidth: 60 + TOOLBAR_GAP + 60 + TOOLBAR_GAP + TOOLBAR_GAP + ICON_BUTTON_W,
      widths,
      items: ITEMS,
      fixedLeft: 0,
      rightReserve: 0,
    });
    expect([...r.inline]).toEqual(['mode']);
    expect(r.folded).toEqual(['plan', 'watch', 'workspace']);
  });

  it('treats an unmeasured item as zero-width rather than dropping it', () => {
    const r = computeFold({
      containerWidth: 10_000,
      widths: { mode: 60 },
      items: ITEMS,
      fixedLeft: 0,
      rightReserve: 0,
    });
    expect(r.folded).toEqual([]);
  });
});
