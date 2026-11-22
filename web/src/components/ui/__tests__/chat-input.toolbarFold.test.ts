// @vitest-environment node
/**
 * Composer toolbar fit path. Previously untestable — the math lived inside the
 * component and depended on a layout engine jsdom doesn't have; `computeFold`
 * is pure, so the contract can be pinned directly:
 *  1. no measurements → everything stays inline (jsdom / pre-paint);
 *  2. items fold from the TAIL of the declared priority order;
 *  3. the ⋯ trigger pays for itself out of the budget;
 *  4. the loop breaks on the first miss — a narrow low-priority item never
 *     jumps ahead of a wide high-priority one;
 *  5. the model pill gives way only once every item has folded, and its
 *     level goes before its name shrinks;
 *  6. past the name's floor, the name itself gives way to the logo.
 */
import { describe, it, expect } from 'vitest';
import { computeFold, TOOLBAR_GAP, ICON_BUTTON_W } from '../chat-input.useToolbarFold';

const ITEMS = [
  { id: 'mode', visible: true },
  { id: 'extra', visible: true },
  { id: 'watch', visible: true },
  { id: 'workspace', visible: true },
];

const WIDTHS = { mode: 60, extra: 60, watch: 70, workspace: 120 };

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
    expect([...r.inline].sort()).toEqual(['extra', 'mode', 'watch', 'workspace']);
    expect(r.folded).toEqual([]);
  });

  it('keeps everything inline when the container has not been measured yet', () => {
    const r = fold(0);
    expect(r.folded).toEqual([]);
  });

  it('treats an all-zero width map as unmeasured', () => {
    const r = computeFold({
      containerWidth: 100,
      widths: { mode: 0, extra: 0, watch: 0, workspace: 0 },
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
    const r = fold(widthFor(['mode', 'extra', 'watch', 'workspace']));
    expect(r.folded).toEqual([]);
  });

  it('folds the lowest-priority item first', () => {
    // One pixel short of fitting all four.
    const r = fold(widthFor(['mode', 'extra', 'watch', 'workspace']) - 1);
    expect(r.folded).toEqual(['workspace']);
    expect([...r.inline]).toEqual(['mode', 'extra', 'watch']);
  });

  it('folds from the tail as the container keeps shrinking', () => {
    expect(fold(widthFor(['mode', 'extra', 'watch'], { overflow: true })).folded).toEqual(['workspace']);
    expect(fold(widthFor(['mode', 'extra'], { overflow: true })).folded).toEqual(['watch', 'workspace']);
    expect(fold(widthFor(['mode'], { overflow: true })).folded).toEqual(['extra', 'watch', 'workspace']);
    expect(fold(widthFor([], { overflow: true })).folded).toEqual(['mode', 'extra', 'watch', 'workspace']);
  });

  it('charges the ⋯ trigger to the budget', () => {
    // Enough room for all four pills, but not for three pills + the trigger.
    const w = widthFor(['mode', 'extra', 'watch']) + TOOLBAR_GAP;
    expect(fold(w).folded).toEqual(['watch', 'workspace']);
  });

  it('subtracts fixed chrome from the budget', () => {
    const w = widthFor(['mode', 'extra'], { overflow: true });
    expect(fold(w).folded).toEqual(['watch', 'workspace']);
    expect(fold(w, { fixedLeft: 40 }).folded).toEqual(['extra', 'watch', 'workspace']);
    expect(fold(w, { rightReserve: 40 }).folded).toEqual(['extra', 'watch', 'workspace']);
  });

  it('breaks at the first miss instead of packing a narrower later item', () => {
    // Room for `mode` + a 60px pill, but `extra` here is the wide one: priority
    // wins, so `watch` does NOT get promoted into the leftover space.
    const widths = { mode: 60, extra: 200, watch: 60, workspace: 60 };
    const r = computeFold({
      containerWidth: 60 + TOOLBAR_GAP + 60 + TOOLBAR_GAP + TOOLBAR_GAP + ICON_BUTTON_W,
      widths,
      items: ITEMS,
      fixedLeft: 0,
      rightReserve: 0,
    });
    expect([...r.inline]).toEqual(['mode']);
    expect(r.folded).toEqual(['extra', 'watch', 'workspace']);
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

describe('computeFold: the model pill', () => {
  // Left: one 40px toggle. Right: the model pill, 130 with its level, 60
  // with no level and the name at its floor.
  const items = [{ id: 'toggle', visible: true }];
  const widths = { toggle: 40 };
  const toggle = 40 + TOOLBAR_GAP;
  const trigger = TOOLBAR_GAP + ICON_BUTTON_W;
  const fit = (containerWidth: number, status = 0) => computeFold({
    containerWidth, widths, items, fixedLeft: 0, rightReserve: 130, rightFloor: 60, status,
  });

  it('keeps the level while the row holds the whole pill beside the items', () => {
    expect(fit(130 + toggle)).toMatchObject({ folded: [], levelHidden: false, nameHidden: false });
  });

  it('folds the items before the level goes', () => {
    const r = fit(130 + toggle - 1);
    expect(r.folded).toEqual(['toggle']);
    expect(r.levelHidden).toBe(false);
  });

  it('drops the level before the name shrinks', () => {
    // Everything folded, a pixel short of the whole pill: the name keeps its
    // room and the level goes.
    expect(fit(130 + trigger - 1)).toMatchObject({ folded: ['toggle'], levelHidden: true, nameHidden: false });
  });

  it('lets a host status take the level, not an item', () => {
    // The row holds the toggle beside the pill at its floor, so the 30px
    // status costs the level its room rather than the toggle its slot.
    expect(fit(130 + toggle, 30)).toMatchObject({ folded: [], levelHidden: true });
  });

  it('hides the model name only once the folded row cannot hold it at its floor', () => {
    expect(fit(60 + trigger)).toMatchObject({ folded: ['toggle'], levelHidden: true, nameHidden: false });
    expect(fit(60 + trigger - 1).nameHidden).toBe(true);
  });

  it('charges no ⋯ trigger when there are no items', () => {
    const fold = (containerWidth: number) => computeFold({
      containerWidth, widths: { model: 130 }, items: [], fixedLeft: 0, rightReserve: 130, rightFloor: 60,
    });
    expect(fold(130)).toMatchObject({ levelHidden: false, nameHidden: false });
    expect(fold(129)).toMatchObject({ levelHidden: true, nameHidden: false });
    expect(fold(59).nameHidden).toBe(true);
  });
});
