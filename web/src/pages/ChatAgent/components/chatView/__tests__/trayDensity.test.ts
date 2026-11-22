// @vitest-environment node
/**
 * The tray's one-row rule: the most detailed layout that fits, chips moved
 * into Status in their order once even the tight forms do not, and a margin
 * before stepping back up so a ticking price cannot flip the row.
 */
import { describe, it, expect } from 'vitest';
import {
  chooseTrayLayout,
  FULL_LAYOUT,
  INITIAL_TRAY_STATE,
  rowWidth,
  STEP_UP_HOLD_MS,
  STEP_UP_SLACK,
  stepTray,
  trayLayouts,
  type TrayDensity,
  type TrayDensityState,
  type TrayLayout,
  type TrayWidths,
} from '../trayDensity';
import type { TrayChipId } from '../useTrayContents';

type Row = Partial<Record<TrayChipId, number>>;

const GAP = 6;
const STATUS = 26;
/** What "+N" adds to the Status button. */
const MORE = 14;

function widths(rows: Record<TrayDensity, Row>, available: number): TrayWidths {
  const toRow = (row: Row) => [
    ...Object.entries(row).map(([id, width]) => ({ id: id as TrayChipId, width })),
    { id: 'status' as const, width: STATUS },
  ];
  return {
    rows: { full: toRow(rows.full), compact: toRow(rows.compact), tight: toRow(rows.tight) },
    moreWidth: MORE,
    available,
    gap: GAP,
  };
}

// As measured on a real thread: a watch on two tickers, a price wakeup, a
// running plan and running subagents.
const THREAD: Record<TrayDensity, Row> = {
  full: { plan: 325, watch: 324, wakeups: 199, tasks: 110 },
  compact: { plan: 180, watch: 170, wakeups: 120, tasks: 110 },
  tight: { plan: 70, watch: 170, wakeups: 60, tasks: 50 },
};

const need = (row: Row) => rowWidth([...Object.values(row), STATUS], GAP);
const compact: TrayLayout = { density: 'compact', moved: [] };
const tight: TrayLayout = { density: 'tight', moved: [] };

describe('chooseTrayLayout', () => {
  it('keeps every chip in full while they fit', () => {
    expect(chooseTrayLayout(widths(THREAD, 2000), FULL_LAYOUT)).toEqual(FULL_LAYOUT);
  });

  it('shortens the chips on the measured thread rather than wrap', () => {
    expect(chooseTrayLayout(widths(THREAD, 736), FULL_LAYOUT)).toEqual(compact);
  });

  it('counts the plan in full at the width compact leaves it, since its title truncates first', () => {
    const fullWithShortPlan = need({ ...THREAD.full, plan: THREAD.compact.plan });
    expect(chooseTrayLayout(widths(THREAD, fullWithShortPlan), FULL_LAYOUT)).toEqual(FULL_LAYOUT);
    expect(chooseTrayLayout(widths(THREAD, fullWithShortPlan - 1), FULL_LAYOUT)).toEqual(compact);
  });

  it('goes tight when compact does not fit', () => {
    expect(chooseTrayLayout(widths(THREAD, need(THREAD.tight)), FULL_LAYOUT)).toEqual(tight);
  });

  it('moves the least urgent chips into Status in order, skipping those not there', () => {
    const rows: Record<TrayDensity, Row> = {
      full: { plan: 300, watch: 300, wakeups: 200, commands: 150, tasks: 150, analysts: 150, reportBack: 200 },
      compact: { plan: 200, watch: 200, wakeups: 150, commands: 150, tasks: 150, analysts: 150, reportBack: 200 },
      tight: { plan: 70, watch: 150, wakeups: 60, commands: 50, tasks: 50, analysts: 50, reportBack: 100 },
    };
    const tightNeed = need(rows.tight);
    const after = (...ids: TrayChipId[]) => tightNeed + MORE - ids.reduce((sum, id) => sum + rows.tight[id]! + GAP, 0);
    expect(chooseTrayLayout(widths(rows, after('reportBack')), FULL_LAYOUT).moved).toEqual(['reportBack']);
    expect(chooseTrayLayout(widths(rows, after('reportBack', 'wakeups')), FULL_LAYOUT).moved).toEqual(['reportBack', 'wakeups']);
    expect(chooseTrayLayout(widths(rows, after('reportBack', 'wakeups', 'watch')), FULL_LAYOUT).moved)
      .toEqual(['reportBack', 'wakeups', 'watch']);
    // Without a wakeup, the watch is next after the report-back label.
    const { wakeups: _full, ...noWakeups } = rows.tight;
    const without: Record<TrayDensity, Row> = { full: rows.full, compact: rows.compact, tight: noWakeups };
    const fit = need(noWakeups) + MORE - (rows.tight.reportBack! + GAP) - (rows.tight.watch! + GAP);
    expect(chooseTrayLayout(widths(without, fit), FULL_LAYOUT).moved).toEqual(['reportBack', 'watch']);
  });

  it('never moves the plan, the subagents, the Analysts or the Status button, even when nothing fits', () => {
    const layout = chooseTrayLayout(widths(THREAD, 10), FULL_LAYOUT);
    expect(layout).toEqual({ density: 'tight', moved: ['wakeups', 'watch'] });
    expect(trayLayouts(widths(THREAD, 10)).flatMap(({ layout: l }) => l.moved))
      .not.toEqual(expect.arrayContaining(['plan', 'tasks', 'analysts', 'status']));
  });

  it('counts what the "+N" adds to the Status button', () => {
    const exactlyTight = need(THREAD.tight);
    // Moving the wakeups frees its width and a gap, and the button grows by MORE.
    const moved = exactlyTight - THREAD.tight.wakeups! - GAP + MORE;
    expect(chooseTrayLayout(widths(THREAD, moved), FULL_LAYOUT).moved).toEqual(['wakeups']);
    expect(chooseTrayLayout(widths(THREAD, moved - 1), FULL_LAYOUT).moved).toEqual(['wakeups', 'watch']);
  });

  it('steps back up only with room to spare, so a ticking price cannot flip the row', () => {
    const full = need({ ...THREAD.full, plan: THREAD.compact.plan });
    // Shown compact: full fitting by less than the margin keeps it compact.
    expect(chooseTrayLayout(widths(THREAD, full + STEP_UP_SLACK - 1), compact)).toEqual(compact);
    expect(chooseTrayLayout(widths(THREAD, full + STEP_UP_SLACK), compact)).toEqual(FULL_LAYOUT);
    // Shown full, it stays full as long as it fits at all.
    expect(chooseTrayLayout(widths(THREAD, full), FULL_LAYOUT)).toEqual(FULL_LAYOUT);
    // A price a few pixels wider each tick, then narrower: the row holds.
    let layout = FULL_LAYOUT;
    for (const delta of [3, -3, 3, -3]) {
      const ticked = { ...THREAD, full: { ...THREAD.full, watch: THREAD.full.watch! + delta } };
      layout = chooseTrayLayout(widths(ticked, full + 1), layout);
    }
    // The first wider tick stepped down; the narrower ones never brought it back up.
    expect(layout).toEqual(compact);
  });

  it('steps back from moved chips the same way', () => {
    const moved: TrayLayout = { density: 'tight', moved: ['wakeups'] };
    const exactlyTight = need(THREAD.tight);
    expect(chooseTrayLayout(widths(THREAD, exactlyTight + STEP_UP_SLACK - 1), moved)).toEqual(moved);
    expect(chooseTrayLayout(widths(THREAD, exactlyTight + STEP_UP_SLACK), moved)).toEqual(tight);
  });

  it('holds its level against a swing wider than the margin while step-ups are held, but still steps down', () => {
    const full = need({ ...THREAD.full, plan: THREAD.compact.plan });
    // Two tickers crossing a digit boundary together: well past the margin.
    const swing = STEP_UP_SLACK * 2;
    const roomy = widths(THREAD, full + swing);
    expect(chooseTrayLayout(roomy, compact)).toEqual(FULL_LAYOUT);
    expect(chooseTrayLayout(roomy, compact, { stepUp: false })).toEqual(compact);
    expect(chooseTrayLayout(widths(THREAD, need(THREAD.tight)), compact, { stepUp: false })).toEqual(tight);
  });
});

describe('stepTray: holding a step back up', () => {
  const fullNeed = need({ ...THREAD.full, plan: THREAD.compact.plan });
  const roomy = fullNeed + STEP_UP_SLACK;
  /** The watch chip wider by `by` in full, as a price gaining digits would make it. */
  const wider = (by: number) => ({ ...THREAD, full: { ...THREAD.full, watch: THREAD.full.watch! + by } });
  // The full row stepped down to compact at t=0 at `roomy`, the watch then 40px wider.
  function steppedDown(width = roomy): TrayDensityState {
    const state = stepTray(INITIAL_TRAY_STATE, widths(wider(40), width), 0);
    expect(state.layout).toEqual(compact);
    return state;
  }

  it('returns the same state when nothing changes', () => {
    const state = steppedDown();
    expect(stepTray(state, widths(wider(40), roomy), 10)).toBe(state);
  });

  it('holds a step up the chips alone allow until the hold from the step down runs out', () => {
    const state = steppedDown();
    const early = stepTray(state, widths(THREAD, roomy), 1000);
    expect(early).toBe(state);
    // Due when the hold runs out, counted from the step down.
    expect(stepTray(early, widths(THREAD, roomy), state.down!.at + STEP_UP_HOLD_MS - 1)).toBe(state);
    expect(stepTray(early, widths(THREAD, roomy), state.down!.at + STEP_UP_HOLD_MS).layout).toEqual(FULL_LAYOUT);
  });

  it('steps up at once when the row grew since it stepped down', () => {
    const state = steppedDown();
    expect(stepTray(state, widths(THREAD, roomy + 1), 1000).layout).toEqual(FULL_LAYOUT);
  });

  it('spends the growth on the step up it made, holding a further one the chips alone allow', () => {
    // Stepped all the way down to tight, at the tight row's own width.
    const tightNeed = need(THREAD.tight);
    const start = stepTray(INITIAL_TRAY_STATE, widths(THREAD, tightNeed), 0);
    expect(start.layout).toEqual(tight);
    // The row grows to where compact fits with the watch 200px wider, but
    // full does not: up to compact at once.
    const widerWatch = {
      ...THREAD,
      full: { ...THREAD.full, watch: THREAD.full.watch! + 200 },
      compact: { ...THREAD.compact, watch: THREAD.compact.watch! + 200 },
    };
    const grown = stepTray(start, widths(widerWatch, roomy), 1000);
    expect(grown.layout).toEqual(compact);
    // The watch shrinks back: full now fits at the same width, but only the
    // chips changed, so it waits out the hold from the step down.
    const fits = widths(THREAD, roomy);
    expect(chooseTrayLayout(fits, compact)).toEqual(FULL_LAYOUT);
    const held = stepTray(grown, fits, 2000);
    expect(held.layout).toEqual(compact);
    // Still counted from the step down at 0, not the step up at 1000.
    expect(held.down?.at).toBe(0);
    expect(stepTray(held, fits, STEP_UP_HOLD_MS).layout).toEqual(FULL_LAYOUT);
  });

  it('steps down at once while a step up is held', () => {
    const state = steppedDown();
    const down = stepTray(state, widths(THREAD, need(THREAD.tight)), 100);
    expect(down.layout).toEqual(tight);
    expect(down.down).toEqual({ at: 100, width: need(THREAD.tight) });
  });

  it('a bouncing price flips the row at most once per hold', () => {
    let state = INITIAL_TRAY_STATE;
    const layouts: TrayLayout[] = [];
    for (let i = 0; i < 10; i++) {
      state = stepTray(state, widths(i % 2 ? THREAD : wider(40), roomy), i * 400);
      layouts.push(state.layout);
    }
    expect(layouts.every((layout) => layout.density === 'compact')).toBe(true);
  });
});

describe('rowWidth', () => {
  it('adds a gap between chips, none at the ends', () => {
    expect(rowWidth([], GAP)).toBe(0);
    expect(rowWidth([40], GAP)).toBe(40);
    expect(rowWidth([40, 60, 26], GAP)).toBe(138);
  });
});
