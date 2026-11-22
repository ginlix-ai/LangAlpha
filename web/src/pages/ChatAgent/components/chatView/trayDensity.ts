/**
 * How much the composer tray's chips say, so they keep to one row: every chip
 * in full while that fits, then shorter forms, then the least urgent chips
 * move into the Status button. Decided from measured widths alone, so the
 * rule holds without a layout to test it in.
 */
import type { TrayChipId } from './useTrayContents';

export type TrayDensity = 'full' | 'compact' | 'tight';

export const TRAY_DENSITIES: readonly TrayDensity[] = ['full', 'compact', 'tight'];

/** What the probe measures: every chip, and the Status button that ends the row. */
export type MeasuredId = TrayChipId | 'status';

/**
 * What the tray hands to the Status button once even the tight forms do not
 * fit, least urgent first. The plan, the subagents and the Analysts never go:
 * they are what runs now, or what waits on the reader.
 */
export const MOVE_ORDER: readonly TrayChipId[] = ['reportBack', 'wakeups', 'watch', 'commands'];

export interface TrayLayout {
  density: TrayDensity;
  /** The chips shown in the Status button instead of the row, in `MOVE_ORDER`. */
  moved: readonly TrayChipId[];
}

export const FULL_LAYOUT: TrayLayout = { density: 'full', moved: [] };

export interface ChipWidth {
  id: MeasuredId;
  width: number;
}

export interface TrayWidths {
  /** Each density's row in order, every chip at its natural width there. */
  rows: Readonly<Record<TrayDensity, readonly ChipWidth[]>>;
  /** What the Status button grows by when it counts the chips moved into it. */
  moreWidth: number;
  /** The row's own width. */
  available: number;
  gap: number;
}

/**
 * Room a more detailed layout must leave before the tray steps up to it. A
 * ticking price moves a chip's width by a digit's worth; without the margin a
 * row at the edge would switch forms with the ticks.
 */
export const STEP_UP_SLACK = 12;

/**
 * How long a row that stepped down keeps from stepping back up while its own
 * width holds. Two tickers crossing a digit boundary together move the watch
 * chip by more than any slack, so a price bouncing on one would otherwise flip
 * the row with every tick; held, it changes at most this often.
 */
export const STEP_UP_HOLD_MS = 5000;

export function rowWidth(widths: readonly number[], gap: number): number {
  return widths.length === 0 ? 0 : widths.reduce((sum, width) => sum + width, 0) + gap * (widths.length - 1);
}

/** Each layout the tray can take, most detailed first, with the width it needs. */
export function trayLayouts({ rows, moreWidth, gap }: Omit<TrayWidths, 'available'>): { layout: TrayLayout; need: number }[] {
  // In full the plan gives up width before the row does: its title truncates
  // down to what the compact form leaves it.
  const compactPlan = rows.compact.find((chip) => chip.id === 'plan')?.width;
  const full = rows.full.map((chip) => (chip.id === 'plan' && compactPlan != null ? Math.min(chip.width, compactPlan) : chip.width));
  const tight = rowWidth(rows.tight.map((chip) => chip.width), gap);
  const layouts = [
    { layout: FULL_LAYOUT, need: rowWidth(full, gap) },
    { layout: { density: 'compact', moved: [] } satisfies TrayLayout, need: rowWidth(rows.compact.map((chip) => chip.width), gap) },
    { layout: { density: 'tight', moved: [] } satisfies TrayLayout, need: tight },
  ];
  let freed = 0;
  const moved: TrayChipId[] = [];
  for (const id of MOVE_ORDER) {
    const chip = rows.tight.find((c) => c.id === id);
    if (!chip) continue;
    freed += chip.width + gap;
    moved.push(id);
    layouts.push({ layout: { density: 'tight', moved: [...moved] }, need: tight - freed + moreWidth });
  }
  return layouts;
}

/** How detailed a layout is: 0 for full, one more per density down and per chip moved. */
function layoutDetail(layout: TrayLayout): number {
  return TRAY_DENSITIES.indexOf(layout.density) + layout.moved.length;
}

/**
 * The most detailed layout whose chips fit on one row. One more detailed
 * than `current` must fit with `STEP_UP_SLACK` to spare; `current` and less
 * detailed ones need only fit. With `stepUp` off nothing more detailed than
 * `current` is taken (`STEP_UP_HOLD_MS`). When nothing fits, every chip that
 * can move has.
 */
export function chooseTrayLayout(widths: TrayWidths, current: TrayLayout, { stepUp = true }: { stepUp?: boolean } = {}): TrayLayout {
  const layouts = trayLayouts(widths);
  const at = layoutDetail(current);
  const fits = layouts.find(({ layout, need }) => {
    const up = layoutDetail(layout) < at;
    return (stepUp || !up) && need + (up ? STEP_UP_SLACK : 0) <= widths.available;
  });
  return (fits ?? layouts[layouts.length - 1]).layout;
}

function sameLayout(a: TrayLayout, b: TrayLayout): boolean {
  return a.density === b.density && a.moved.length === b.moved.length && a.moved.every((id, i) => id === b.moved[i]);
}

/** The layout shown, and the last step down while its hold may still count. */
export interface TrayDensityState {
  layout: TrayLayout;
  /** When the row last stepped down, and how wide it was then, or since its
   *  last step back up: a row wider than this has room the hold does not wait out. */
  down: { at: number; width: number } | null;
}

export const INITIAL_TRAY_STATE: TrayDensityState = { layout: FULL_LAYOUT, down: null };

/**
 * The next state for widths measured at `now`. A row that grew since its
 * last step steps up at once; when only the chips shrank, not until
 * `STEP_UP_HOLD_MS` after the step down, which a tick bouncing on a digit
 * cannot outlast. A step up spends the growth it used, so a later one that
 * only the chips' shrinking allows waits out the hold too. The same state
 * comes back when nothing changed, so a measure when a hold runs out is free.
 */
export function stepTray(state: TrayDensityState, widths: TrayWidths, now: number): TrayDensityState {
  const { layout, down } = state;
  const held = !!down && widths.available <= down.width && now < down.at + STEP_UP_HOLD_MS;
  const next = chooseTrayLayout(widths, layout, { stepUp: !held });
  const from = layoutDetail(layout);
  const to = layoutDetail(next);
  let nextDown = down;
  if (to > from) {
    nextDown = { at: now, width: widths.available };
  } else if (to < from && down) {
    // Up again: the width it took is spent, and the hold, while it lasts, still counts from the step down.
    nextDown = now < down.at + STEP_UP_HOLD_MS ? { at: down.at, width: widths.available } : null;
  }
  const same = sameLayout(next, layout);
  if (same && nextDown === down) return state;
  return { layout: same ? layout : next, down: nextDown };
}
