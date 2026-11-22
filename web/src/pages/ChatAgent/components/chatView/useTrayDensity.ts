/**
 * Keeps the tray's row to one line: measures what each density needs and
 * holds the layout `chooseTrayLayout` picks.
 *
 * What it measures is the probe (`TrayProbe`): the same chips drawn unseen at
 * every density at their natural widths. The probe never depends on the
 * layout shown, so a choice cannot feed back into its own inputs, and the
 * row never has to be tried at one density to learn it does not fit.
 */
import { useEffect, useEffectEvent, useLayoutEffect, useState, type RefObject } from 'react';
import { flushSync } from 'react-dom';
import { INITIAL_TRAY_STATE, STEP_UP_HOLD_MS, stepTray, type ChipWidth, type MeasuredId, type TrayLayout, type TrayWidths } from './trayDensity';

function chipWidths(row: Element | null): ChipWidth[] {
  if (!row) return [];
  return [...row.children].flatMap((el) => (
    el instanceof HTMLElement && el.dataset.chip
      ? [{ id: el.dataset.chip as MeasuredId, width: el.getBoundingClientRect().width }]
      : []
  ));
}

function readWidths(row: HTMLElement | null, probe: HTMLElement | null): TrayWidths | null {
  if (!row || !probe) return null;
  const style = getComputedStyle(row);
  const available = row.getBoundingClientRect().width - (parseFloat(style.paddingLeft) || 0) - (parseFloat(style.paddingRight) || 0);
  // A row with no width is not laid out (hidden, or no layout at all): it
  // says nothing about what fits.
  if (!(available > 0)) return null;
  const level = (name: string) => probe.querySelector(`:scope > [data-level="${name}"]`);
  const rows = { full: chipWidths(level('full')), compact: chipWidths(level('compact')), tight: chipWidths(level('tight')) };
  const status = rows.tight.find((chip) => chip.id === 'status')?.width ?? 0;
  const more = chipWidths(level('more'))[0]?.width ?? status;
  return { rows, moreWidth: Math.max(0, more - status), available, gap: parseFloat(style.columnGap) || 0 };
}

export function useTrayDensity(
  rowRef: RefObject<HTMLElement | null>,
  probeRef: RefObject<HTMLElement | null>,
  { shown, hold }: {
    /** The row and the probe are mounted. */
    shown: boolean;
    /** Keep the layout as it is while the liquid runs from or to the shapes on screen. */
    hold: boolean;
  },
): TrayLayout {
  const [state, setState] = useState(INITIAL_TRAY_STATE);
  const measure = useEffectEvent((evenHeld = false) => {
    if (hold && !evenHeld) return;
    const widths = readWidths(rowRef.current, probeRef.current);
    if (!widths) return;
    const next = stepTray(state, widths, performance.now());
    if (next !== state) setState(next);
  });
  // A step up held back comes due when the hold from the last step down runs
  // out, so measure again then.
  const { down } = state;
  useEffect(() => {
    if (!down) return;
    const timer = window.setTimeout(() => measure(), Math.max(0, down.at + STEP_UP_HOLD_MS - performance.now()) + 1);
    return () => window.clearTimeout(timer);
  }, [down]);
  // Shown: measured before the first frame paints, since nothing may have
  // resized to say so, even under a hold. That is the unfold's first frame,
  // whose liquid then measures the chips as they settle.
  useLayoutEffect(() => {
    if (shown) measure(true);
  }, [shown]);
  // Let go: what changed while the liquid ran applies now.
  useLayoutEffect(() => {
    if (!hold) measure();
  }, [hold]);
  // The row resized, or a chip came, went or changed its width: each probe
  // level is as wide as its chips, so any change to one resizes it, whether
  // a commit here made it or a price ticked. Applied at once, so the frame
  // that resized never paints the row at the old layout.
  useLayoutEffect(() => {
    const row = rowRef.current;
    const probe = probeRef.current;
    if (!shown || !row || !probe) return;
    const observer = new ResizeObserver(() => flushSync(() => measure()));
    observer.observe(row);
    for (const level of probe.children) observer.observe(level);
    return () => observer.disconnect();
  }, [shown, rowRef, probeRef]);
  return state.layout;
}
