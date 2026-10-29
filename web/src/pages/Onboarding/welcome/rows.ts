/**
 * A turn's card coming up under the agent's line a row at a time. The card is
 * a column of options, a heading or two and a way on, and the whole column
 * fading up at once landed as one block the instant the line's last word did.
 *
 * Every turn's card is shaped differently, so its rows are read from the
 * layout: an element whose children stack is opened up, and one whose children
 * sit on a single line (an option, a chip row that fits, the footer) is a unit
 * of its own. Units that start on the same line (the chips of a wrapped group,
 * a two-column grid) are a row and rise together.
 */
import { cssEase, DURATION, EASE_OUT } from '@/lib/motion';

/** Between one row and the next, for a card with few rows. */
const STEP_MS = 40;
/** The longest the last row starts after the first: a tall card steps faster. */
const SPREAD_MS = 280;
/** Units this close at the top start on the same line. */
const SAME_LINE_PX = 4;
/** Never opened up: each is drawn as one thing. */
const LEAVES =
  'label, button, a, input, textarea, select, h1, h2, h3, h4, p, img, svg, canvas, [role="radio"], [role="checkbox"]';

const RISE: Keyframe[] = [
  { opacity: 0, transform: 'translateY(8px)' },
  { opacity: 1, transform: 'none' },
];

const shownChildren = (el: Element) =>
  [...el.children].filter(
    (child): child is HTMLElement => child instanceof HTMLElement && child.getClientRects().length > 0,
  );

const top = (el: Element) => el.getBoundingClientRect().top;

/** The units of `root`, in document order. */
export function unitsOf(root: HTMLElement): HTMLElement[] {
  const units: HTMLElement[] = [];
  const visit = (el: HTMLElement) => {
    const children = shownChildren(el);
    if (el !== root) {
      if (el.matches(LEAVES) || children.length === 0) {
        units.push(el);
        return;
      }
      if (children.length > 1 && children.every((child) => Math.abs(top(child) - top(children[0])) <= SAME_LINE_PX)) {
        units.push(el);
        return;
      }
    }
    children.forEach(visit);
  };
  visit(root);
  return units;
}

/** Units grouped by the line they start on, top to bottom. */
export function rowsOf<T>(units: readonly T[], topOf: (unit: T) => number): T[][] {
  const sorted = units.map((unit) => ({ unit, at: topOf(unit) })).sort((a, b) => a.at - b.at);
  const rows: T[][] = [];
  let rowTop = -Infinity;
  for (const { unit, at } of sorted) {
    if (at - rowTop > SAME_LINE_PX) {
      rows.push([]);
      rowTop = at;
    }
    rows[rows.length - 1].push(unit);
  }
  return rows;
}

/** When each of `rows` rows starts, after `lead`. */
export function rowDelays(rows: number, lead: number): number[] {
  const step = rows > 1 ? Math.min(STEP_MS, SPREAD_MS / (rows - 1)) : 0;
  return Array.from({ length: rows }, (_, i) => lead + i * step);
}

/** Brings `root`'s rows up in turn, the first after `lead` ms. */
export function riseInRows(root: HTMLElement, lead = 0): void {
  if (typeof root.animate !== 'function') return;
  const rows = rowsOf(unitsOf(root), top);
  const delays = rowDelays(rows.length, lead);
  const timing = {
    duration: DURATION.reveal * 1000,
    easing: cssEase(EASE_OUT),
    fill: 'backwards' as const,
  };
  rows.forEach((row, i) => {
    for (const unit of row) unit.animate(RISE, { ...timing, delay: delays[i] });
  });
}
