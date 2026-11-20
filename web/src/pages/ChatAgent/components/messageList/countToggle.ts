import { createContext, useEffect, useLayoutEffect, useState } from 'react';
import { animate } from '@/lib/framer';
import { EASE_COLLAPSE, SPRING_FOLD } from '@/lib/motion';
import { tallClose } from './transcriptMotion';

/**
 * A row the reader's toggle brings or sends away, as the panel moves it. The
 * row answers `paint` and `land` only while this is the panel it last joined,
 * so one that has gone, or joined again mid-fold, needs no word from the fold.
 */
export interface PanelRow {
  box: HTMLElement;
  opening: boolean;
  /** The row's height when the toggle lands and the one it is going to, in px. */
  from: number;
  to: number;
  /** How much of the content showed when the toggle landed, 0 to 1: a row
   *  caught mid-close reopens from where it had dimmed to. */
  shown: number;
  paint: (height: number, shown: number) => void;
  /** The row is where it was going. */
  land: () => void;
}

const sum = (values: number[]) => values.reduce((total, value) => total + value, 0);

/** Moves `rows`, in list order, as one box opening or shutting. */
function movePanel(rows: PanelRow[], opening: boolean): () => void {
  if (rows.length === 0) return () => {};
  const sizes = rows.map((row) => (opening ? row.to : row.from));
  const from = sum(rows.map((row) => row.from));
  const to = opening ? sum(sizes) : 0;
  // A stopped tween ticks once more as it stops.
  let live = true;
  // Let go of once landed: a row the fold sent away has left the list, and
  // the block keeps this fold's stop until its next toggle.
  let moving = rows;
  const land = () => {
    live = false;
    for (const row of moving) row.land();
    moving = [];
  };
  const controls = animate(from, to, {
    ...(opening ? SPRING_FOLD : { duration: tallClose(from), ease: EASE_COLLAPSE }),
    onUpdate: (edge) => {
      if (!live) return;
      const done = to === from ? 1 : (edge - from) / (to - from);
      let above = 0;
      moving.forEach((row, i) => {
        row.paint(Math.min(sizes[i], Math.max(0, edge - above)), opening ? row.shown + (1 - row.shown) * done : row.shown * (1 - done));
        above += sizes[i];
      });
    },
    onComplete: () => {
      if (live) land();
    },
  });
  return () => {
    if (!live) return;
    live = false;
    controls.stop();
    // Rows the next fold did not take are where they were going.
    land();
  };
}

/**
 * The rows the reader's toggle brings or sends away, moved as one panel: the
 * `Disclosure` the transcript's other folds are, drawn across rows that each
 * keep a box of their own, since a step never changes parent between the live
 * rows and the settled ones. One height runs from shut to the rows' sum on the
 * fold spring and is dealt to the rows top down, so the content stands where
 * it will rest and the edge passes down over it, the rows under the panel
 * riding that edge. Closing, the edge comes back up on the collapse curve and
 * the content dims with what is left.
 *
 * Each row moving on its own opened every box at once with nothing in it and
 * brought the content in after, so a count of twelve steps opened a hole of
 * twelve rows and filled it a beat later, and closing emptied the rows before
 * anything under them moved.
 */
function createPanelFold() {
  let joined: PanelRow[] = [];
  let stop = () => {};
  return {
    /** From a row's layout effect, in the commit of the toggle. */
    join(row: PanelRow) {
      joined.push(row);
    },
    /** Once the rows have joined. A toggle that lands on a fold still moving
     *  takes its rows from where they are. */
    start() {
      const rows = joined.sort((a, b) => (a.box.compareDocumentPosition(b.box) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1));
      joined = [];
      stop();
      const stops = [true, false].map((opening) => movePanel(rows.filter((row) => row.opening === opening), opening));
      stop = () => stops.forEach((end) => end());
    },
  };
}

/**
 * What a block tells its rows for the commit in which the reader opens or
 * closes its count. The rows that commit brings or sends away fold as one
 * panel. The step the status line names is on screen either way, on the line
 * while the count is closed and in a row of its own once it is opened: the row
 * taking it over is there at once and the one giving it up is gone at once
 * (LiveRow), so the step stays put while the finished steps unfold over it or
 * fold away.
 */
export interface CountToggle {
  panel: ReturnType<typeof createPanelFold>;
  /** The step moving from one row to another, by row key. */
  step: string | null;
  /** The height of the row that drew the step until the toggle: the line's
   *  row and a step's own differ by a few px, and the step eases from the one
   *  to the other. */
  stepHeight: number | null;
}

export const CountToggleContext = createContext<CountToggle | null>(null);

/** The height of the row drawing `step` in the block that holds `from`. Read
 *  at the click: the rows' own effects run in list order, so the row taking
 *  the step can come before the one giving it up. */
function stepRowHeight(from: HTMLElement, step: string): number | null {
  const rows = from.closest('[data-activity-block]')?.querySelectorAll<HTMLElement>('[data-step-row]') ?? [];
  // A row the last toggle sent away may still be there, shut.
  const heights = [...rows]
    .filter((row) => row.dataset.stepRow?.split(' ').includes(step))
    .map((row) => row.getBoundingClientRect().height);
  return heights.length > 0 ? Math.max(...heights) : null;
}

/**
 * A block's count toggle: what to hand `CountToggleContext`, and what the
 * count's button calls as it is pressed. `lineStep` is the step on the status
 * line, if one is up.
 */
export function useCountToggle(lineStep: string | null) {
  // Named for the one commit: left named, the line going away at the turn's
  // end would be gone with no fold.
  const [toggled, setToggled] = useState<Pick<CountToggle, 'step' | 'stepHeight'> | null>(null);
  useEffect(() => { if (toggled) setToggled(null); }, [toggled]);
  const [panel] = useState(createPanelFold);
  // The rows join from their own layout effects, which run before this one.
  useLayoutEffect(() => { if (toggled) panel.start(); }, [toggled, panel]);
  return {
    toggle: toggled && { ...toggled, panel },
    noteToggle: (button: HTMLElement) => setToggled({
      step: lineStep,
      stepHeight: lineStep === null ? null : stepRowHeight(button, lineStep),
    }),
  };
}
