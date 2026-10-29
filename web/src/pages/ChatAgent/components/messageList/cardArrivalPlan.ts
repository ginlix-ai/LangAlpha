/**
 * The arithmetic of a card taking its row's place (CardArrivals), on plain
 * values: which place each card takes over, and how tall its box starts so the
 * list keeps the height it was drawn at.
 */

/** A row naming a landing card's call, as the commit that landed it left it. */
export interface RowAfterCommit<T> {
  row: T;
  /** The row's activity block, if the commit took it away: the row was its
   *  only item, and the block went with it. */
  goneBlock: T | null;
  /** The row left a block that stays, handing its place to the card (LiveRow). */
  handedOver: boolean;
}

/**
 * What a landing card takes the place of: the first row naming its call that
 * gave its place up, or the block that left with it. More than one row can
 * name a call (a status line stands for every call in its block), and the one
 * that hands over is the one that left. Null when every row stayed.
 */
export function pickSource<T>(rows: readonly RowAfterCommit<T>[]): T | null {
  for (const { row, goneBlock, handedOver } of rows) {
    if (goneBlock !== null) return goneBlock;
    if (handedOver) return row;
  }
  return null;
}

/** A landing card's box, as the commit drew it. */
export interface ArrivingBox {
  height: number;
  /** The gap its block opens above it, which the box may start short of. */
  gap: number;
  /** The place its card takes over, or null for one that took none. */
  source: { height: number } | null;
}

/**
 * Where each landing card's box starts, so the list keeps the height it was
 * drawn at: what the commit added (`grew`) is shared by the space each card's
 * row gave up (evenly when none did). A box never starts shorter than the gap
 * its block opened, nor taller than its row's space and that gap; a share one
 * cannot take passes to the others, and only what none can take moves the
 * list. Below zero, the box starts closed and its block's gap that much short.
 */
export function splitGrowth(boxes: readonly ArrivingBox[], grew: number): number[] {
  const starts = boxes.map((box) => box.height);
  const weight = (box: ArrivingBox) => box.source?.height ?? 0;
  let open = boxes.map((_, i) => i);
  let left = grew;
  while (open.length > 0) {
    const total = open.reduce((sum, i) => sum + weight(boxes[i]), 0);
    const share = (i: number) => (total > 0 ? weight(boxes[i]) / total : 1 / open.length);
    const bounded: number[] = [];
    for (const i of open) {
      const box = boxes[i];
      const low = -box.gap;
      const high = Math.max(box.height, weight(box) + box.gap);
      const start = box.height - left * share(i);
      starts[i] = Math.min(Math.max(start, low), high);
      if (starts[i] !== start) bounded.push(i);
    }
    if (bounded.length === 0) break;
    for (const i of bounded) left -= boxes[i].height - starts[i];
    open = open.filter((i) => !bounded.includes(i));
  }
  return starts;
}
