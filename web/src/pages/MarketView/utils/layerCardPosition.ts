/** A layer's explain card as placed beside the pointer: its size and margins. */
export interface CardMetrics {
  width: number;
  height: number;
  /** Between the pointer and the card. */
  gap: number;
  /** Kept clear of the host's edges. */
  edge: number;
}

/**
 * Where an explain card goes for a pointer at `(x, y)` in a `w`×`h` host: up
 * and to the left of it, since the OHLC tooltip owns down-right, else beside
 * or below it where the host's edge leaves no room.
 */
export function cardPosition(
  x: number,
  y: number,
  w: number,
  h: number,
  card: CardMetrics,
): { left: number; top: number } {
  let left = x - card.gap - card.width;
  if (left < card.edge) left = Math.min(x + card.gap, w - card.width - card.edge);
  let top = y - card.gap - card.height;
  if (top < card.edge) top = Math.min(y + card.gap, h - card.height - card.edge);
  return { left: Math.max(card.edge, left), top: Math.max(card.edge, top) };
}
