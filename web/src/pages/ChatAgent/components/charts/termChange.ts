export type ChangeDirection = 'up' | 'down' | 'flat';

/** Under half a hundredth reads as no change: `0.00%` in gray, not a green gain. */
export function changeDirection(value: number | null | undefined): ChangeDirection {
  if (value == null || !Number.isFinite(value) || Math.abs(value) < 0.005) return 'flat';
  return value > 0 ? 'up' : 'down';
}

/**
 * A move in percent as every card prints one: its direction, the sign that
 * direction takes, and its size, `1.70%`. Signed it reads `+1.70%`, `-0.24%`,
 * and a move too small to count `0.00%`, unsigned; `TermChange` draws the
 * sign as an arrow. The sign is the hyphen-minus the money beside it prints,
 * so a copied figure reads as one.
 */
export function percentMove(value: number): { dir: ChangeDirection; sign: '+' | '-' | ''; size: string } {
  const dir = changeDirection(value);
  return { dir, sign: dir === 'up' ? '+' : dir === 'down' ? '-' : '', size: `${Math.abs(value).toFixed(2)}%` };
}
