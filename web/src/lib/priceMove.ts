/**
 * A price move's direction, color and printed size, shared by the chat's
 * cards and watch, the Status panel and the menu bar, so a move reads the same
 * wherever it shows. Apart from `marketUtils`, which the entry reaches: these
 * are read only by lazy routes.
 */
export type ChangeDirection = 'up' | 'down' | 'flat';

/** Under half the last place printed reads as no change: `0.00%` in gray, not a green gain. */
export function changeDirection(value: number | null | undefined, places = 2): ChangeDirection {
  if (value == null || !Number.isFinite(value) || Math.abs(value) < 0.5 / 10 ** places) return 'flat';
  return value > 0 ? 'up' : 'down';
}

/** A direction's color: gain, loss, or none for flat. */
export function directionTone(dir: ChangeDirection): string {
  return dir === 'up' ? 'var(--color-profit)' : dir === 'down' ? 'var(--color-loss)' : 'var(--color-text-tertiary)';
}

/** A move's color: gain, loss, or none for flat and unknown. */
export function moveTone(pct: number | null | undefined, places = 2): string {
  return directionTone(changeDirection(pct, places));
}

/**
 * A move in percent as every card prints one: its direction, the sign that
 * direction takes, and its size, `1.70%`. Signed it reads `+1.70%`, `-0.24%`,
 * and a move too small to count `0.00%`, unsigned; `TermChange` draws the
 * sign as an arrow. The sign is the hyphen-minus the money beside it prints,
 * so a copied figure reads as one.
 */
export function percentMove(value: number, places = 2): { dir: ChangeDirection; sign: '+' | '-' | ''; size: string } {
  const dir = changeDirection(value, places);
  return { dir, sign: dir === 'up' ? '+' : dir === 'down' ? '-' : '', size: `${Math.abs(value).toFixed(places)}%` };
}

/** A move as the menu bar and a chat's watch print it: `+0.78%`, `-1.20%`,
 *  `0.00%`; a tray short of room prints one place, `-14.0%`. */
export function signedPercent(value: number, places = 2): string {
  const { sign, size } = percentMove(value, places);
  return `${sign}${size}`;
}
