/** Fill colors from an agent's colors: translucency added, or the agent's own kept within a cap. */

import { parseColor, withAlpha } from '../canvasKit';

/** Whether a CSS color already carries its own alpha (then a fill uses it as given). */
export function hasAlpha(color: string): boolean {
  return parseColor(color)?.a != null;
}

/**
 * A fill from an agent color: an opaque color gets our translucency, a color
 * that carries alpha is the agent's choice, kept up to `cap` so one heavy wash
 * cannot bury the candles under it.
 */
export function fillOf(color: string, alpha: number, cap = 0.2): string {
  const given = parseColor(color)?.a;
  if (given == null) return withAlpha(color, alpha);
  return given > cap ? withAlpha(color, cap) : color;
}
