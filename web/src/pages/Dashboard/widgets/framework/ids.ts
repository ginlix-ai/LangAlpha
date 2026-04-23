/**
 * Generate a new widget instance id. Used by the Add button, Duplicate flow,
 * and preset factories — all three previously had their own generator with
 * slightly different shapes.
 *
 * Collision-safe: preset factories generate 4+ ids synchronously, all sharing
 * the same `Date.now()` ms. We add a process-lifetime monotonic counter so
 * those sibling ids can never collide. A 4-char random tail still protects
 * against cross-tab re-seeding on reload.
 *
 * Layout: `<prefix>_<timestamp-base36>_<counter-base36>_<random-base36>`.
 */
let _seq = 0;
export function newWidgetId(prefix = 'w'): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return `${prefix}_${crypto.randomUUID()}`;
  }
  const ts = Date.now().toString(36);
  const seq = (++_seq).toString(36);
  const rand = Math.random().toString(36).slice(2, 6);
  return `${prefix}_${ts}_${seq}_${rand}`;
}
