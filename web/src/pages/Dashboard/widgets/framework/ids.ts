/**
 * Generate a new widget instance id. Used by the Add button, Duplicate flow,
 * and preset factories — all three previously had their own generator with
 * slightly different shapes.
 *
 * Layout: `<prefix>_<timestamp-base36>_<random-base36>`. Timestamp-first keeps
 * IDs roughly sortable by creation; the random suffix collapses collisions
 * when two IDs are generated in the same millisecond.
 */
export function newWidgetId(prefix = 'w'): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`;
}
