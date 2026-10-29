/**
 * The list helpers the Plugins page's lists share: the search predicate that
 * decides which rows show, and grouping rows under a derived key.
 */

/** Case-insensitive substring match over any of the row's text fields. */
export function matchesFilter(
  filter: string,
  ...fields: (string | null | undefined)[]
): boolean {
  const needle = filter.trim().toLowerCase();
  if (!needle) return true;
  return fields.some((f) => !!f && f.toLowerCase().includes(needle));
}

/** Group rows by a derived key, input order preserved inside each group. One
 *  array per group, appended in place: a spread per row costs O(N^2). */
export function groupBy<T, K>(items: readonly T[], keyOf: (item: T) => K): Map<K, T[]> {
  const groups = new Map<K, T[]>();
  for (const item of items) {
    const key = keyOf(item);
    const bucket = groups.get(key);
    if (bucket) bucket.push(item);
    else groups.set(key, [item]);
  }
  return groups;
}
