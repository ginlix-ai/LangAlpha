/**
 * Origin grouping for the Plugins page lists: what an origin is, how to group
 * rows by one, and the search predicate that decides which rows are grouped at
 * all. A plugin's origin is the repo it was installed from (host + owner/repo),
 * so several picks out of one marketplace stack under one deck; zip installs
 * group under "uploaded".
 */

export const UPLOADED_ORIGIN = 'uploaded';

/** Stack a group into a deck only past this row count; below it a plain
 * header reads faster than a collapsed stack. */
export const STACK_THRESHOLD = 4;

export function pluginSourceOrigin(plugin: {
  source_type: string;
  source_ref?: string | null;
}): string {
  if (plugin.source_type === 'zip' || !plugin.source_ref) return UPLOADED_ORIGIN;
  try {
    const url = new URL(plugin.source_ref);
    const segments = url.pathname.split('/').filter(Boolean);
    // owner/repo is the repo identity on every supported forge; deeper path
    // segments (tree/<ref>/<sub>) address one plugin inside it.
    const repo = segments.slice(0, 2).map((s) => s.replace(/\.git$/, ''));
    return [url.hostname, ...repo].join('/');
  } catch {
    return plugin.source_ref;
  }
}

/** Case-insensitive substring match over any of the row's text fields. */
export function matchesFilter(
  filter: string,
  ...fields: (string | null | undefined)[]
): boolean {
  const needle = filter.trim().toLowerCase();
  if (!needle) return true;
  return fields.some((f) => !!f && f.toLowerCase().includes(needle));
}

/**
 * Group rows by a derived key, input order preserved inside each group.
 *
 * Replaces the `m.set(k, [...(m.get(k) ?? []), row])` spread that had grown a
 * copy in each of the five grouping sites: it rebuilds a group's whole array
 * per row, so grouping N rows into one deck costs O(N^2).
 */
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
