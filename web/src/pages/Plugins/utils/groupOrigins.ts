/**
 * Origin grouping for the Plugins page lists. A plugin's origin is the repo
 * it was installed from (host + owner/repo), so several picks out of one
 * marketplace stack under one deck; zip installs group under "uploaded".
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
