import { resolveFreshnessBadge, type FreshnessFields } from '@/lib/freshness';

type SourcedFreshness = FreshnessFields & { source?: string | null };

function freshnessKey(row: FreshnessFields): string | null {
  const spec = resolveFreshnessBadge(row);
  return spec ? `${spec.key}:${spec.minutes ?? ''}` : null;
}

function sourceOf(row: SourcedFreshness): string | null {
  return row.source ?? row.freshness?.source ?? null;
}

/**
 * How current a card's rows are, said once: the state most rows share goes in
 * the header, and a row keeps its own mark only where it differs, so one stale
 * symbol among live ones still shows. Rows with no freshness say nothing
 * either way.
 *
 * `common` is what the header's tag shows. It names a source only when every
 * row it speaks for shares one: a batch the backend split across providers
 * must not credit them all to the first.
 */
export function commonFreshness<T extends SourcedFreshness>(
  rows: readonly T[],
): { common: SourcedFreshness | null; differs: (row: T) => boolean } {
  const groups = new Map<string, T[]>();
  for (const row of rows) {
    const key = freshnessKey(row);
    if (key == null) continue;
    const group = groups.get(key);
    if (group) group.push(row);
    else groups.set(key, [row]);
  }
  let commonKey: string | null = null;
  let members: T[] = [];
  for (const [key, group] of groups) {
    if (group.length > members.length) {
      commonKey = key;
      members = group;
    }
  }
  const differs = (row: T) => {
    const key = freshnessKey(row);
    return key != null && key !== commonKey;
  };
  if (!members.length) return { common: null, differs };
  const sources = new Set(members.map(sourceOf));
  const source = sources.size === 1 ? [...sources][0] : null;
  const { tier, freshness } = members[0];
  return { common: { tier, freshness: freshness && { ...freshness, source }, source }, differs };
}
