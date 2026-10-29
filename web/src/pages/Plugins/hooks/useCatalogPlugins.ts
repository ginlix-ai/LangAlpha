import { useMarketplace } from '@/hooks/usePlugins';
import type { MarketplaceEntry } from '@/pages/ChatAgent/utils/api';

/**
 * The installed plugins that are catalog entries, each with its entry, keyed
 * by plugin name as the catalog names them: what tells a catalog install from
 * a pasted git URL, which are both stored as a git source, and whether the
 * catalog holds a newer version. Null until the catalog answers or when it
 * cannot be read; `pluginOrigin` then keeps the reading it had before it could
 * tell, rather than relabelling every row the moment the listing lands. A
 * server with no catalog answers an empty map, so nothing there reads as from
 * it.
 */
export function useCatalogPlugins(): ReadonlyMap<string, MarketplaceEntry> | null {
  const { data } = useMarketplace();
  if (!data) return null;
  return new Map(
    data.configured ? data.entries.flatMap((e) => (e.plugin ? [[e.plugin, e] as const] : [])) : [],
  );
}

/** The catalog's newer revision of `plugin`, the URL its Update moves it to;
 *  null when the catalog holds nothing newer or does not list it. */
export function catalogUpdateFor(
  catalog: ReadonlyMap<string, MarketplaceEntry> | null,
  plugin: string,
): string | null {
  const entry = catalog?.get(plugin);
  return entry?.update_available ? entry.source_url : null;
}
