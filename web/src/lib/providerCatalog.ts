/**
 * The deployment's provider catalog, indexed by every provider a model can
 * name: an entry's own, a region of it (a variant inside its entry, the
 * region its own connection), and another API of it (a route, which is the
 * entry's connection). Every surface that maps a provider to its entry or its
 * connection reads this one index.
 */
import type { ProviderCatalogEntry } from '@/components/model/types';

export interface CatalogRow {
  /** The catalog entry the provider is listed under. */
  entry: ProviderCatalogEntry;
  /** The connection its key and its models belong to. */
  connection: string;
  sdk?: string;
}

const NO_NAMES: ReadonlySet<string> = new Set();

/** The user's own provider names, from their stored `custom_providers`. */
export function ownProviderNames(customProviders: readonly { name?: unknown }[] | null | undefined): Set<string> {
  return new Set((customProviders ?? []).flatMap((cp) => (typeof cp?.name === 'string' && cp.name ? [cp.name] : [])));
}

/** The connection a provider is one API of, else the provider itself. */
export type ConnectionOf = (provider: string) => string;

export interface CatalogIndex {
  rows: ReadonlyMap<string, CatalogRow>;
  entryFor: (provider: string) => ProviderCatalogEntry | null;
  connectionOf: ConnectionOf;
}

/**
 * `own` are the names of the user's own providers. One saved before a gateway
 * took its name for a route keeps it, as the server routes it
 * (`route_custom_entry`), so the route is not indexed under it: its models
 * stay the user's provider's, never the gateway's to rewrite or remove.
 */
export function indexCatalog(
  catalog: readonly ProviderCatalogEntry[],
  own: ReadonlySet<string> = NO_NAMES,
): CatalogIndex {
  const rows = new Map<string, CatalogRow>();
  for (const entry of catalog) {
    rows.set(entry.provider, { entry, connection: entry.provider, sdk: entry.sdk });
    for (const v of entry.region_variants ?? []) {
      rows.set(v.provider, { entry, connection: v.provider, sdk: v.sdk ?? entry.sdk });
    }
    for (const route of entry.routes ?? []) {
      if (!own.has(route)) rows.set(route, { entry, connection: entry.provider });
    }
  }
  return {
    rows,
    entryFor: (provider) => rows.get(provider)?.entry ?? null,
    connectionOf: (provider) => rows.get(provider)?.connection ?? provider,
  };
}
