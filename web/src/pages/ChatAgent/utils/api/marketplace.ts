/**
 * Plugin marketplace: the curated catalog the Discover tab lists. Read-only;
 * installing an entry goes through `installPluginFromUrl` with its
 * `source_url`, the same path a pasted git URL takes.
 */
import { api } from '@/api/client';

/** One vocabulary term. The catalog ships both languages, so a new region
 *  needs no release here. */
export interface MarketplaceTerm {
  key: string;
  label: string;
  label_zh: string | null;
}

/** Display order is the array order. */
export interface MarketplaceVocabulary {
  assets: MarketplaceTerm[];
  regions: MarketplaceTerm[];
  data: MarketplaceTerm[];
}

export interface MarketplacePublisher {
  name: string;
  url: string | null;
  official: boolean;
}

export interface MarketplaceCoverage {
  /** Asset class to the regions it covers: `{ equity: ['cn', 'hk'] }`. */
  markets: Record<string, string[]>;
  data: string[];
}

export interface MarketplaceEntry {
  name: string;
  description: string;
  /** data | research | modeling | reporting | quant | trading, or another the
   *  catalog adds later. */
  category: string | null;
  license: string | null;
  publisher: MarketplacePublisher | null;
  homepage: string | null;
  /** Same-origin icon path; `bundleArt()` resolves it. */
  icon_url: string | null;
  keywords: string[];
  auth: 'oauth' | 'api-key' | 'none' | null;
  /** What the user needs before the entry works, in the catalog's words. */
  requires: string | null;
  coverage: MarketplaceCoverage | null;
  /** Read from the entry's own files, so known only when they ship in the
   *  catalog repository; absent for an entry that lives upstream. */
  contents?: MarketplaceContents | null;
  source_url: string;
  installed: boolean;
  /** The installed plugin that is this entry, matched on its source the way
   *  the installer reads it rather than on its name; null when none is. */
  plugin: string | null;
  /** `plugin` holds something other than what `source_url` installs now;
   *  updating it with `source_url` brings it level. False when not installed. */
  update_available: boolean;
}

/** What installing an entry adds and the keys install asks for. */
export interface MarketplaceContents {
  connectors: string[];
  skills: string[];
  keys: { name: string; label: string }[];
}

export interface MarketplaceResponse {
  /** False when the server has no catalog set; every list is then empty. */
  configured: boolean;
  name: string | null;
  description: string | null;
  url: string | null;
  vocabulary: MarketplaceVocabulary;
  entries: MarketplaceEntry[];
}

export async function getMarketplace(): Promise<MarketplaceResponse> {
  const { data } = await api.get<MarketplaceResponse>('/api/v1/marketplace');
  return data;
}
