import type { ProviderCatalogEntry, ProviderCategory } from '@/components/model/types';

/**
 * The key picker shows one row per provider, then the ways in it offers.
 * The catalog lists each plan as its own entry and each region of a plan as
 * a `region_variant` of it, so this folds them back under the provider they
 * belong to (`group`, the top-level key in providers.json).
 */

export type Plan = 'api_key' | 'coding_plan';

/** One place a key can be saved: a plan of a provider, in one region. */
export interface KeyOption {
  /** The catalog entry the option comes from: the plan's own entry. */
  entry: ProviderCatalogEntry;
  /** Where the key is saved: the entry itself or one of its regions. */
  provider: string;
  plan: Plan;
  region: string;
  base_url: string | null;
  /** The wire format the key is spoken with there. */
  sdk: string | null;
  use_response_api: boolean;
  /** Whose manifest models the connection offers (the catalog's `models_from`). */
  models_from: string;
}

export interface ProviderGroup {
  group: string;
  name: string;
  category: ProviderCategory | null;
  /** Whose mark the row shows: the group's own provider when it has one. */
  markProvider: string;
  options: KeyOption[];
}

export type CategoryFilter = 'all' | ProviderCategory;

export interface KeyFilter {
  query: string;
  category: CategoryFilter;
}

export const NO_FILTER: KeyFilter = { query: '', category: 'all' };

/** The list's sections, in order; a provider with no category goes last. */
export const CATEGORIES: readonly ProviderCategory[] = ['closed_source', 'open_source', 'inference'];

const REGION_ORDER: Record<string, number> = { intl: 0, sg: 1, cn: 2 };

/** The places a catalog entry takes a key: its own, then each of its regions. */
export function optionsOf(entry: ProviderCatalogEntry): KeyOption[] {
  const plan: Plan = entry.access_type === 'coding_plan' ? 'coding_plan' : 'api_key';
  const own: KeyOption = {
    entry,
    provider: entry.provider,
    plan,
    region: entry.region ?? 'intl',
    base_url: entry.base_url ?? null,
    sdk: entry.sdk ?? null,
    use_response_api: entry.use_response_api ?? false,
    models_from: entry.models_from ?? entry.brand_key ?? entry.provider,
  };
  const regions = (entry.region_variants ?? []).map((v): KeyOption => ({
    entry,
    provider: v.provider,
    plan,
    region: v.region ?? 'intl',
    base_url: v.base_url ?? null,
    sdk: v.sdk ?? own.sdk,
    use_response_api: v.use_response_api ?? own.use_response_api,
    models_from: v.models_from ?? own.models_from,
  }));
  return [own, ...regions];
}

export function groupProviders(entries: readonly ProviderCatalogEntry[]): ProviderGroup[] {
  const groups = new Map<string, ProviderGroup>();
  for (const entry of entries) {
    // A server from before `group` existed: each entry stands alone.
    const key = entry.group ?? entry.provider;
    let g = groups.get(key);
    if (!g) {
      g = {
        group: key,
        name: entry.group_display_name ?? entry.display_name,
        category: entry.category ?? null,
        markProvider: entry.provider,
        options: [],
      };
      groups.set(key, g);
    }
    if (entry.provider === key) g.markProvider = entry.provider;
    g.options.push(...optionsOf(entry));
  }
  for (const g of groups.values()) {
    g.options.sort((a, b) => (REGION_ORDER[a.region] ?? 9) - (REGION_ORDER[b.region] ?? 9));
  }
  return [...groups.values()];
}

export function filterGroups(groups: readonly ProviderGroup[], f: KeyFilter): ProviderGroup[] {
  const q = f.query.trim().toLowerCase();
  return groups.filter((g) => {
    if (f.category !== 'all' && g.category !== f.category) return false;
    return !q || g.name.toLowerCase().includes(q) || g.group.toLowerCase().includes(q);
  });
}

/** Where a provider's key form starts: the region a key is saved for, else its first. */
export function startingOption(g: ProviderGroup, saved: ReadonlySet<string>): KeyOption {
  return g.options.find((o) => saved.has(o.provider)) ?? g.options[0];
}
