import type { SearchDepthInfo, SearchProviderCatalog } from '@/hooks/useAllModels';
import type { UserPreferences } from '@/types/api';

export interface SearchPreference {
  /** The stored provider, or '' (Default) when the catalog no longer declares it. */
  provider: string;
  /** The levels to offer: the chosen provider's, in manifest order, when it
   *  declares more than one; none otherwise, and then no depth control. */
  depths: SearchDepthInfo[];
  /** The stored level, or '' (Default) when the chosen provider lacks it. */
  depth: string;
}

/** The web search choice as the controls show it, read against the catalog:
 *  a stored value the catalog does not back reads as Default. */
export function readSearchPreference(
  prefs: UserPreferences | null | undefined,
  catalog: SearchProviderCatalog | null | undefined,
): SearchPreference {
  const other = (prefs?.other_preference ?? {}) as Record<string, unknown>;
  const provider = typeof other.search_provider === 'string' && catalog?.[other.search_provider]
    ? other.search_provider
    : '';
  const declared = provider ? catalog?.[provider]?.depths ?? [] : [];
  const depths = declared.length > 1 ? declared : [];
  const depth = typeof other.search_depth === 'string' && depths.some((d) => d.name === other.search_depth)
    ? other.search_depth
    : '';
  return { provider, depths, depth };
}

/**
 * Picking a provider. Depth levels are provider-scoped, so a stale level may
 * not exist on the new provider and is cleared. A provider offering one level
 * has no depth control at all: the stored level stays, for a switch back to
 * want. Default is not that case. It shows no control either, but resolve
 * time still reads the stored level against whichever provider the deployment
 * picks, so an invisible one would keep applying.
 */
export function searchProviderPatch(next: string, catalog: SearchProviderCatalog | null | undefined): Partial<UserPreferences> {
  const depths = next ? catalog?.[next]?.depths ?? [] : [];
  return {
    other_preference: {
      search_provider: next || null,
      ...(!next || depths.length > 1 ? { search_depth: null } : {}),
    },
  };
}

export function searchDepthPatch(next: string): Partial<UserPreferences> {
  return { other_preference: { search_depth: next || null } };
}
