import { useMemo } from 'react';
import { useModels } from './useModels';
import { usePreferences } from './usePreferences';
import { indexCatalog, ownProviderNames } from '@/lib/providerCatalog';
import { modelPrefs } from '@/lib/modelPreferences';
import type { ProviderCatalogEntry } from '@/components/model/types';
import {
  groupProviders,
  optionsOf,
  type KeyOption,
  type ProviderGroup,
} from '@/components/modelAccess/providerOptions';

/** Where a saved key sits among its provider's plans and regions. */
export interface KeyPlace {
  group: ProviderGroup;
  option: KeyOption;
}

const NO_ENTRIES: ProviderCatalogEntry[] = [];

/**
 * The deployment's provider catalog, read the ways every surface reads it:
 * the key providers folded by group, the coding plans folded the same way, the
 * local servers, and the entry, place and key option a saved provider belongs
 * to (a region is a variant inside its plan's entry, not an entry of its own).
 *
 * A coding plan is a subscription the user adds with a key, so it is listed
 * with the subscriptions, under the plan's own name, and not among the keys.
 *
 * A local server is one by its access type: `dynamic_models` says the server
 * lists its own models, which an API provider could also do, and such a
 * provider belongs with the keys, not in both lists.
 */
export function useProviderCatalog() {
  const { models, isLoading } = useModels();
  const catalog = models?.provider_catalog ?? NO_ENTRIES;
  const { preferences } = usePreferences();
  const customProviders = modelPrefs(preferences).custom_providers;
  return useMemo(() => {
    const keyGroups = groupProviders(catalog.filter((e) => e.access_type === 'api_key' && e.byok_eligible !== false));
    const planGroups = groupProviders(catalog.filter((e) => e.access_type === 'coding_plan' && e.byok_eligible !== false))
      .map((g) => ({ ...g, name: g.options[0].entry.display_name }));
    const localEntries = catalog.filter((e) => e.access_type === 'local');

    const { entryFor, connectionOf } = indexCatalog(catalog, ownProviderNames(customProviders));

    const placeOf = (provider: string): KeyPlace | undefined => {
      for (const group of [...keyGroups, ...planGroups]) {
        const option = group.options.find((o) => o.provider === provider);
        if (option) return { group, option };
      }
      return undefined;
    };

    /** The key option saved under `provider`, whichever list its entry is in. */
    const optionFor = (provider: string): KeyOption | null => {
      const entry = entryFor(provider);
      return entry ? optionsOf(entry).find((o) => o.provider === provider) ?? null : null;
    };

    return { catalog, keyGroups, planGroups, localEntries, entryFor, placeOf, optionFor, connectionOf, isLoading };
  }, [catalog, customProviders, isLoading]);
}
