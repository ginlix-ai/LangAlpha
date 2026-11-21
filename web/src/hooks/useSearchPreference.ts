import { useModels } from '@/hooks/useModels';
import { usePreferences } from '@/hooks/usePreferences';
import { useSearchTierAllows } from '@/hooks/useSearchTierAllows';
import { readSearchPreference } from '@/lib/searchPreferences';

/** The user's web search choice as the controls show it, with the catalog
 *  behind it and which of its options the user's plan reaches. */
export function useSearchPreference() {
  const { preferences } = usePreferences();
  const { models } = useModels();
  const tierAllows = useSearchTierAllows();
  const catalog = models?.search_providers ?? null;
  const providers = Object.entries(catalog ?? {});
  return {
    ...readSearchPreference(preferences, catalog),
    catalog,
    providers,
    tierAllows,
    /** A provider is above the user's plan, which the row says. */
    gated: providers.some(([, p]) => !tierAllows(p.min_tier)),
  };
}
