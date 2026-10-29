import { useCallback, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { getAvailableModels } from '@/api/model';
import { getModelDisplayName } from '@/components/ui/chat-input.helpers';
import { withCustomModels, type ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { payerLabel, payerOfBilling } from '@/lib/modelPayer';
import { CACHE_ONLY_META, queryKeys } from '@/lib/queryKeys';
import { modelPrefs } from '@/lib/modelPreferences';
import { indexCatalog, ownProviderNames } from '@/lib/providerCatalog';
import { getPreferences } from '@/pages/Dashboard/utils/api';
import type { UserPreferences } from '@/types/api';

/**
 * A model's name as the composer pill prints it, for a surface handed a
 * manifest key (a stream event, a saved preference). Read from the catalog
 * the composer already loaded rather than fetched: a shared link has no
 * composer and may have no session to fetch with, and there the key, tidied,
 * will do.
 */
export function useModelName(): (id: string) => string {
  const metadata = useModelMetadata();
  return useCallback((id: string) => getModelDisplayName(id, metadata), [metadata]);
}

/** The catalog's model entries with the user's own laid over them, as the
 *  composer reads them, so a model saved from a gateway's catalog is named as
 *  the catalog names it. Undefined until the composer has loaded the catalog. */
export function useModelMetadata(): Record<string, ModelMetadataEntry> | undefined {
  const { data } = useCachedModels();
  const { data: preferences } = useQuery({
    queryKey: queryKeys.user.preferences(),
    queryFn: getPreferences as () => Promise<UserPreferences>,
    enabled: false,
    meta: CACHE_ONLY_META,
  });
  return useMemo(() => {
    if (!data?.model_metadata) return undefined;
    const { rows } = indexCatalog(data.provider_catalog ?? [], ownProviderNames(modelPrefs(preferences).custom_providers));
    return withCustomModels(data.model_metadata, modelPrefs(preferences).custom_models ?? [], (p) => rows.get(p)?.sdk, data.gateway_index);
  }, [data, preferences]);
}

function useCachedModels() {
  return useQuery({
    queryKey: queryKeys.models.all,
    queryFn: getAvailableModels,
    enabled: false,
    meta: CACHE_ONLY_META,
  });
}

/** Who pays for a call on `model`, as its tag reads, from the `billing_type`
 *  a stream event reported; undefined where the event names no payer. The
 *  event's `provider`, the one the call went through, wins over the catalog's:
 *  a model the user added on a sign-in may share another provider's name. */
export function usePaidByLabel(): (
  paidBy: string | undefined, model: string, provider?: string,
) => string | undefined {
  const { t } = useTranslation();
  const { data } = useCachedModels();
  return useCallback((paidBy: string | undefined, model: string, provider?: string) => {
    const entry = data?.model_metadata?.[model];
    // The route's own access, where the event names it: a model the user added
    // on a local server is in no catalog entry, or in a built-in's it shadows.
    // A call on another API of a connection (OpenCode Go's Chat Completions)
    // is paid as the connection.
    const { entryFor, connectionOf } = indexCatalog(data?.provider_catalog ?? []);
    const route = provider ? entryFor(provider) : undefined;
    const payer = payerOfBilling(
      paidBy,
      provider
        ? { ...entry, provider: connectionOf(provider), ...(route ? { access_type: route.access_type } : {}) }
        : entry,
    );
    return payer ? payerLabel(t, payer, (slug) => entryFor(slug)?.display_name) : undefined;
  }, [t, data]);
}
