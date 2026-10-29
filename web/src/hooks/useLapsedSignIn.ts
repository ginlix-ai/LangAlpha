import { useCallback, useMemo } from 'react';
import { isSubscriptionProvider, type SubscriptionProvider } from '@/api/oauth';
import { useAllModels } from './useAllModels';
import { useConfiguredProviders } from './useConfiguredProviders';

/**
 * The subscription a model runs on when that sign-in has to be renewed, else
 * null. Its models stay pickable, since the user may reconnect at any moment,
 * but every turn on one is refused until then, so the pickers say so first.
 */
export function useLapsedSignIn(): (model: string | null | undefined) => SubscriptionProvider | null {
  const { providers } = useConfiguredProviders();
  const { metadata } = useAllModels();
  const lapsed = useMemo(
    () => new Set(providers.filter((p) => p.access_type === 'oauth' && p.state === 'needs_reauth').map((p) => p.provider)),
    [providers],
  );
  return useCallback((model) => {
    const provider = model ? metadata[model]?.provider : undefined;
    return provider && lapsed.has(provider) && isSubscriptionProvider(provider) ? provider : null;
  }, [lapsed, metadata]);
}
