import { useCallback, useMemo } from 'react';
import { isSubscriptionProvider, type SubscriptionProvider } from '@/api/oauth';
import { useAllModels } from './useAllModels';
import { useConfiguredProviders } from './useConfiguredProviders';

/** A subscription model whose sign-in cannot run a turn: one that has to be
 *  renewed (`expired`), or none at all, never made or since removed
 *  (`missing`). Only the subscription runs its models, so either way every
 *  turn on one is refused. */
export interface SignInGap {
  provider: SubscriptionProvider;
  reason: 'expired' | 'missing';
}

/**
 * What stands between a model and its subscription, else null. Its models
 * stay pickable, since the user may sign in at any moment, but the pickers say
 * so before a send would be refused. A subscription reads as missing only once
 * every status has been read: one not read yet (loading, or paused offline),
 * or one the server could not report, is not known to be gone.
 */
export function useLapsedSignIn(): (model: string | null | undefined) => SignInGap | null {
  const { providers, statusPending, statusError } = useConfiguredProviders();
  const { metadata } = useAllModels();
  const state = useMemo(
    () => new Map(providers.filter((p) => p.access_type === 'oauth').map((p) => [p.provider, p.state])),
    [providers],
  );
  const known = !statusPending && !statusError;
  return useCallback((model) => {
    const provider = model ? metadata[model]?.provider : undefined;
    if (!provider || !isSubscriptionProvider(provider)) return null;
    const signIn = state.get(provider);
    if (signIn === 'needs_reauth') return { provider, reason: 'expired' };
    if (signIn === undefined && known) return { provider, reason: 'missing' };
    return null;
  }, [state, known, metadata]);
}
