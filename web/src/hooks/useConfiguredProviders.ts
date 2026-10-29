import { useMemo } from 'react';
import { useQueries, useQuery, type UseQueryResult } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useApiKeys } from './useApiKeys';
import {
  getOAuthStatus,
  SUBSCRIPTIONS,
  subscriptionKey,
  type ConnectionState,
  type OAuthStatus,
  type SubscriptionProvider,
} from '@/api/oauth';
import type { ByokProvider } from '@/components/model/types';

// Kept apart from this hook so code that only reads a key's mask (the payer
// of a model) does not load the queries behind it.
export { SERVER_KEY, isOwnKey } from '@/lib/ownKey';

export interface ConfiguredProvider {
  provider: string;
  display_name: string;
  access_type: 'api_key' | 'oauth' | 'coding_plan' | 'local';
  /** Parent/brand group key — equals ``provider`` when the entry is the brand
   * itself. Carried through so model filters can treat the brand as
   * configured when any of its variants are (e.g. z-ai-coding → z-ai). */
  brand_key?: string;
  /** Saved with the key; the key then goes there, whatever model it serves. */
  base_url?: string | null;
  /** Connected subscription plan for OAuth providers (e.g. "pro", "team").
   * Consulted by the ``oauth_plans`` per-model gate; null/absent = unknown. */
  plan_type?: string | null;
  /** Subscriptions: whether the sign-in still works. Keys are always `connected`. */
  state?: ConnectionState;
  /** Keys: the stored key as the server masks it (first 3 + last 4). */
  masked_key?: string | null;
  /** Subscriptions: the account signed in. */
  email?: string | null;
  account_id?: string | null;
  /** A provider the user defined (`custom_providers`), not a catalog one. */
  is_custom?: boolean;
  parent_provider?: string;
}

function statusQuery(provider: SubscriptionProvider) {
  return {
    queryKey: queryKeys.oauth.status(subscriptionKey(provider)),
    queryFn: () => getOAuthStatus(provider),
    staleTime: 60_000,
    retry: false,
  };
}

/** One subscription's status, from the cache every surface that shows it shares. */
export function useOAuthStatus(provider: SubscriptionProvider) {
  return useQuery(statusQuery(provider));
}

// Module scope, so useQueries reruns it only when a status changes.
const combineStatuses = (results: UseQueryResult<OAuthStatus>[]) => ({
  /** In `SUBSCRIPTIONS` order. */
  statuses: results.map((r) => r.data),
  isLoading: results.some((r) => r.isLoading),
  isError: results.some((r) => r.isError),
});

/**
 * Returns a unified list of all configured providers (BYOK keys + OAuth connections).
 * Used by the wizard to show what's already connected and to mark provider cards.
 */
export function useConfiguredProviders() {
  const { apiKeys, isLoading: keysLoading } = useApiKeys();
  const oauth = useQueries({
    queries: SUBSCRIPTIONS.map((s) => statusQuery(s.provider)),
    combine: combineStatuses,
  });
  const { statuses } = oauth;

  const { providers, withdrawn } = useMemo(() => {
    const saved = (apiKeys as { providers?: unknown } | null)?.providers;
    const rows = (Array.isArray(saved) ? saved as ByokProvider[] : []).filter((p) => p.has_key);
    const asConnection = (p: ByokProvider): ConfiguredProvider => ({
      provider: p.provider,
      display_name: p.display_name ?? p.provider,
      access_type: p.access_type ?? 'api_key',
      brand_key: p.brand_key ?? p.provider,
      base_url: p.base_url ?? null,
      state: 'connected',
      masked_key: p.masked_key ?? null,
      is_custom: p.is_custom === true,
      ...(p.parent_provider ? { parent_provider: p.parent_provider } : {}),
    });
    const keys = rows.filter((p) => !p.withdrawn).map(asConnection);
    const subscriptions = SUBSCRIPTIONS.flatMap((s, i): ConfiguredProvider[] => {
      const status = statuses[i];
      if (!status?.connected) return [];
      return [{
        provider: s.provider,
        display_name: s.name,
        access_type: 'oauth',
        plan_type: status.plan_type ?? null,
        state: status.state,
        email: status.email,
        account_id: status.account_id,
      }];
    });
    return { providers: [...keys, ...subscriptions], withdrawn: rows.filter((p) => p.withdrawn).map(asConnection) };
  }, [apiKeys, statuses]);

  /** Set of configured provider keys for fast lookup */
  const configuredSet = useMemo(
    () => new Set(providers.map((p) => p.provider)),
    [providers],
  );

  return {
    providers,
    /** Keys stored for providers the catalog no longer has: no connection,
     *  only something to remove. Never in `providers`. */
    withdrawn,
    configuredSet,
    /** A connection that runs models: a subscription waiting to be signed in
     *  again is listed for its reconnect, but pays for no turn, as the
     *  server's setup gate counts it. */
    hasAny: providers.some((p) => p.state === 'connected'),
    /** The account's BYOK switch: off, the server runs no turn on a stored key. */
    byokEnabled: (apiKeys as Record<string, unknown> | null)?.byok_enabled === true,
    isLoading: keysLoading || oauth.isLoading,
    /** The subscriptions' statuses are still being read. */
    statusLoading: oauth.isLoading,
    /** A status that could not be read; its row shows unknown, not disconnected. */
    statusError: oauth.isError,
  };
}
