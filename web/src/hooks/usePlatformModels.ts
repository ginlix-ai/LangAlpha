/**
 * Hook + utility for fetching the user's model-access tier from the platform
 * service.
 *
 * Fail-open: when the endpoint is unavailable (network error, auth failure),
 * `usePlatformModels()` yields a null `platform` and `getModelAccess()` treats
 * every model as accessible.
 */
import { useQuery } from '@tanstack/react-query';
import { api } from '@/api/client';
import { queryKeys } from '@/lib/queryKeys';
import { isPlatformMode } from '@/config/hostMode';
import type { PlatformModelsResponse, ModelAccess } from '@/types/platform';

/**
 * Fetch the authenticated user's model-access info from the platform service.
 *
 * `platform` is `null` on any error (fail-open when the endpoint is
 * unavailable). Data is cached for 5 minutes to avoid repeated calls.
 * `isLoading` separates "not answered yet" from that fail-open `null`: until
 * the answer arrives, a locked model reads as reachable.
 */
export function usePlatformModels(): { platform: PlatformModelsResponse | null; isLoading: boolean } {
  const { data, isLoading } = useQuery<PlatformModelsResponse>({
    queryKey: queryKeys.platform.models(),
    queryFn: async () => {
      const res = await api.get<PlatformModelsResponse>('/api/auth/models');
      return res.data;
    },
    // Skip entirely in OSS mode — there's no platform auth service.
    enabled: isPlatformMode,
    staleTime: 5 * 60_000,
    gcTime: Infinity,
    retry: false,
    // Fail-open: swallow errors so the hook returns undefined (-> null below).
    meta: { suppressError: true },
    throwOnError: false,
  });
  // Validate shape — in OSS mode the endpoint may not exist and Vite can
  // return HTML (status 200), which the API client hands back as a string.
  const platform = data && Array.isArray(data.byok_providers) ? data : null;
  return { platform, isLoading };
}

/**
 * Determine a model's access status given the platform response.
 *
 * Priority: BYOK > OAuth > tier check > locked.
 *
 * When `platform` is null (endpoint unavailable), every model is treated as
 * accessible so the UI never blocks anything.
 *
 * @param modelTier - The tier required by the model (from model_metadata.tier).
 * @param provider  - The parent/brand provider slug (e.g. "anthropic").
 * @param platform  - Platform response, or null if unavailable.
 */
export function getModelAccess(
  modelTier: number,
  provider: string,
  platform: PlatformModelsResponse | null,
): ModelAccess {
  if (!platform) return 'platform'; // No platform endpoint — everything accessible
  if (platform.byok_providers.includes(provider)) return 'byok';
  if (platform.oauth_providers.includes(provider)) return 'oauth';
  if (modelTier <= platform.model_tier) return 'platform';
  return 'locked';
}
