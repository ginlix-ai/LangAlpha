import { useMemo } from 'react';
import { keepPreviousData, useQueries, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import {
  getLocalProviderModels,
  getProviderAvailableModels,
  listEndpointModels,
  type AvailableModels,
} from '@/api/model';

const STALE_MS = 10 * 60_000;

/**
 * What a connected key or subscription can run, from the provider's own list
 * intersected with the manifest. `q` narrows the untuned rest server-side
 * (OpenRouter is searched this way); the previous answer stays on screen while
 * the next one loads, so the list does not blank on every keystroke.
 */
export function useAvailableModels(provider: string | null, opts: { enabled?: boolean; q?: string } = {}) {
  const q = opts.q?.trim() ?? '';
  return useQuery({
    queryKey: queryKeys.providerModels.available(provider ?? '', q),
    queryFn: ({ signal }) => getProviderAvailableModels(provider!, { q: q || undefined, signal }),
    enabled: !!provider && (opts.enabled ?? true),
    staleTime: STALE_MS,
    retry: false,
    placeholderData: keepPreviousData,
  });
}

/** Reads the list again past the server's cache (limited server-side to one a
 *  provider every 10s) and stores it as the whole-list answer. */
export function useRefreshAvailableModels() {
  const queryClient = useQueryClient();
  return async (provider: string): Promise<AvailableModels> => {
    const fresh = await getProviderAvailableModels(provider, { refresh: true });
    queryClient.setQueryData(queryKeys.providerModels.available(provider, ''), fresh);
    // Searched answers came from the old list.
    const whole = queryKeys.providerModels.available(provider, '');
    queryClient.removeQueries({
      queryKey: queryKeys.providerModels.availableFor(provider),
      predicate: (query) => query.queryKey.at(-1) !== whole.at(-1),
    });
    return fresh;
  };
}

/**
 * What a custom endpoint lists, read once the URL and key have rested: the
 * server reads it with the key, so each keystroke would be a request to the
 * endpoint. Not kept after the step closes, since the key is in the query key.
 */
export function useEndpointModels(apiFormat: string, baseUrl: string, apiKey: string) {
  const liveUrl = baseUrl.trim();
  const liveKey = apiKey.trim();
  const format = useDebouncedValue(apiFormat, 600);
  const url = useDebouncedValue(liveUrl, 600);
  const key = useDebouncedValue(liveKey, 600);
  const settled = format === apiFormat && url === liveUrl && key === liveKey;
  const query = useQuery({
    queryKey: queryKeys.providerModels.endpoint(format, url, key),
    queryFn: ({ signal }) => listEndpointModels({ api_format: format, base_url: url, api_key: key }, signal),
    // Each value rests on its own, so only once all three have does the key
    // name one moment's form, never a new URL with the key it replaced.
    enabled: settled && /^https?:\/\/\S+$/i.test(url) && !!key,
    staleTime: STALE_MS,
    gcTime: 0,
    retry: false,
  });
  return { ...query, settled };
}

/** A local server's own list (Ollama, LM Studio, vLLM). */
export function useLocalProviderModels(provider: string | null, enabled = true) {
  return useQuery({
    queryKey: queryKeys.providerModels.local(provider ?? ''),
    queryFn: () => getLocalProviderModels(provider!),
    enabled: !!provider && enabled,
    staleTime: 60_000,
    retry: false,
  });
}

/**
 * The manifest models each provider's credential cannot run, by provider: the
 * `missing` of every list that was read. A provider whose list failed, is
 * still loading or fell back is absent, so nothing of its is hidden (fail open).
 */
export function useMissingModels(providers: readonly string[]): {
  missingByProvider: Map<string, Set<string>>;
  /** A list still on its first read: until it answers, its credential's
   *  manifest models read as reachable. */
  isLoading: boolean;
} {
  const results = useQueries({
    queries: providers.map((provider) => ({
      queryKey: queryKeys.providerModels.available(provider, ''),
      queryFn: ({ signal }: { signal: AbortSignal }) => getProviderAvailableModels(provider, { signal }),
      staleTime: STALE_MS,
      retry: false,
    })),
  });
  // One string for every answer, so the map keeps its identity until an
  // answer actually changes (the result array is new on every render).
  const signature = results
    .map((r) => (r.data?.status === 'ok' ? r.data.missing.join('\n') : ''))
    .join('\0');
  const missingByProvider = useMemo(() => {
    const out = new Map<string, Set<string>>();
    signature.split('\0').forEach((missing, i) => {
      if (missing && providers[i]) out.set(providers[i], new Set(missing.split('\n')));
    });
    return out;
  }, [signature, providers]);
  return { missingByProvider, isLoading: results.some((r) => r.isLoading) };
}
