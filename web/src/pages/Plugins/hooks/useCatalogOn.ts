import { useMarketplace } from '@/hooks/usePlugins';

/**
 * Whether this server has a plugin catalog, so the ways into Discover lead
 * somewhere. Null until it answers: a way in shown while the listing loads
 * would be taken away again on a server without one. A catalog that is set
 * but cannot be read counts as on, since Discover says why with a retry.
 */
export function useCatalogOn(): boolean | null {
  const { data, error } = useMarketplace();
  if (data) return data.configured;
  return error ? true : null;
}
