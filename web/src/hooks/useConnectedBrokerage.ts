import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { queryKeys } from '@/lib/queryKeys';
import { needsOauthConnect } from '@/pages/ChatAgent/components/mcp/mcpState';
import { brokerageForUrl } from '@/pages/Plugins/brokerages';

// The nav asks this on every page, so it loads with the app shell, and the
// fetchers are imported only when a query runs: `useBrokerages` and
// `useMcpCatalog` would bring all of useMcpServers and the MCP client into the
// first load. The keys and stale times are theirs, so both halves still read
// and fill the cache the Plugins page uses.
const mcpApi = () => import('@/pages/ChatAgent/utils/api/mcp');

/**
 * Whether the user has a live connection to one of the shipped brokerages.
 *
 * Three answers, not two: `undefined` is "not known yet", and a caller that
 * reads it as `false` announces "you have no broker" to someone whose catalog
 * simply has not landed. Both halves are queries the Plugins page already
 * owns, so this is a cache read once that page has been opened, and the join
 * is the same host match every brokerage surface uses -- a row pointed at
 * another address has stopped being the broker, here as everywhere else.
 */
export function useHasConnectedBrokerage(): boolean | undefined {
  const { data: brokerages } = useQuery({
    queryKey: queryKeys.brokerages.list(),
    queryFn: () => mcpApi().then((api) => api.getBrokerages()),
    staleTime: Infinity,
  });
  const { data: catalog } = useQuery({
    queryKey: queryKeys.mcp.catalog(),
    queryFn: () => mcpApi().then((api) => api.getMcpCatalog()),
    staleTime: 60_000,
  });
  return useMemo(() => {
    if (!brokerages || !catalog) return undefined;
    return catalog.servers.some(
      (row) => !needsOauthConnect(row.oauth_status) && !!brokerageForUrl(row.url, brokerages),
    );
  }, [brokerages, catalog]);
}
