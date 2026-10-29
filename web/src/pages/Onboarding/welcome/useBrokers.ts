import { useBrokerages, useMcpCatalog } from '@/hooks/useMcpServers';
import { connectBlock, resolveBrokerages, type ResolvedBroker } from '@/pages/Plugins/brokerages';
import { isPluginSuppressed } from '@/pages/Plugins/utils/provenance';

/**
 * The brokerages the welcome flow offers, and which of them are connected.
 *
 * A row whose plugin is switched off stays out of the agent's tools whatever
 * the flow does, so it is left out, and the Brokerages tab is where its reason
 * is spelled out. A connected one is always kept, whatever else is true of it.
 *
 * A broker that only the desktop app can connect (Robinhood) is not a card
 * here, since its Connect would do nothing, but it is not left out either: it
 * is `desktopOnly`, for the step to say where it can be connected.
 */
export function useBrokers() {
  const offers = useBrokerages();
  const catalog = useMcpCatalog();
  const resolved = resolveBrokerages(offers.data ?? [], catalog.data?.servers ?? []);
  const offered = [...resolved.values()].filter(
    ({ row, connected }) => connected || !row || (row.transport === 'http' && !isPluginSuppressed(row)),
  );
  const rows = offered.filter(({ vendor, connected }) => connected || connectBlock(vendor) === null);
  return {
    rows,
    desktopOnly: offered.filter(({ vendor, connected }) => !connected && connectBlock(vendor) === 'native-only'),
    /** A desktop app too old to finish any sign-in, which blocks every broker
     *  at once; asked of no vendor because it depends on none. */
    shellOutdated: connectBlock(null) === 'shell-outdated',
    resolved,
    connected: rows.filter((r) => r.connected),
    loading: offers.isLoading || catalog.isLoading,
    /** Both lists are in, so what is connected is known rather than assumed. */
    ready: offers.data !== undefined && catalog.data !== undefined,
    offersError: offers.error,
    catalogError: catalog.error,
    refetchOffers: offers.refetch,
    refetchCatalog: catalog.refetch,
  };
}

export type { ResolvedBroker };
