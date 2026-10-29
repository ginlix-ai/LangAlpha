import { useBrokerages } from '@/hooks/useMcpServers';
import { brokerageForUrl, type Brokerage } from '../brokerages';

/**
 * Which shipped brokerage an address belongs to, as every surface reads it:
 * the row's cell, its page, and the connect it starts.
 *
 * `undefined` is the registry unanswered (in flight, or asked and failed),
 * which is a different thing from resolving to no vendor: read as "none", a
 * row that is actually a broker loses the questions its vendor asks, and a
 * native-only one gets a live button into the dead end. Unknown holds
 * Connect; wrong spends something on the user's behalf.
 *
 * Not in `brokerages.ts`, which the app shell loads for the nav: the registry
 * query would bring the whole MCP client onto the first load.
 */
export function useVendorOf(): (url?: string | null) => Brokerage | null | undefined {
  const { data: brokerages } = useBrokerages();
  return (url) => (brokerages ? brokerageForUrl(url, brokerages) : undefined);
}
