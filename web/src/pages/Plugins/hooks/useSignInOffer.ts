import { useMcpCatalog } from '@/hooks/useMcpServers';
import type { McpSignInNeeded } from '@/pages/ChatAgent/utils/api';
import { connectRequestFor, useMcpOauthActions } from './useMcpOauthActions';

/**
 * The sign-in a switch was refused for, offered rather than started.
 *
 * A plugin switches on only once every server it carries is connected, and
 * the 409 that says otherwise names them. The first is the one offered: a
 * reconnect that lands switches its row on, after which the plugin's own
 * switch goes through or names the next. The row's address and its last
 * capability answer come from the catalog, as the row's own reconnect would
 * send them.
 */
export function useSignInOffer(returnTo: string) {
  const oauth = useMcpOauthActions({ returnTo });
  const { data: catalog } = useMcpCatalog();

  function requestFor(servers: McpSignInNeeded[]) {
    const first = servers[0];
    if (!first) return null;
    const row = catalog?.servers.find((s) => s.name === first.server);
    return connectRequestFor(row ?? { name: first.server, url: first.url || null });
  }

  /** Offer the sign-in in its own message, for a single switch refused. */
  function offer(servers: McpSignInNeeded[]) {
    const request = requestFor(servers);
    if (request) oauth.offerSignIn(request);
  }

  /** Start it, for a caller whose own message already carries the offer. */
  function start(servers: McpSignInNeeded[]) {
    const request = requestFor(servers);
    if (request) oauth.connect(request);
  }

  return { oauth, offer, start };
}
