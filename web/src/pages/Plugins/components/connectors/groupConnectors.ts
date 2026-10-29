import type { BuiltinMcpServer, CatalogServer } from '@/pages/ChatAgent/utils/api';
import { groupBy, matchesFilter } from '../../utils/groupOrigins';
import { connectorState } from '../../utils/connectorState';
import { connectorNeedsUser } from './attention';

/** The header search over a connector: its name, its description, and the
 *  plugin it came with, so searching a plugin's name finds its connectors. */
export function connectorMatches(query: string, server: CatalogServer | BuiltinMcpServer): boolean {
  return matchesFilter(query, server.name, server.description, server.plugin_name);
}

/**
 * The Connectors list by source: what needs the user first, then what they
 * added by hand, then one group per installed plugin, then what ships with the
 * app. A connector in Needs attention is not repeated in its plugin's group:
 * the problem is the place it lives until it is fixed. The lists come in
 * already narrowed by the search and the state pills.
 */
export interface ConnectorGroups {
  attention: CatalogServer[];
  own: CatalogServer[];
  /** Larger groups first, so the single-connector plugins can sit side by
   *  side at the end rather than each taking a full-width band. */
  plugins: Array<{ plugin: string; servers: CatalogServer[]; pluginOff: boolean }>;
  builtins: BuiltinMcpServer[];
  /** Every connector left, across all groups. */
  total: number;
}

export function groupConnectors(
  servers: CatalogServer[],
  builtins: BuiltinMcpServer[],
): ConnectorGroups {
  const attention: CatalogServer[] = [];
  const rest: CatalogServer[] = [];
  for (const server of servers) {
    (connectorNeedsUser(server) ? attention : rest).push(server);
  }
  // What failed before what is waiting for a sign-in: a refusal is something
  // that used to work.
  const rank = (s: CatalogServer) => (connectorState(s).kind === 'failing' ? 0 : 1);
  attention.sort((a, b) => rank(a) - rank(b));

  // Hand-added connectors have no plugin and key under ''.
  const byPlugin = groupBy(rest, (s) => s.plugin_name || '');
  const own = byPlugin.get('') ?? [];
  byPlugin.delete('');
  const plugins = [...byPlugin.entries()]
    .map(([plugin, list]) => ({
      plugin,
      servers: list,
      pluginOff: list.every((s) => s.plugin_enabled === false),
    }))
    .sort((a, b) => b.servers.length - a.servers.length || a.plugin.localeCompare(b.plugin));

  return {
    attention,
    own,
    plugins,
    builtins,
    total: servers.length + builtins.length,
  };
}
