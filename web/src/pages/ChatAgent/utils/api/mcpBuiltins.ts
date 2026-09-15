/**
 * Builtin MCP servers: process-global, toggled per user account-wide.
 */
import { api } from '@/api/client';

export interface BuiltinMcpServer {
  name: string;
  description: string;
  transport: string;
  enabled: boolean;
  /** Path on this origin to the mark the server declared in its handshake. */
  icon_url?: string | null;
  /** The bundle that ships this server, and whether that bundle is on: the
   *  same provenance pair a catalog row carries, so both group and explain
   *  themselves the same way. */
  plugin_name?: string | null;
  plugin_enabled?: boolean | null;
  /** Workspaces with a disable-marker for this builtin, all-scopes view only. */
  disabled_workspace_ids?: string[];
}

export async function getBuiltinMcpServers(): Promise<{ servers: BuiltinMcpServer[] }> {
  const { data } = await api.get('/api/v1/mcp/builtin-servers', {
    params: { all_scopes: true },
  });
  return data;
}

/** Account-wide toggle: applies to every workspace, no workspace re-enable. */
export async function setBuiltinMcpServerEnabled(name: string, enabled: boolean) {
  const { data } = await api.patch(`/api/v1/mcp/builtin-servers/${name}/enabled`, { enabled });
  return data as { name: string; enabled: boolean };
}
