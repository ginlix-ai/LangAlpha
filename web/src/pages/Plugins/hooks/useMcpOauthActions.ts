import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useDisconnectMcpOauth, useRefreshMcpOauthSchemas } from '@/hooks/useMcpServers';
import { formatApiErrorDetail, startMcpOauth } from '@/pages/ChatAgent/utils/api';

/**
 * The user-tier OAuth connect lifecycle: the one thing the Plugins MCP tab
 * genuinely owns, with no counterpart on the workspace MCP tab.
 *
 * The vendor bearer never leaves the host — a sandbox reaches the server
 * through the egress relay — so connecting here is all a workspace needs.
 */
export function useMcpOauthActions() {
  const { t } = useTranslation();
  const disconnectMutation = useDisconnectMcpOauth();
  const refreshMutation = useRefreshMcpOauthSchemas();
  const [connectingName, setConnectingName] = useState<string | null>(null);
  const [refreshingName, setRefreshingName] = useState<string | null>(null);

  async function connect(name: string) {
    setConnectingName(name);
    try {
      const { authorize_url } = await startMcpOauth(name, '/plugins?tab=mcp');
      // Full-page navigation into the vendor's consent screen; the backend
      // callback lands back on /plugins with ?mcp_connected / ?mcp_error.
      window.location.assign(authorize_url);
    } catch (err) {
      setConnectingName(null);
      toast({
        variant: 'destructive',
        title: t('plugins.oauth.connectFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  async function disconnect(name: string) {
    try {
      await disconnectMutation.mutateAsync(name);
      toast({
        title: t('plugins.oauth.disconnectedTitle'),
        description: t('plugins.oauth.disconnectedDesc', { server: name }),
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.oauth.disconnectFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  async function refreshSchemas(name: string) {
    setRefreshingName(name);
    try {
      const result = await refreshMutation.mutateAsync(name);
      if (result.status === 'ok' && !result.error) {
        toast({
          title: t('plugins.oauth.refreshedTitle'),
          description: t('plugins.oauth.refreshedDesc', {
            server: name,
            count: result.tool_count,
          }),
        });
      } else if (result.status === 'ok') {
        // The cache keeps `status`/`tools` from the last good snapshot on a
        // failed re-discovery but always overwrites `error` — so an ok status
        // carrying error text means this attempt failed and the count below is
        // stale. Claiming success here would be a lie. The error string itself
        // stays out of the copy: it can be a raw connection error against a
        // user-chosen address, i.e. an internal-reachability oracle.
        toast({
          title: t('plugins.oauth.refreshFailedStaleTitle'),
          description: t('plugins.oauth.refreshFailedStaleDesc', {
            server: name,
            count: result.tool_count,
          }),
        });
      } else {
        toast({
          variant: 'destructive',
          title: t('plugins.oauth.refreshFailed'),
          description: result.error || result.status,
        });
      }
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.oauth.refreshFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setRefreshingName(null);
    }
  }

  return { connectingName, refreshingName, connect, disconnect, refreshSchemas };
}
