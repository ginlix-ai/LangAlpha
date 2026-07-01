import React, { useCallback, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { LineChart, Lock, Loader2 } from 'lucide-react';
import { queryKeys } from '@/lib/queryKeys';
import { OAUTH_BROADCAST_CHANNEL, OAUTH_POPUP_WINDOW_NAME, OAUTH_POPUP_FEATURES } from '@/lib/oauthPopup';
import { toast } from '@/components/ui/use-toast';
import {
  getRobinhoodStatus,
  initiateRobinhood,
  disconnectRobinhood,
  formatApiErrorDetail,
} from '../../utils/api';

interface RobinhoodConnectCardProps {
  workspaceId: string;
}

/**
 * Connect a workspace to Robinhood's Agentic Trading MCP via OAuth.
 *
 * The flow: a popup (opened synchronously to keep the user-gesture) navigates to
 * the authorize URL; the backend callback closes it and broadcasts
 * `oauth-complete`, which refreshes the status + MCP server list. Trade execution
 * stays gated server-side, so this card surfaces a read-only/preview-only note.
 */
export function RobinhoodConnectCard({ workspaceId }: RobinhoodConnectCardProps) {
  const queryClient = useQueryClient();
  const statusKey = ['robinhood', 'status', workspaceId];

  const { data: status, isLoading } = useQuery({
    queryKey: statusKey,
    queryFn: () => getRobinhoodStatus(workspaceId),
    enabled: !!workspaceId,
  });

  const refresh = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: statusKey });
    queryClient.invalidateQueries({ queryKey: queryKeys.mcp.workspace(workspaceId) });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryClient, workspaceId]);

  // The popup posts `oauth-complete` once the callback finishes.
  useEffect(() => {
    if (typeof BroadcastChannel === 'undefined') return;
    const channel = new BroadcastChannel(OAUTH_BROADCAST_CHANNEL);
    const onMessage = (event: MessageEvent) => {
      if (event.data?.type === 'oauth-complete') refresh();
    };
    channel.addEventListener('message', onMessage);
    return () => {
      channel.removeEventListener('message', onMessage);
      channel.close();
    };
  }, [refresh]);

  const connectMutation = useMutation({
    mutationFn: async () => {
      // Open synchronously inside the click handler to preserve the user gesture.
      const popup = window.open('about:blank', OAUTH_POPUP_WINDOW_NAME, OAUTH_POPUP_FEATURES);
      try {
        const { authorize_url } = await initiateRobinhood(workspaceId);
        if (popup) popup.location.href = authorize_url;
        else window.open(authorize_url, OAUTH_POPUP_WINDOW_NAME, OAUTH_POPUP_FEATURES);
      } catch (err) {
        if (popup) popup.close();
        throw err;
      }
    },
    onError: (err) => {
      toast({
        title: 'Could not start Robinhood connection',
        description: formatApiErrorDetail(err),
        variant: 'destructive',
      });
    },
  });

  const disconnectMutation = useMutation({
    mutationFn: () => disconnectRobinhood(workspaceId),
    onSuccess: () => {
      refresh();
      toast({ title: 'Robinhood disconnected' });
    },
    onError: (err) => {
      toast({
        title: 'Could not disconnect Robinhood',
        description: formatApiErrorDetail(err),
        variant: 'destructive',
      });
    },
  });

  const connected = !!status?.connected;

  return (
    <div
      className="flex items-start justify-between gap-3 p-3 rounded-lg"
      style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
    >
      <div className="min-w-0 flex flex-col gap-1">
        <div className="flex items-center gap-2 flex-wrap">
          <LineChart className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
          <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            Robinhood Agentic Trading
          </span>
          {connected && (
            <span
              className="text-[10px] px-1.5 py-0.5 rounded uppercase tracking-wide"
              style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
            >
              connected
            </span>
          )}
          <span
            className="inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded uppercase tracking-wide"
            style={{ color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' }}
            title="Order placement and cancellation are disabled. The agent can read accounts, quotes, and preview orders only."
          >
            <Lock className="h-2.5 w-2.5" />
            trading gated
          </span>
        </div>
        <span className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>
          {connected
            ? 'Read accounts, positions, quotes, and order previews. Placing or cancelling orders is disabled.'
            : 'Connect to let the agent read your Robinhood accounts and preview orders. Trade execution stays disabled.'}
        </span>
      </div>

      <div className="flex-shrink-0">
        {connected ? (
          <button
            type="button"
            onClick={() => disconnectMutation.mutate()}
            disabled={disconnectMutation.isPending}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
            style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
          >
            {disconnectMutation.isPending && <Loader2 className="h-3 w-3 animate-spin" />}
            Disconnect
          </button>
        ) : (
          <button
            type="button"
            onClick={() => connectMutation.mutate()}
            disabled={connectMutation.isPending || isLoading}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
            style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
          >
            {connectMutation.isPending && <Loader2 className="h-3 w-3 animate-spin" />}
            Connect
          </button>
        )}
      </div>
    </div>
  );
}
