import { useTranslation } from 'react-i18next';
import { Link2, Link2Off, Pencil, RefreshCw, Trash2 } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import { McpOauthPill } from '@/pages/ChatAgent/components/mcp/McpStatusPill';
import {
  canDisconnectOauth,
  needsOauthConnect,
} from '@/pages/ChatAgent/components/mcp/mcpState';
import {
  EnabledToggle,
  KebabTrigger,
  MetaText,
  ServerNameLine,
  ServerRowShell,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { isPluginOwned } from '../utils/provenance';
import { PluginSuppressedBadge } from './PluginBadges';
import { ScopeControl, type ScopeWorkspace } from './ScopeControl';
import { rowSelection, type BulkSelection } from './useBulkSelection';

/**
 * One user-tier MCP row on the Plugins page, in either the `Your servers`
 * list or a plugin's deck.
 *
 * Row anatomy mirrors the workspace MCP tab (`McpServerRow`): identity line
 * (icon + name + transport badge), then the status line (OAuth pill + scope
 * text), then the description — same primitives, same rhythm. What this row
 * has and that one doesn't is the OAuth connect lifecycle: the vendor bearer
 * never leaves the host, so "Connect" here is all a sandbox needs.
 */

export function McpCatalogRow({
  server,
  workspaces,
  selection,
  connecting,
  refreshing,
  toggling,
  scopeBusy,
  onOpen,
  onConnect,
  onDisconnect,
  onRefreshSchemas,
  onEdit,
  onRequestDelete,
  onToggle,
  onSetWorkspaceDisabled,
  onMove,
}: {
  server: CatalogServer;
  workspaces: ScopeWorkspace[];
  selection: BulkSelection;
  connecting: boolean;
  refreshing: boolean;
  toggling: boolean;
  /** A move or a per-workspace deny flip is in flight for this row. */
  scopeBusy: boolean;
  onOpen: () => void;
  onConnect: () => void;
  onDisconnect: () => void;
  onRefreshSchemas: () => void;
  onEdit: () => void;
  onRequestDelete: () => void;
  onToggle: (enabled: boolean) => void;
  onSetWorkspaceDisabled: (workspaceId: string, disabled: boolean) => void;
  onMove: (toWorkspaceId: string) => void;
}) {
  const { t } = useTranslation();
  const oauthEligible = server.transport === 'http';
  const status = server.oauth_status ?? null;

  return (
    <ServerRowShell
      testid={`server-row-${server.name}`}
      {...rowSelection(selection, `catalog:${server.name}`)}
      tile={<IdentityTile name={server.name} />}
      onOpen={onOpen}
      main={
        <>
          <ServerNameLine name={server.name} onOpen={onOpen} />

          {/* Status line: OAuth pill (state needing attention), then quiet
              metadata — scope, tool count, transport. */}
          <div className="flex items-center gap-2 flex-wrap">
            {status && <McpOauthPill status={status} />}
            <MetaText>
              {server.enabled
                ? t('plugins.servers.enabledState')
                : t('plugins.servers.disabledState')}
            </MetaText>
            {status === 'connected' && typeof server.tool_count === 'number' && server.tool_count > 0 && (
              <MetaText>{t('mcp.row.toolCount', { count: server.tool_count })}</MetaText>
            )}
            <MetaText>{server.transport}</MetaText>
            <PluginSuppressedBadge row={server} variant="prose" />
          </div>

          {server.description && (
            <p className="text-[0.6875rem] line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
              {server.description}
            </p>
          )}
        </>
      }
      actions={
        <>
          {oauthEligible && needsOauthConnect(status) && (
            <button
              type="button"
              onClick={onConnect}
              disabled={connecting}
              className="inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
            >
              {connecting ? <Loader size={12} className="text-current" /> : <Link2 className="h-3 w-3" />}
              {status ? t('plugins.oauth.reconnect') : t('plugins.oauth.connect')}
            </button>
          )}

          <ScopeControl
            workspaces={workspaces}
            scopeWorkspaceId={null}
            disabledWorkspaceIds={server.disabled_workspace_ids ?? []}
            checklistLocked={!server.enabled}
            busy={scopeBusy}
            moveBlockedReason={
              // OAuth connections exist only at the user tier, so a connected
              // server cannot move into a workspace. A plugin-owned row stays
              // put too: moving it would orphan the plugin's ownership row.
              isPluginOwned(server)
                ? t('plugins.scope.movePluginBlocked', { plugin: server.plugin_name })
                : status && status !== 'revoked'
                  ? t('plugins.scope.moveOauthBlocked')
                  : null
            }
            onSetWorkspaceDisabled={onSetWorkspaceDisabled}
            onMove={(toWorkspaceId) => {
              if (toWorkspaceId) onMove(toWorkspaceId);
            }}
          />

          {/* Enabled toggle — fans out to every workspace */}
          <EnabledToggle
            enabled={!!server.enabled}
            name={server.name}
            disabled={toggling}
            onToggle={() => onToggle(!server.enabled)}
          />

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <KebabTrigger
                busy={refreshing}
                aria-label={t('mcp.row.actionsAria', { name: server.name })}
              />
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {/* Editing a plugin-owned row detaches it from the plugin, so the
                  item says Customize and carries the consequence in its
                  tooltip. The save path is the same PUT; the backend clears
                  ownership and returns the detach warning, surfaced by
                  onSaveWarnings. */}
              <DropdownMenuItem
                onSelect={onEdit}
                title={
                  isPluginOwned(server)
                    ? t('plugins.component.customizeHint', { plugin: server.plugin_name })
                    : undefined
                }
              >
                <Pencil className="h-3.5 w-3.5 mr-2" />
                {isPluginOwned(server)
                  ? t('plugins.component.customize')
                  : t('mcp.row.edit')}
              </DropdownMenuItem>
              {oauthEligible && status === 'connected' && (
                <DropdownMenuItem onSelect={onRefreshSchemas}>
                  <RefreshCw className="h-3.5 w-3.5 mr-2" />
                  {t('plugins.oauth.refreshSchemas')}
                </DropdownMenuItem>
              )}
              {oauthEligible && canDisconnectOauth(status) && (
                <DropdownMenuItem onSelect={onDisconnect}>
                  <Link2Off className="h-3.5 w-3.5 mr-2" />
                  {t('plugins.oauth.disconnect')}
                </DropdownMenuItem>
              )}
              {/* Not for a plugin-owned row: it belongs to the plugin, and the
                  bulk bar already refuses these. Removing one means Customize
                  (which detaches it) or uninstalling the plugin — offering
                  Delete here promised a third way that does not exist. */}
              {!isPluginOwned(server) && (
                <DropdownMenuItem onSelect={onRequestDelete} variant="destructive">
                  <Trash2 className="h-3.5 w-3.5 mr-2" />
                  {t('mcp.row.delete')}
                </DropdownMenuItem>
              )}
            </DropdownMenuContent>
          </DropdownMenu>
        </>
      }
    />
  );
}
