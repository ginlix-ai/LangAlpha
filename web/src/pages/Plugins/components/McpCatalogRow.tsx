import { useTranslation } from 'react-i18next';
import { Pencil, Trash2 } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { McpOauthPill } from '@/pages/ChatAgent/components/mcp/McpStatusPill';
import { needsOauthConnect } from '@/pages/ChatAgent/components/mcp/mcpState';
import {
  EnabledToggle,
  KebabTrigger,
  MetaText,
  ServerNameLine,
  ServerRowShell,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { brokerageArt, mcpServerArt } from '@/lib/brandArt';
import { type Brokerage } from '../brokerages';
import { isPluginOwned } from '../utils/provenance';
import {
  ConnectButton,
  OauthMenuItems,
  ToolCountText,
  VendorNotes,
} from './OauthRowParts';
import { PluginSuppressedBadge } from './PluginBadges';
import { ScopeControl, scopeLocked, type ScopeWorkspace } from './ScopeControl';
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
  vendor,
  registryUnavailable,
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
  /** Which shipped brokerage this row's URL resolves to, `null` for none, and
   *  `undefined` while the registry is still unanswered. Resolved by the list
   *  rather than here: the registry is one static query, and asking it per row
   *  put an observer behind every server the user owns. */
  vendor: Brokerage | null | undefined;
  /** The registry was asked and did not answer, as opposed to not yet having
   *  been answered. Only decides whether the held button has a note to point
   *  at; what holds it is `vendor` being unresolved either way. */
  registryUnavailable?: boolean;
  workspaces: ScopeWorkspace[];
  selection: BulkSelection;
  connecting: boolean;
  refreshing: boolean;
  toggling: boolean;
  /** A move or a per-workspace deny flip is in flight for this row. */
  scopeBusy: boolean;
  onOpen: () => void;
  /** Handed the vendor this row's URL resolves to, so the caller need not
   *  resolve it a second time and reach a different answer. Never fires while
   *  that is `undefined`: `ConnectButton` holds the one gate, and an unresolved
   *  registry is one of the things it refuses on. */
  onConnect: (vendor: Brokerage | null | undefined) => void;
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
  const unconnected = oauthEligible && needsOauthConnect(status);
  const rowKey = `catalog-${server.name}`;

  return (
    <ServerRowShell
      testid={`server-row-${server.name}`}
      {...rowSelection(selection, `catalog:${server.name}`)}
      tile={
        <BrandMark
          name={server.name}
          kind="server"
          art={brokerageArt(vendor) ?? mcpServerArt(server)}
        />
      }
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
            <ToolCountText status={status} count={server.tool_count} />
            <MetaText>{server.transport}</MetaText>
            <PluginSuppressedBadge row={server} variant="prose" />
            <VendorNotes vendor={vendor} unconnected={unconnected} rowKey={rowKey} />
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
          {unconnected && (
            <ConnectButton
              status={status}
              connecting={connecting}
              vendor={vendor}
              registryUnavailable={registryUnavailable}
              rowKey={rowKey}
              testid={`catalog-connect-${server.name}`}
              onClick={() => onConnect(vendor)}
            />
          )}

          <ScopeControl
            workspaces={workspaces}
            scopeWorkspaceId={null}
            disabledWorkspaceIds={server.disabled_workspace_ids ?? []}
            checklistLocked={scopeLocked(server)}
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
              <OauthMenuItems
                status={oauthEligible ? status : null}
                onRefreshSchemas={onRefreshSchemas}
                onDisconnect={onDisconnect}
              />
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
