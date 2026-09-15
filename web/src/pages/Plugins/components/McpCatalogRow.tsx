import { useTranslation } from 'react-i18next';
import { AlertTriangle, KeyRound, Pencil, Trash2 } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { McpOauthPill } from '@/pages/ChatAgent/components/mcp/McpStatusPill';
import {
  needsOauthConnect,
  probeRowState,
  probeStillLanding,
} from '@/pages/ChatAgent/components/mcp/mcpState';
import {
  EnabledToggle,
  KebabTrigger,
  MetaText,
  ServerNameLine,
  ServerRowShell,
} from '@/components/mcp/McpPrimitives';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { brokerageArt, mcpServerArt } from '@/lib/brandArt';
import { type Brokerage } from '../brokerages';
import { useFlashWorkspace } from '../hooks/useFlashWorkspace';
import { isPluginOwned } from '../utils/provenance';
import {
  ConnectButton,
  OauthMenuItems,
  ToolCountText,
  VendorNotes,
} from './OauthRowParts';
import { PluginSuppressedBadge } from './PluginBadges';
import { RowNote } from './RowNote';
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
  const flashWorkspace = useFlashWorkspace();
  const oauthEligible = server.transport === 'http';
  const status = server.oauth_status ?? null;
  // What the host-side probe learned about the row, read through the one
  // verdict table. A row probed and found open or header-authenticated is not
  // an OAuth row, so it gets no Connect button however http it is. Until the
  // probe answers, http still reads as OAuth-eligible, the way it always did;
  // the verdict takes the button away seconds later when the server turns out
  // not to want one.
  const probe = probeRowState(server.probe?.verdict);
  const oauthByProbe = probe?.oauth === 'wants';
  // Only while a verdict can still arrive. Afterwards the slot goes quiet: a
  // row with no verdict is a row with no verdict, and Connect already treats
  // one leniently.
  const checking = probeStillLanding(server);
  // What the row can still claim from OAuth. A revoked connection is history
  // once the headers answer on their own: discovery reads a revoked claim as no
  // claim and issues the header grant, so the row has to say the server is
  // usable rather than keep asking for a connection it no longer needs.
  // `status` still speaks where the connection's own history decides.
  const claim = status === 'revoked' && probe?.oauth === 'no' ? null : status;
  // Only a verdict that settled the question takes the button away. A check
  // that never reached the server learned nothing about auth, so it leaves the
  // row where an unprobed one sits.
  const unconnected =
    oauthEligible && needsOauthConnect(claim) && (!!claim || !probe || probe.oauth !== 'no');
  // A live OAuth status is the dominant answer, so its pill speaks instead,
  // and where Connect is on screen it already says what the OAuth note would.
  const probeNote =
    !claim && probe?.noteKey && !(oauthByProbe && unconnected)
      ? { key: probe.noteKey, tone: probe.tone, wire: probe.wire }
      : null;
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
            {claim && <McpOauthPill status={claim} />}
            <MetaText>
              {server.enabled
                ? t('plugins.servers.enabledState')
                : t('plugins.servers.disabledState')}
            </MetaText>
            <ToolCountText status={claim} count={server.tool_count} />
            {checking && !claim && <MetaText>{t('mcp.probe.rowChecking')}</MetaText>}
            {unconnected && !claim && oauthByProbe && <MetaText>{t('mcp.probe.rowOauth')}</MetaText>}
            <MetaText>{server.transport}</MetaText>
            <PluginSuppressedBadge row={server} variant="prose" />
            <VendorNotes vendor={vendor} unconnected={unconnected} rowKey={rowKey} />
          </div>

          {probeNote && (
            <RowNote
              icon={probeNote.tone === 'warning' ? AlertTriangle : KeyRound}
              tone={probeNote.tone}
            >
              {(probeNote.wire && server.probe?.error) || t(probeNote.key)}
            </RowNote>
          )}

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
              status={claim}
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
            // Flash has no sandbox, so it can install only directly bound
            // tools. The server answers whether this row has any; offering
            // Flash on a row that has none is a switch that does nothing.
            flashWorkspace={server.has_direct_tools ? flashWorkspace : undefined}
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
