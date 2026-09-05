import { useTranslation } from 'react-i18next';
import { AlertTriangle, Server, Trash2 } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { brokerageArt } from '@/lib/brandArt';
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
import { settledGrant, type Brokerage } from '../brokerages';
import { isPluginOwned } from '../utils/provenance';
import {
  ConnectButton,
  OauthMenuItems,
  ToolCountText,
  VendorNotes,
} from './OauthRowParts';
import { OrderCapabilityBadges } from './OrderCapabilityBadges';
import { RowNote } from './RowNote';
import { ScopeControl, scopeLocked, type ScopeWorkspace } from './ScopeControl';

/**
 * One shipped brokerage, in whichever of its two states the user is in: an
 * offer with no row behind it yet, or their own catalog row.
 *
 * Both states are one row rather than two components because the second state
 * is the first one plus a row — the identity, the vendor's constraints and the
 * description never change, and rendering them from two places is how the two
 * would eventually disagree about a broker that can place orders.
 *
 * Every action here is the same mutation the MCP tab's row calls, so a
 * brokerage has exactly one implementation of connect, toggle, scope and
 * remove no matter which surface the user found it on. What this row does not
 * offer is Edit: the address is the one thing we pick rather than the user, so
 * changing it means going to the tab where it is an ordinary server.
 */
export function BrokerageRow({
  brokerage,
  row,
  vendor,
  workspaces,
  connecting,
  refreshing,
  toggling,
  scopeBusy,
  onConnect,
  onDisconnect,
  onRefreshSchemas,
  onToggle,
  onRequestRemove,
  onSetWorkspaceDisabled,
  onOpenInMcpTab,
  onOpen,
}: {
  brokerage: Brokerage;
  /** The user's catalog row for it, or null while this is still an offer. */
  row: CatalogServer | null;
  /** Which shipped brokerage this row's URL actually resolves to, `null` once
   *  it has been pointed somewhere else. Resolved by the list rather than here,
   *  so the tab paragraph and this row cannot reach different answers. */
  vendor: Brokerage | null;
  workspaces: ScopeWorkspace[];
  connecting: boolean;
  refreshing: boolean;
  toggling: boolean;
  scopeBusy: boolean;
  /** Handed the vendor this row's URL still resolves to, which is null once
   *  the row has been pointed somewhere else. */
  onConnect: (vendor: Brokerage | null) => void;
  onDisconnect: () => void;
  onRefreshSchemas: () => void;
  onToggle: (enabled: boolean) => void;
  onRequestRemove: () => void;
  onSetWorkspaceDisabled: (workspaceId: string, disabled: boolean) => void;
  onOpenInMcpTab: () => void;
  /** Open this broker's detail. Offered on an unadded row too: what a broker
   *  can do is the thing to read before deciding to add it. */
  onOpen: () => void;
}) {
  const { t } = useTranslation();
  const status = row?.oauth_status ?? null;
  // The same question the MCP tab asks of its own rows, because past the first
  // write this is one of them: the registry ships an http address, but the row
  // it creates is the user's to edit, transport included. The backend refuses
  // an OAuth connect on anything else, so a row pointed at a local command has
  // a Connect button whose only outcome is a failure toast. An offer has no row
  // to ask, and what it would create is always http.
  const oauthEligible = !row || row.transport === 'http';
  const unconnected = oauthEligible && needsOauthConnect(status);
  // One reading of the grant for the whole row: the badges and the note
  // below both draw it, and they used to disagree about null.
  const granted = settledGrant(row?.granted_capabilities, status);
  const rowKey = `brokerage-${brokerage.name}`;
  // Their row, their address: once it exists the URL is editable in the MCP
  // tab, and a row pointing somewhere else is no longer this broker whatever
  // the name still says. Worth saying out loud on a row that places orders.
  //
  // A row that is no longer this broker does not get this broker's constraints
  // either, which is why `vendor` and not `brokerage` is what the button and the
  // notes read. The name is what the tab found it by; the address is what it is.
  //
  // Compared against this row's own broker rather than tested for null, because
  // an address can be moved to the OTHER shipped broker: that resolves to a
  // vendor, and a null test read it as "still Robinhood". The row went on
  // wearing Robinhood's name and label while the notes and the connect button,
  // which read `vendor`, had already switched to IBKR's terms.
  const redirected = !!row && vendor?.name !== brokerage.name;
  // A redirected row stops being presented as the broker. It kept the vendor's
  // label and, with no description of its own, the vendor's description too --
  // so a row pointing at an address the user chose was still introduced as
  // "Interactive Brokers account: portfolio, positions...". It goes back to
  // wearing its own name, which is what it is now. The mark below follows the
  // same rule and drops the vendor's art, but the monogram it falls back to
  // stays keyed on the row's name, which is the one thing that did not move --
  // re-keying that would split the tint across the two tabs.
  const title = redirected ? (row?.name ?? brokerage.name) : brokerage.label;
  const description = redirected
    ? row?.description
    : row?.description || brokerage.description;

  return (
    <ServerRowShell
      testid={rowKey}
      onOpen={onOpen}
      // Off `vendor`, like every other vendor-specific thing on this row: a
      // row edited to another host keeps its name and drops to a monogram,
      // rather than drawing a broker's mark over somebody else's endpoint.
      tile={
        <BrandMark
          name={brokerage.name}
          kind="server"
          art={brokerageArt(vendor)}
        />
      }
      main={
        <>
          <ServerNameLine name={title} onOpen={onOpen} />

          <div className="flex items-center gap-2 flex-wrap">
            {status && <McpOauthPill status={status} />}
            {row ? (
              <MetaText>
                {row.enabled
                  ? t('plugins.servers.enabledState')
                  : t('plugins.servers.disabledState')}
              </MetaText>
            ) : (
              <MetaText>{t('plugins.brokerages.notAdded')}</MetaText>
            )}
            <ToolCountText status={status} count={row?.tool_count} />
            {/* What this broker can do about orders, which is the first thing
                anyone wants off a brokerage row and the last thing the page
                could say. Before a connection they are what it offers; after
                one they are what it may actually do. */}
            <OrderCapabilityBadges vendor={vendor} granted={granted} />
            {/* Connected and granted nothing: every tool is refused, and the
                only other sign of it is the agent failing to call one.

                Read off `settledGrant`, the same value the badges above draw,
                so the row cannot say "offered" and "granted nothing" in the
                same breath -- which is what it did for a brokerage connected
                before its tools were curated, whose stored answer is null. */}
            {granted?.length === 0 && (
              <RowNote icon={AlertTriangle} tone="warning">
                {t('plugins.brokerages.grantedNone')}
              </RowNote>
            )}
            {redirected && (
              <RowNote icon={AlertTriangle} tone="warning">
                {t('plugins.brokerages.redirectedNote')}
              </RowNote>
            )}
            <VendorNotes vendor={vendor} unconnected={unconnected} rowKey={rowKey} />
          </div>

          {description && (
            <p
              className="text-[0.6875rem] line-clamp-2"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {description}
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
              rowKey={rowKey}
              // Filled while this is still an offer: it is the one thing the
              // tab is asking for. Once a row exists, reconnecting is a repair
              // and drops back to the quiet outline every other row uses.
              emphasis={row ? 'quiet' : 'loud'}
              testid={`brokerage-connect-${brokerage.name}`}
              onClick={() => onConnect(vendor)}
            />
          )}

          {row && (
            <>
              <ScopeControl
                workspaces={workspaces}
                scopeWorkspaceId={null}
                disabledWorkspaceIds={row.disabled_workspace_ids ?? []}
                checklistLocked={scopeLocked(row)}
                // A brokerage is an account-wide identity, so the only scope
                // question it has is which workspaces may reach it. Moving one
                // into a single workspace would strand the OAuth connection,
                // which exists at the user tier and nowhere else.
                allowWorkspaceTargets={false}
                moveBlockedReason={t('plugins.scope.moveOauthBlocked')}
                busy={scopeBusy}
                onSetWorkspaceDisabled={onSetWorkspaceDisabled}
              />

              <EnabledToggle
                enabled={!!row.enabled}
                name={title}
                disabled={toggling}
                onToggle={() => onToggle(!row.enabled)}
              />

              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <KebabTrigger
                    busy={refreshing}
                    aria-label={t('mcp.row.actionsAria', { name: title })}
                  />
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <OauthMenuItems
                    status={status}
                    onRefreshSchemas={onRefreshSchemas}
                    onDisconnect={onDisconnect}
                  />
                  {/* The escape hatch, named for where it goes rather than for
                      what it does there: the address and headers stay editable,
                      just not from the tab whose whole point is that we picked
                      the address. */}
                  <DropdownMenuItem onSelect={onOpenInMcpTab}>
                    <Server className="h-3.5 w-3.5 mr-2" />
                    {t('plugins.brokerages.openInMcp')}
                  </DropdownMenuItem>
                  {/* Hidden on a plugin's row, the way the Connectors tab
                      hides it: that row belongs to the plugin, and the delete
                      route's ownership check does not stop this one, so the
                      only thing standing between the two surfaces agreeing is
                      this condition. Removing it there means uninstalling. */}
                  {!isPluginOwned(row) && (
                    <DropdownMenuItem onSelect={onRequestRemove} variant="destructive">
                      <Trash2 className="h-3.5 w-3.5 mr-2" />
                      {t('mcp.row.delete')}
                    </DropdownMenuItem>
                  )}
                </DropdownMenuContent>
              </DropdownMenu>
            </>
          )}
        </>
      }
    />
  );
}
