import { Fragment, useId, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight, Pencil } from 'lucide-react';
import { EnabledToggle } from '@/components/mcp/McpPrimitives';
import { oauthLabelKey } from '@/pages/ChatAgent/components/mcp/McpStatusPill';
import { probeRowState } from '@/pages/ChatAgent/components/mcp/mcpState';
import { useMcpCatalogServerTools } from '@/hooks/useMcpServers';
import { useUserVaultSecrets } from '@/hooks/useUserVault';
import { brokerageArt, mcpServerArt } from '@/lib/brandArt';
import { builtinServerMark } from '../utils/pluginSurface';
import { settledGrant, type Brokerage } from '../brokerages';
import { serverKeyRefs } from '@/hooks/useSecretUsage';
import type { BuiltinMcpServer, CatalogServer } from '@/pages/ChatAgent/utils/api';
import { connectorState, switchOnBlock } from '../utils/connectorState';
import { isPluginOwned } from '../utils/provenance';
import { useSectionNav } from '../hooks/useSectionNav';
import { useVendorOf } from '../hooks/useVendorOf';
import { useHomeOption, useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import {
  DetailAsideBlock,
  DetailHeader,
  DetailPage,
  DetailSection,
} from './DetailPage';
import { BrokerFacts } from './BrokerageDetailParts';
import { ConnectButton } from './OauthRowParts';
import { OrderCapabilityBadges } from './OrderCapabilityBadges';
import { PluginSuppressedBadge } from './PluginBadges';
import { ScopeControl } from './ScopeControl';
import { scopeOff } from '../utils/scopeOff';
import { ConnectorCallout, pickCallout } from './connectors/ConnectorCallout';
import { ServerFacts } from './connectors/ServerFacts';
import { serverDetailVerbs } from './connectors/ServerDetailVerbs';
import { ServerToolsSection } from './connectors/ServerToolsSection';
import { useBindingWrites } from './connectors/useBindingWrites';
import { useConnectorScope } from './connectors/useConnectorScope';
import { useKeyFix } from './connectors/useKeyFix';
import './connectors/connectors.css';

/**
 * A connector's own page, for every origin the Plugins page lists. The union
 * keeps each origin honest about what it knows: builtins carry no user config,
 * catalog rows carry the full config plus the host-side tool snapshot.
 *
 * A brokerage is the one origin that exists before its row does, which is why
 * its `server` is nullable and its identity comes from the registry instead.
 * It is otherwise an ordinary catalog row and shares every section below.
 *
 * The header's switch follows the one rule every surface keeps: a connector is
 * not switched on until it is connected, so while a sign-in or a fix is
 * outstanding the switch holds on its way on, and the callout at the top of
 * the page carries the way through. It reads the row's own flag, which is what
 * workspaces get, so a row a check refused after it was on reads On and can
 * always be switched off.
 */

/** The origins the Connectors tab resolves; a brokerage is not one. */
export type McpServerDetailData =
  | { origin: 'builtin'; server: BuiltinMcpServer }
  | { origin: 'user'; server: CatalogServer };

export type ServerDetailData =
  | McpServerDetailData
  | { origin: 'brokerage'; brokerage: Brokerage; server: CatalogServer | null };

export function ServerDetail({
  data,
  onClose,
  onToggle,
  toggling = false,
  onConnect,
  connecting = false,
  onEdit,
  onRemove,
  removeConfirm,
  onRefreshTools,
  refreshingTools = false,
}: {
  data: ServerDetailData;
  onClose: () => void;
  /** Absent = the surface has no toggle for this row (render read-only). */
  onToggle?: (enabled: boolean) => void;
  toggling?: boolean;
  /** Start or repair the connection, through the caller's connect flow (the
   *  vendor gate and the brokerage consent live there). On a brokerage it is
   *  also the only way to change what the connection was granted, which is
   *  why it stays offered on one that is already connected. */
  onConnect?: () => void;
  connecting?: boolean;
  /** Open the edit form; on a plugin's row the save detaches it. */
  onEdit?: () => void;
  /** Ask to remove a row the user added; never offered on a plugin's row. */
  onRemove?: () => void;
  /** The remove question, when one is open for this row. */
  removeConfirm?: ReactNode;
  /** Discover the tools again, as the row menu's Refresh tools does. */
  onRefreshTools?: () => void;
  refreshingTools?: boolean;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  const { openDetail } = useSectionNav();
  const openPlugin = (plugin: string) => openDetail({ kind: 'plugin', name: plugin });
  const home = useHomeOption();
  const { workspaces, loading: workspacesLoading } = useWorkspaceOptions();
  const { data: vault } = useUserVaultSecrets();
  const { origin } = data;
  const offer = data.origin === 'brokerage' ? data.brokerage : null;
  const catalog =
    data.origin === 'user' || data.origin === 'brokerage' ? data.server : null;
  const builtin = data.origin === 'builtin' ? data.server : null;
  const scope = useConnectorScope(catalog?.name ?? builtin?.name ?? null);
  const catalogTools = useMcpCatalogServerTools(catalog?.name ?? null);
  const writes = useBindingWrites(catalog?.name ?? null);
  const keyFix = useKeyFix(catalog);

  // Resolved the same way the cell that opened this page resolves it, off the
  // address rather than the name: the two surfaces have to agree about which
  // vendor a row still points at, and about holding Connect while the
  // registry has not answered. A brokerage with no row yet has no address to
  // resolve, and the offer it was opened from is the answer.
  const vendorOf = useVendorOf();
  const resolved = vendorOf(catalog?.url);
  const vendor = catalog ? resolved : (offer ?? resolved);
  const redirected = !!offer && !!catalog && vendor?.name !== offer.name;
  // Off the pill's own exhaustive table: a status added later is a compile
  // error there rather than a label that silently goes missing here.
  const oauthLabel = oauthLabelKey(catalog?.oauth_status);
  const granted = settledGrant(catalog?.granted_capabilities, catalog?.oauth_status);
  const groups = vendor?.capabilities ?? [];

  // A brokerage wears the vendor's label until its row is pointed elsewhere,
  // exactly as its row does; every other origin is its own name and always was.
  const name =
    data.origin === 'brokerage'
      ? redirected
        ? (data.server?.name ?? data.brokerage.name)
        : data.brokerage.label
      : data.server.name;
  // `http` is what enabling a brokerage would create, and the only transport an
  // OAuth connect is allowed on; past that first write the row owns the answer.
  const transport =
    data.origin === 'brokerage'
      ? (data.server?.transport ?? 'http')
      : data.server.transport;
  const description =
    data.origin === 'brokerage'
      ? redirected
        ? data.server?.description
        : data.server?.description || data.brokerage.description
      : data.server.description;

  // The connection, for every origin that has one. A builtin runs beside the
  // app and is always connected; a brokerage with no row is not connected yet.
  const state = catalog ? connectorState(catalog) : null;
  // A saved key's re-check holds the switch-on too: the save purged the row's
  // last check, so until the re-check answers nothing else would, and a
  // switch-on in that gap would beat a refusal by a second.
  const block = builtin
    ? null
    : keyFix.failed && probeRowState(keyFix.failed.verdict)?.oauth !== 'wants'
      ? 'failing'
      : state
        ? switchOnBlock(state)
        : 'connect';
  const checkingKey = keyFix.outcome?.phase === 'checking';
  const enabled = builtin ? builtin.enabled : !!catalog?.enabled;
  const held = !enabled && (block !== null || checkingKey);
  const plugin = catalog?.plugin_name ?? builtin?.plugin_name ?? null;
  const owned = !!catalog && isPluginOwned(catalog) && origin === 'user';

  // A server that places orders has a ledger, and this is the way into it. Off
  // the curation rather than the snapshot, so a broker whose tools have never
  // been discovered still offers the link to the orders it already placed.
  const ordersVendor = vendor?.name ?? catalog?.name ?? null;
  const hasOrders =
    !!ordersVendor && (catalogTools.data?.order_modes?.length ?? 0) > 0;

  const stored = new Set((vault?.secrets ?? []).map((s) => s.name));
  const keyNames = catalog ? serverKeyRefs(catalog) : [];
  const connectButton = onConnect
    ? (emphasis: 'quiet' | 'loud', rowKey = `detail-${name}`) => (
        <ConnectButton
          status={catalog?.oauth_status ?? null}
          connecting={connecting}
          vendor={vendor}
          rowKey={rowKey}
          emphasis={emphasis}
          size="md"
          onClick={onConnect}
        />
      )
    : null;
  const editButton = (primary: boolean) =>
    onEdit && (
      <button
        type="button"
        className={primary ? 'connector-btn is-primary' : 'connector-btn'}
        onClick={onEdit}
      >
        <Pencil className="h-3.5 w-3.5" />
        {t('plugins.connectorsPage.edit')}
      </button>
    );
  const callout = pickCallout(state, keyFix, origin === 'brokerage' && !catalog);
  const verbs = serverDetailVerbs(t, {
    brokerage: origin === 'brokerage',
    builtin: !!builtin,
    ordersVendor: hasOrders ? ordersVendor : null,
    reconnect: !callout && connectButton?.('quiet', `brokerage-detail-${name}`),
    plugin,
    owned,
    onOpenPlugin: openPlugin,
    edit: editButton(false),
    onRemove,
  });

  const toggleHint = enabled
    ? undefined
    : block === 'connect'
      ? t('plugins.connectorsPage.toggleNeedsConnect')
      : block === 'failing'
        ? t('plugins.connectorsPage.toggleNeedsFix')
        : undefined;

  const hasScope = !!builtin || !!catalog;

  // The quiet line under the name: where it came from, and how it signs in.
  // What it is, how it connects and how many tools it carries are the
  // breadcrumb's, the facts' and the tools heading's to say, once each.
  const pluginLink = plugin && (
    <button type="button" className="connector-head-link" onClick={() => openPlugin(plugin)}>
      {plugin}
      <ChevronRight className="h-3 w-3" aria-hidden />
    </button>
  );
  const metaParts: ReactNode[] = [];
  if (builtin) metaParts.push(t('plugins.connectorsPage.builtIn'));
  if (plugin && origin !== 'brokerage') {
    metaParts.push(
      <>
        {t('plugins.connectorsPage.partOf')}
        {pluginLink}
      </>,
    );
  } else if (origin === 'user') {
    metaParts.push(t('plugins.connectorsPage.addedByYou'));
  }
  if (origin === 'brokerage' && !catalog) metaParts.push(t('plugins.brokerages.notAdded'));
  if (oauthLabel) metaParts.push(t(oauthLabel));

  return (
    <DetailPage
      labelId={labelId}
      onClose={onClose}
      crumb={name}
      footer={
        (verbs.length > 0 || removeConfirm) && (
          <>
            {verbs}
            {removeConfirm}
          </>
        )
      }
      aside={
        <>
          {hasScope && (
            <DetailAsideBlock title={t('plugins.connectorsPage.availability')}>
              <ScopeControl
                workspaces={workspaces}
                scopeWorkspaceId={null}
                disabledWorkspaceIds={(catalog ?? builtin)?.disabled_workspace_ids ?? []}
                off={scopeOff(catalog ?? builtin ?? {})}
                homeWorkspace={catalog ? home : undefined}
                newWorkspacesOn={catalog?.enabled_in_new_workspaces}
                loading={workspacesLoading}
                busy={scope.busy}
                onSetWorkspaceDisabled={scope.setWorkspaceDisabled}
              />
              {catalog ? (
                <>
                  <div className="connector-switch-row">
                    <span>{t('plugins.connectorsPage.newWorkspaces')}</span>
                    <EnabledToggle
                      enabled={catalog.enabled_in_new_workspaces !== false}
                      name={t('plugins.connectorsPage.newWorkspaces')}
                      disabled={scope.busy}
                      onToggle={() =>
                        scope.setNewWorkspacesOn(catalog.enabled_in_new_workspaces === false)
                      }
                    />
                  </div>
                  {block && (
                    <p className="connector-aside-note">{t('plugins.connectorsPage.takesEffect')}</p>
                  )}
                </>
              ) : (
                <p className="connector-aside-note">{t('plugins.connectorsPage.builtinScopeNote')}</p>
              )}
            </DetailAsideBlock>
          )}
          {vendor && origin === 'brokerage' && (
            <DetailAsideBlock title={t('plugins.brokerages.detail.broker')}>
              <BrokerFacts vendor={vendor} rowUrl={catalog?.url} />
            </DetailAsideBlock>
          )}
          <DetailAsideBlock title={t('plugins.connectorsPage.details')}>
            <ServerFacts
              catalog={catalog}
              transport={transport}
              plugin={plugin}
              onOpenPlugin={origin !== 'brokerage' ? openPlugin : undefined}
              keyNames={keyNames}
              stored={stored}
            />
          </DetailAsideBlock>
        </>
      }
      header={
        <DetailHeader
          name={name}
          labelId={labelId}
          {...(builtin
            ? builtinServerMark(builtin)
            : {
                kind: 'server' as const,
                art: brokerageArt(vendor) ?? (catalog ? mcpServerArt(catalog) : undefined),
              })}
          description={description || undefined}
          meta={
            <>
              {metaParts.map((part, i) => (
                <Fragment key={i}>
                  {i > 0 && <span className="connector-head-sep" aria-hidden>·</span>}
                  <span className="connector-head-meta">{part}</span>
                </Fragment>
              ))}
              <PluginSuppressedBadge row={catalog ?? builtin} variant="prose" />
              {/* The one thing a broker is asked first, in the same words the
                  consent toggle uses for it. */}
              {origin === 'brokerage' && (
                <OrderCapabilityBadges vendor={vendor} granted={granted} />
              )}
            </>
          }
          controls={
            onToggle && (
              // Stays live while the owning plugin is off: the row keeps its
              // own `enabled`, and that flag decides whether it comes back when
              // the plugin does. What holds it on its way on is the
              // connection: a connector is not switched on until it is
              // connected, and the reason sits under the switch, where a held
              // switch is the first thing tried.
              <span className="flex flex-col items-end gap-1.5">
                <span className={held ? 'connector-onoff is-blocked' : 'connector-onoff'}>
                  {enabled ? t('plugins.surface.on') : t('plugins.surface.off')}
                  <EnabledToggle
                    enabled={enabled}
                    name={name}
                    disabled={toggling || held}
                    onToggle={() => onToggle(!enabled)}
                  />
                </span>
                {/* Not on a phone, where it squeezed the name beside it into a
                    sliver; the callout right under the head says the same. */}
                {toggleHint && (
                  <span
                    className="hidden sm:block max-w-60 text-right text-[0.6875rem] leading-snug"
                    style={{ color: 'var(--color-text-tertiary)' }}
                    data-testid="detail-toggle-hint"
                  >
                    {toggleHint}
                  </span>
                )}
              </span>
            )
          }
        />
      }
    >
      {callout && (
        <ConnectorCallout
          callout={callout}
          catalog={catalog}
          state={state}
          name={name}
          vendor={vendor}
          signInKnown={block === 'connect'}
          keyNames={keyNames}
          stored={stored}
          fix={keyFix}
          connect={connectButton}
          edit={editButton(true)}
        />
      )}

      <ServerToolsSection
        catalog={catalog}
        builtin={builtin}
        brokerage={origin === 'brokerage'}
        groups={groups}
        granted={granted}
        signInFirst={block === 'connect'}
        writes={writes}
        // Offered where the row menu offers it: a live sign-in is what the
        // refresh dials with.
        onRefresh={catalog?.oauth_status === 'connected' ? onRefreshTools : undefined}
        refreshing={refreshingTools}
      />

      {catalog?.instruction && (
        <DetailSection title={t('plugins.connectorsPage.howAgentUses')}>
          <p className="connector-prose">{catalog.instruction}</p>
        </DetailSection>
      )}
    </DetailPage>
  );
}
