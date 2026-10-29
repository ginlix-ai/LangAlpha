import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from '@/lib/framer';
import { toast } from '@/components/ui/use-toast';
import {
  useBrokerages,
  useMcpCatalog,
  useBuiltinMcpServers,
  useToggleBuiltinMcpServer,
  useCreateMcpCatalogServer,
  useUpdateMcpCatalogServer,
  useDeleteMcpCatalogServer,
  useToggleMcpCatalogServer,
  useImportMcpCatalogServers,
} from '@/hooks/useMcpServers';
import { McpServerModal } from '@/pages/ChatAgent/components/mcp/McpServerModal';
import { McpImportModal } from '@/pages/ChatAgent/components/mcp/McpImportModal';
import { useMcpServerList } from '@/pages/ChatAgent/components/mcp/useMcpServerList';
import { toastConnectorAdded } from '@/pages/ChatAgent/components/mcp/mcpAddedToast';
import { ConfirmStrip } from '@/components/mcp/McpPrimitives';
import {
  formatApiErrorDetail,
  probeMcpServer,
  type CatalogServer,
} from '@/pages/ChatAgent/utils/api';
import { refusalSaying } from '@/pages/ChatAgent/utils/api/refusal';
import { invalidateMcpFanout } from '@/hooks/usePlugins';
import { useAddIntent } from '../hooks/useAddIntent';
import { useDetailParam } from '../hooks/useDetailParam';
import { markJustAdded } from '../hooks/useJustAdded';
import { useMcpBulkActions } from '../hooks/useMcpBulkActions';
import { connectRequestFor } from '../hooks/useMcpOauthActions';
import { useSectionNav } from '../hooks/useSectionNav';
import { usePluginListSurface } from '../hooks/usePluginListSurface';
import { useSignInRetry } from '../hooks/useSignInRetry';
import { useVendorOf } from '../hooks/useVendorOf';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { isEffectivelyEnabled, isPluginOwned } from '../utils/provenance';
import { connectorRowKey, type ConnectorRow } from '../utils/rowKeys';
import { CatalogStart } from './CatalogStart';
import { REGISTRY_NOTE_ID } from './OauthRowParts';
import { PluginListFrame } from './PluginListFrame';
import { RetryNote } from './RetryNote';
import { ServerDetail, type McpServerDetailData } from './ServerDetail';
import { CellModeProvider, SectionLink, SurfaceGrid, SurfaceSection } from './surface/Surface';
import {
  BuiltinConnectorCell,
  BuiltinConnectorMenu,
  ConnectorCell,
  ConnectorMenu,
} from './connectors/ConnectorCells';
import { connectorNeedsUser } from './connectors/attention';
import { connectorMatches, groupConnectors } from './connectors/groupConnectors';
import './connectors/connectors.css';

/**
 * The Connectors section, Yours view: every MCP server the account has, by
 * where it came from. What needs the user comes first, then what they added by
 * hand, one group per installed plugin, and what ships with the app. Every
 * server lives on the account: an enabled row is on in every workspace that
 * has not switched it off, and a disabled row is off everywhere.
 *
 * A cell carries one state and a menu; availability, tools and the fix for a
 * problem live on the connector's own page (`?detail=server:NAME`), which this
 * file renders in place of the list. The list stays mounted underneath so a
 * fold opened or a scroll position is still there on the way back.
 *
 * Remote (http) servers carry the OAuth connect lifecycle; the vendor bearer
 * never leaves the host, so "Connect" here is all a sandbox needs.
 */

/** Groups longer than this many rows fold behind a "See …" line. */
const OWN_ROWS = 3;
const GROUP_ROWS = 2;

export function McpServers() {
  const { t } = useTranslation();
  const catalogQuery = useMcpCatalog();
  const { data: catalog, isLoading } = catalogQuery;
  const {
    data: builtinData,
    error: builtinError,
    refetch: refetchBuiltins,
  } = useBuiltinMcpServers();
  const builtinToggleMutation = useToggleBuiltinMcpServer();
  const createMutation = useCreateMcpCatalogServer();
  const updateMutation = useUpdateMcpCatalogServer();
  const deleteMutation = useDeleteMcpCatalogServer();
  const toggleMutation = useToggleMcpCatalogServer();
  const importMutation = useImportMcpCatalogServers();
  const { workspaces: wsOptions, loading: wsLoading } = useWorkspaceOptions();
  const { openDetail } = useSectionNav();
  // One observer for the whole list. The registry is static and identical for
  // every row, so asking per row bought nothing and cost an observer per server.
  const {
    data: brokerages,
    error: brokeragesError,
    refetch: refetchBrokerages,
  } = useBrokerages();
  // Resolved off the address rather than the row's identity: a brokerage row
  // is the user's to edit once it exists, so the vendor's constraints follow
  // wherever the URL still points.
  const vendorOf = useVendorOf();

  const {
    oauth,
    togglingName,
    deletingName,
    openAdd,
    openAddWith,
    openEdit,
    openImport,
    toggle,
    requestDelete,
    cancelDelete,
    confirmDelete,
    modal,
    importer,
  } = useMcpServerList<CatalogServer>({
    create: createMutation.mutateAsync,
    update: updateMutation.mutateAsync,
    toggle: toggleMutation.mutateAsync,
    remove: deleteMutation.mutateAsync,
    // Removing a connector removes it from every workspace, so the strip
    // confirms first, and stays up if the delete fails, to retry or cancel.
    confirmBeforeDelete: true,
    onSaveWarnings: (warnings) =>
      toast({ title: t('plugins.servers.warningTitle'), description: warnings.join('\n') }),
    onCreated: ({ name, toolCount }) => {
      markJustAdded('server', name);
      toastConnectorAdded({ name, toolCount, onOpen: () => detail.open(name), t });
    },
    onToggleWarnings: (warnings) =>
      toast({ title: t('plugins.servers.enabledWithWarnings'), description: warnings.join('\n') }),
    onToggleError: (err) =>
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      }),
    onDeleteError: (err) =>
      toast({
        variant: 'destructive',
        title: t('plugins.servers.deleteFailed'),
        description: formatApiErrorDetail(err),
      }),
    pendingFor: (server, editing) =>
      editing
        ? { kind: 'server_edit', name: editing.name, server, expectedUrl: editing.url ?? null }
        : { kind: 'server', server },
  });

  // An error only speaks for the rows when there is nothing to fall back on. A
  // refetch that fails still leaves the answer the query already has, and this
  // registry is what the build ships, so that answer is as good as it was.
  const registryUnavailable = !brokerages && !!brokeragesError;
  const [builtinTogglingName, setBuiltinTogglingName] = useState<string | null>(null);

  useAddIntent({ server: openAdd, import: openImport });

  const servers = catalog?.servers ?? [];
  // A save that went by way of a sign-in came back without finishing: its
  // form opens again on what it carried, and says why.
  useSignInRetry(['server', 'server_edit'], !!catalog, (retry, { note, error }) => {
    const said = { note, refusal: error ? refusalSaying(error) : undefined };
    if (retry.kind === 'server') openAddWith(retry.server, said);
    const row = retry.kind === 'server_edit' && servers.find((s) => s.name === retry.name);
    if (row) openEdit(row, { edits: retry.server, said });
  });
  const builtinServers = builtinData?.servers ?? [];
  const maxServers = catalog?.max_servers ?? 0;

  // One list over both tiers, so the pills count and Select reaches the
  // user's rows and the ones the app ships alike.
  const list = usePluginListSurface<ConnectorRow>({
    rows: [
      ...servers.map((server) => ({ tier: 'catalog' as const, server })),
      ...builtinServers.map((server) => ({ tier: 'builtin' as const, server })),
    ],
    matches: (row, query) => connectorMatches(query, row.server),
    key: connectorRowKey,
    name: (row) => row.server.name,
    isOn: (row) => isEffectivelyEnabled(row.server),
    // A built-in never needs the user: it has no sign-in and no key of its
    // own to go wrong.
    needsUser: (row) => row.tier === 'catalog' && connectorNeedsUser(row.server),
    justAdded: 'server',
    startWhen: !!catalog && servers.length === 0,
    invalidate: invalidateMcpFanout,
  });
  const shownServers = list.visible.flatMap((row) => (row.tier === 'catalog' ? [row.server] : []));
  const shownBuiltins = list.visible.flatMap((row) => (row.tier === 'builtin' ? [row.server] : []));
  const groups = groupConnectors(shownServers, shownBuiltins);
  const ownTotal = servers.filter((s) => !s.plugin_name).length;

  const bulk = useMcpBulkActions({
    builtins: shownBuiltins,
    catalog: shownServers,
    surface: list,
    workspaces: wsOptions,
    signIn: (needed) => {
      const row = servers.find((s) => s.name === needed[0]?.server);
      if (row) oauth.connect(connectRequestFor(row));
    },
  });

  // --- The connector's page (?detail=server:NAME) ---
  // Builtin names are reserved against catalog names, and a catalog name is
  // unique per account, so a bare name lookup is unambiguous.
  const detail = useDetailParam<McpServerDetailData>(
    'server',
    (ref) => {
      const builtin = builtinServers.find((s) => s.name === ref.name);
      if (builtin) return { origin: 'builtin' as const, server: builtin };
      const cat = servers.find((s) => s.name === ref.name);
      return cat ? { origin: 'user' as const, server: cat } : null;
    },
    !isLoading && catalog !== undefined && builtinData !== undefined,
  );
  const detailData = detail.target;
  const detailServer = detailData?.origin === 'user' ? detailData.server : null;

  async function handleToggleBuiltin(name: string, enabled: boolean) {
    setBuiltinTogglingName(name);
    try {
      await builtinToggleMutation.mutateAsync({ name, enabled });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setBuiltinTogglingName(null);
    }
  }

  function connect(server: CatalogServer) {
    // One question at a time: a remove question already on screen belongs to
    // a different row and its Yes is not this one's.
    cancelDelete();
    oauth.connect(connectRequestFor(server));
  }

  function askRemove(server: CatalogServer) {
    oauth.cancelPending();
    requestDelete(server);
  }

  const renderCell = (server: CatalogServer) => (
    <CellModeProvider key={server.name} value={list.cellMode({ tier: 'catalog', server })}>
      <ConnectorCell
        server={server}
        vendor={vendorOf(server.url)}
        registryUnavailable={registryUnavailable}
        workspaces={wsOptions}
        workspacesLoading={wsLoading}
        connecting={oauth.connectingName === server.name}
        refreshing={oauth.refreshingName === server.name}
        onOpen={() => detail.open(server.name)}
        onConnect={() => connect(server)}
        menu={
          <ConnectorMenu
            server={server}
            toggling={togglingName === server.name}
            onOpen={() => detail.open(server.name)}
            onToggle={(enabled) => toggle(server, enabled)}
            onEdit={() => openEdit(server)}
            onDisconnect={() => oauth.disconnect(server.name)}
            onRefreshSchemas={() => oauth.refreshSchemas(server.name)}
            onRemove={() => askRemove(server)}
          />
        }
      />
    </CellModeProvider>
  );

  const removeConfirm = deletingName && (
    <div className={detailData ? 'connector-confirm-aside' : 'connector-confirm'}>
      <ConfirmStrip
        message={t('plugins.connectorsPage.removeConfirm', { server: deletingName })}
        confirmLabel={
          deleteMutation.isPending ? t('common.loading') : t('plugins.connectorsPage.remove')
        }
        cancelLabel={t('plugins.servers.deleteConfirmNo')}
        pending={deleteMutation.isPending}
        onConfirm={confirmDelete}
        onCancel={cancelDelete}
      />
    </div>
  );

  // A plugin with one connector does not get a full-width band of its own:
  // the single-connector groups share one row of columns at the end.
  const multiPlugins = groups.plugins.filter((g) => g.servers.length > 1);
  const singlePlugins = groups.plugins.filter((g) => g.servers.length === 1);
  const pluginSection = (group: (typeof groups.plugins)[number]) => (
    <SurfaceSection
      key={group.plugin}
      title={group.plugin}
      count={group.servers.length}
      note={group.pluginOff ? t('plugins.connectorsPage.pluginOff') : undefined}
      action={
        <SectionLink onClick={() => openDetail({ kind: 'plugin', name: group.plugin })}>
          {t('plugins.connectorsPage.openPlugin')}
        </SectionLink>
      }
    >
      <SurfaceGrid {...list.fold(GROUP_ROWS, group.servers.map((s) => s.name))}>
        {group.servers.map(renderCell)}
      </SurfaceGrid>
    </SurfaceSection>
  );

  // The cap is the account's, counted over every catalog row; it is only worth
  // a word when it is close.
  const capNote =
    maxServers > 0 && servers.length >= Math.floor(maxServers * 0.8)
      ? t('plugins.connectorsPage.capUsed', { used: servers.length, max: maxServers })
      : undefined;

  let ownBody: ReactNode = null;
  if (groups.own.length > 0) {
    ownBody = (
      <SurfaceGrid {...list.fold(OWN_ROWS, groups.own.map((s) => s.name))}>
        {groups.own.map(renderCell)}
      </SurfaceGrid>
    );
  } else if (ownTotal === 0 && !list.narrowed && !list.showStart) {
    // Plugin connectors but none added by hand: the section stays, as the
    // place a connector added by URL or command would go.
    ownBody = (
      <p className="connector-quiet">
        {t('plugins.connectorsPage.empty')}
        <button type="button" onClick={openAdd}>
          {t('plugins.connectorsPage.addOne')}
        </button>
      </p>
    );
  }

  // What ships with the app is another query: it stays whatever the user's
  // own list says, loading or failed.
  const builtinSection = groups.builtins.length > 0 && (
    <SurfaceSection title={t('plugins.connectorsPage.builtIn')} count={groups.builtins.length}>
      <SurfaceGrid {...list.fold(GROUP_ROWS, groups.builtins.map((s) => s.name))}>
        {groups.builtins.map((server) => (
          <CellModeProvider
            key={server.name}
            value={list.cellMode({ tier: 'builtin', server })}
          >
            <BuiltinConnectorCell
              server={server}
              workspaces={wsOptions}
              workspacesLoading={wsLoading}
              busy={builtinTogglingName === server.name}
              onOpen={() => detail.open(server.name)}
              menu={
                <BuiltinConnectorMenu
                  server={server}
                  busy={builtinTogglingName === server.name}
                  onOpen={() => detail.open(server.name)}
                  onToggle={(enabled) => handleToggleBuiltin(server.name, enabled)}
                />
              }
            />
          </CellModeProvider>
        ))}
      </SurfaceGrid>
    </SurfaceSection>
  );

  return (
    <div>
      {/* Above every group rather than inside one, because the rows it holds
          are spread across all of them: a plugin's server connects through the
          same button as one the user typed. Held rather than let through, since
          a broker the page cannot recognise gets the plain Connect and, for a
          vendor whose consent screen this build cannot reach, a dead end. */}
      {(registryUnavailable || !!builtinError) && !detailData && (
        <div className="connector-notes">
          {registryUnavailable && (
            <RetryNote
              id={REGISTRY_NOTE_ID}
              text={t('plugins.oauth.registryUnavailableNote')}
              onRetry={() => void refetchBrokerages()}
            />
          )}
          {/* Without a notice a failed load reads as "this build ships
              nothing", while the user's own groups render fine beside it. */}
          {!!builtinError && (
            <RetryNote text={t('plugins.mcp.builtinLoadFailed')} onRetry={() => void refetchBuiltins()} />
          )}
        </div>
      )}

      <PluginListFrame
        list={list}
        query={catalogQuery}
        loadFailed={t('plugins.loadFailed.mcp')}
        noun={t('plugins.filter.nounConnectors')}
        catalogScope="connectors"
        bulk={bulk}
        hidden={!!detailData}
        groupsClassName="connector-groups"
        below={builtinSection}
        confirm={!detailData && removeConfirm}
      >
        {list.showStart && (
          <CatalogStart
            scope="connectors"
            onAdd={(intent) => (intent === 'import' ? openImport() : openAdd())}
          />
        )}

        {groups.attention.length > 0 && (
          <SurfaceSection
            title={t('plugins.connectorsPage.needsAttention')}
            count={groups.attention.length}
            attention
          >
            <SurfaceGrid>{groups.attention.map(renderCell)}</SurfaceGrid>
          </SurfaceSection>
        )}

        {ownBody && (
          <SurfaceSection
            title={t('plugins.connectorsPage.addedByYou')}
            count={groups.own.length}
            note={capNote}
          >
            {ownBody}
          </SurfaceSection>
        )}

        {multiPlugins.map(pluginSection)}

        {singlePlugins.length > 0 && (
          <div className="connector-minis">{singlePlugins.map(pluginSection)}</div>
        )}
      </PluginListFrame>

      <AnimatePresence>
        {modal && (
          <McpServerModal
            {...modal}
            allowDiscover={false}
            onProbe={probeMcpServer}
            saving={createMutation.isPending || updateMutation.isPending}
          />
        )}
      </AnimatePresence>

      <AnimatePresence>
        {importer && (
          <McpImportModal
            {...importer}
            onImport={async (payload) => {
              const report = await importMutation.mutateAsync(payload);
              for (const row of report.results) {
                if (row.status === 'created') markJustAdded('server', row.name);
              }
              return report;
            }}
            onEnable={(name) =>
              toggleMutation.mutateAsync({ name, enabled: true }).then(
                () => undefined,
                (err) => {
                  // Offered as the list's own switch offers it; the import's
                  // row says why it stayed off either way.
                  const row = servers.find((s) => s.name === name);
                  if (row) oauth.offerSignInIfRefused(err, connectRequestFor(row));
                  throw err;
                },
              )
            }
          />
        )}
      </AnimatePresence>

      <AnimatePresence>
        {detailData && (
          <ServerDetail
            key={`${detailData.origin}:${detailData.server.name}`}
            data={detailData}
            onClose={() => {
              cancelDelete();
              detail.close();
            }}
            toggling={
              detailData.origin === 'builtin'
                ? builtinTogglingName === detailData.server.name
                : togglingName === detailData.server.name
            }
            onToggle={(enabled) => {
              if (detailData.origin === 'builtin') {
                handleToggleBuiltin(detailData.server.name, enabled);
              } else {
                toggle(detailData.server, enabled);
              }
            }}
            connecting={!!detailServer && oauth.connectingName === detailServer.name}
            onConnect={detailServer ? () => connect(detailServer) : undefined}
            onEdit={detailServer ? () => openEdit(detailServer) : undefined}
            onRemove={
              detailServer && !isPluginOwned(detailServer) ? () => askRemove(detailServer) : undefined
            }
            removeConfirm={
              detailServer && deletingName === detailServer.name ? removeConfirm : undefined
            }
            onRefreshTools={detailServer ? () => oauth.refreshSchemas(detailServer.name) : undefined}
            refreshingTools={!!detailServer && oauth.refreshingName === detailServer.name}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
