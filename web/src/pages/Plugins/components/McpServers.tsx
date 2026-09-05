import { useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { AlertTriangle, Blocks, Folder, Plus, Server } from 'lucide-react';
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
  useSetMcpServerEnabledInWorkspace,
  useAdoptMcpServerToWorkspace,
  usePromoteMcpServerToTemplate,
} from '@/hooks/useMcpServers';
import { invalidateMcpFanout } from '@/hooks/usePlugins';
import { useUserVaultSecrets, useCreateUserVaultSecret } from '@/hooks/useUserVault';
import { McpServerModal } from '@/pages/ChatAgent/components/mcp/McpServerModal';
import { McpImportModal } from '@/pages/ChatAgent/components/mcp/McpImportModal';
import { isOauthBroken } from '@/pages/ChatAgent/components/mcp/mcpState';
import { useMcpServerList } from '@/pages/ChatAgent/components/mcp/useMcpServerList';
import {
  ConfirmStrip,
  HeaderButton,
  ListEmpty,
  ListError,
  ListHeader,
  ListSkeleton,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import {
  formatApiErrorDetail,
  type BuiltinMcpServer,
  type CatalogServer,
} from '@/pages/ChatAgent/utils/api';
import { groupBy, matchesFilter } from '../utils/groupOrigins';
import { isEffectivelyEnabled, isPluginSuppressed } from '../utils/provenance';
import { withDetail } from '../utils/detailParam';
import { useAddIntent } from '../hooks/useAddIntent';
import { useDetailParam } from '../hooks/useDetailParam';
import { useMcpBulkActions } from '../hooks/useMcpBulkActions';
import { useMcpOauthActions } from '../hooks/useMcpOauthActions';
import { usePluginListSurface } from '../hooks/usePluginListSurface';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { BrokerageConsentDialog } from './BrokerageConsentDialog';
import { BuiltinMcpRow } from './BuiltinMcpRow';
import { BulkActionBar } from './BulkActionBar';
import { EmptyState } from './EmptyState';
import { GroupDeck } from './GroupDeck';
import { ListControls } from './ListControls';
import { McpCatalogRow } from './McpCatalogRow';
import { REGISTRY_NOTE_ID } from './OauthRowParts';
import { RowNote } from './RowNote';
import { brokerageForUrl } from '../brokerages';
import { McpWorkspaceRow } from './McpWorkspaceRow';
import { PluginSuppressedBadge } from './PluginBadges';
import { ServerDetail, type McpServerDetailData } from './ServerDetail';

/**
 * The Plugins → MCP tab, grouped by the package a row came from: one deck per
 * shipped bundle, `Your servers` (hand-made rows), one deck per installed
 * plugin, then one deck per workspace. A builtin no bundle declares keeps the
 * `Platform servers` deck. An enabled user-tier row is inherited by EVERY
 * workspace of the user; a disabled row is an inert template. Remote (http)
 * servers carry the OAuth connect lifecycle — the vendor bearer never leaves
 * the host, so "Connect" here is all a sandbox needs for the server to work.
 *
 * This file owns the tab's composition and the workspace-tier writes; the row
 * bodies, the OAuth lifecycle and the bulk actions each live beside their own
 * reasoning.
 */

export function McpServers() {
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const { data: catalog, isLoading, error } = useMcpCatalog();
  const {
    data: builtinData,
    error: builtinError,
    refetch: refetchBuiltins,
  } = useBuiltinMcpServers();
  const builtinToggleMutation = useToggleBuiltinMcpServer();
  const { data: vault } = useUserVaultSecrets();
  const createMutation = useCreateMcpCatalogServer();
  const updateMutation = useUpdateMcpCatalogServer();
  const deleteMutation = useDeleteMcpCatalogServer();
  const toggleMutation = useToggleMcpCatalogServer();
  const importMutation = useImportMcpCatalogServers();
  const createSecretMutation = useCreateUserVaultSecret();
  const wsEnableMutation = useSetMcpServerEnabledInWorkspace();
  const adoptMutation = useAdoptMcpServerToWorkspace();
  const moveUpMutation = usePromoteMcpServerToTemplate();
  const { workspaces: wsOptions, nameById: wsNameById } = useWorkspaceOptions();

  const {
    modalOpen,
    importOpen,
    editing,
    submitError,
    togglingName,
    deletingName,
    openAdd,
    openEdit,
    closeModal,
    openImport,
    closeImport,
    submit,
    toggle,
    requestDelete,
    cancelDelete,
    confirmDelete,
  } = useMcpServerList<CatalogServer>({
    create: createMutation.mutateAsync,
    update: updateMutation.mutateAsync,
    toggle: toggleMutation.mutateAsync,
    remove: deleteMutation.mutateAsync,
    // Deleting a connector un-inherits it from every workspace, so the strip
    // confirms first — and stays up if the delete fails, to retry or cancel.
    confirmBeforeDelete: true,
    onSaveWarnings: (warnings) =>
      toast({ title: t('plugins.servers.warningTitle'), description: warnings.join('\n') }),
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
  });

  const oauth = useMcpOauthActions();
  // One observer for the whole list. The registry is static and identical for
  // every row, so asking per row bought nothing and cost an observer per server.
  const {
    data: brokerages,
    error: brokeragesError,
    refetch: refetchBrokerages,
  } = useBrokerages();
  // An error only speaks for the rows when there is nothing to fall back on. A
  // refetch that fails still leaves the answer the query already has, and this
  // registry is what the build ships, so that answer is as good as it was.
  const registryUnavailable = !brokerages && !!brokeragesError;
  const [movingName, setMovingName] = useState<string | null>(null);
  const [builtinTogglingName, setBuiltinTogglingName] = useState<string | null>(null);
  // Two busy identities for the same endpoint, because two different rows can
  // be the one the user touched: a user-tier deny checklist marks the user-tier
  // row (by name), a workspace row's own toggle marks that row (by workspace +
  // name). One key for both lights up an unrelated sibling whenever a
  // workspace row shadows an inherited name.
  const [denyBusyName, setDenyBusyName] = useState<string | null>(null);
  const [wsTogglingKey, setWsTogglingKey] = useState<string | null>(null);
  // Bulk delete already excludes plugin-owned rows and the rest of the actions
  // are enable/disable/scope, so no bulk run here can change plugin identity.
  const surface = usePluginListSurface({ invalidate: invalidateMcpFanout });
  const { selection } = surface;

  useAddIntent({ server: openAdd, import: openImport });

  const secretNames = (vault?.secrets ?? []).map((s) => s.name);
  const servers = catalog?.servers ?? [];
  const builtinServers = builtinData?.servers ?? [];
  const allWorkspaceServers = catalog?.workspace_servers ?? [];
  const maxServers = catalog?.max_servers ?? 0;

  const visibleBuiltins = builtinServers.filter(
    (s) =>
      matchesFilter(surface.filter, s.name, s.description, s.plugin_name) &&
      surface.matchesState(s.enabled, isPluginSuppressed(s)),
  );
  const visibleServers = servers.filter(
    (s) =>
      matchesFilter(surface.filter, s.name, s.description, s.plugin_name) &&
      surface.matchesState(
        !!s.enabled,
        isOauthBroken(s.oauth_status) || isPluginSuppressed(s),
      ),
  );
  const visibleWorkspaceServers = allWorkspaceServers.filter(
    (s) =>
      matchesFilter(surface.filter, s.name, s.description) &&
      surface.matchesState(s.enabled),
  );
  // Every row the tab can show, across every section — the one population the
  // "No matches" notice is allowed to be keyed on.
  const visibleTotal =
    visibleBuiltins.length + visibleServers.length + visibleWorkspaceServers.length;

  const ownServers = visibleServers.filter((s) => !s.plugin_name);
  // Builtins group by the bundle that declares them, the same way catalog
  // rows group by the plugin that installed them: one deck per package,
  // whether it arrived in the image or in a zip. What is left over is a
  // server an operator added to agent_config.yaml, which no package claims.
  const bundleSections = [
    ...groupBy(
      visibleBuiltins.filter((s) => s.plugin_name),
      (s) => s.plugin_name as string,
    ).entries(),
  ].sort(([a], [b]) => a.localeCompare(b));
  const unownedBuiltins = visibleBuiltins.filter((s) => !s.plugin_name);
  const pluginSections = [
    ...groupBy(
      visibleServers.filter((s) => s.plugin_name),
      (s) => s.plugin_name as string,
    ).entries(),
  ].sort(([a], [b]) => a.localeCompare(b));
  const workspaceSections = [
    ...groupBy(visibleWorkspaceServers, (s) => s.workspace_id).entries(),
  ].sort(([a], [b]) => (wsNameById.get(a) ?? '').localeCompare(wsNameById.get(b) ?? ''));

  // --- Detail overlay (?detail=server:NAME [&dws=wsid]) ---
  // Builtin names are reserved against catalog names, so a bare name lookup
  // is unambiguous; a `dws` selects the workspace-local row instead.
  const detail = useDetailParam<McpServerDetailData>(
    'server',
    (ref) => {
      if (ref.workspaceId) {
        const row = allWorkspaceServers.find(
          (s) => s.workspace_id === ref.workspaceId && s.name === ref.name,
        );
        return row ? { origin: 'workspace' as const, server: row } : null;
      }
      const builtin = builtinServers.find((s) => s.name === ref.name);
      if (builtin) return { origin: 'builtin' as const, server: builtin };
      const cat = servers.find((s) => s.name === ref.name);
      return cat ? { origin: 'user' as const, server: cat } : null;
    },
    !isLoading && catalog !== undefined && builtinData !== undefined,
  );
  const detailData = detail.target;

  // The visible lists, not the full ones: a bulk action reaches exactly the
  // rows on screen. See the selection-scope note in useBulkSelection.
  const bulk = useMcpBulkActions({
    builtins: visibleBuiltins,
    catalog: visibleServers,
    workspaceServers: visibleWorkspaceServers,
    surface,
    workspaces: wsOptions,
  });

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

  async function setWorkspaceEnabled(
    workspaceId: string,
    name: string,
    enabled: boolean,
  ) {
    try {
      await wsEnableMutation.mutateAsync({ workspaceId, name, enabled });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  /** The per-workspace deny checklist on a user-tier row (builtin or catalog). */
  async function handleSetWorkspaceDisabled(
    name: string,
    workspaceId: string,
    disabled: boolean,
  ) {
    setDenyBusyName(name);
    try {
      await setWorkspaceEnabled(workspaceId, name, !disabled);
    } finally {
      setDenyBusyName(null);
    }
  }

  /** A workspace row's own enabled toggle, from the row or its detail overlay. */
  async function handleSetWorkspaceRowEnabled(
    workspaceId: string,
    name: string,
    enabled: boolean,
  ) {
    // Keyed by row so one row's in-flight toggle doesn't lock its siblings.
    setWsTogglingKey(`${workspaceId}:${name}`);
    try {
      await setWorkspaceEnabled(workspaceId, name, enabled);
    } finally {
      setWsTogglingKey(null);
    }
  }

  async function handleAdopt(name: string, workspaceId: string) {
    setMovingName(name);
    try {
      await adoptMutation.mutateAsync({ workspaceId, name });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.scope.moveFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setMovingName(null);
    }
  }

  async function handleMoveUp(workspaceId: string, name: string) {
    setMovingName(name);
    try {
      await moveUpMutation.mutateAsync({ workspaceId, name, removeSource: true });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.scope.moveFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setMovingName(null);
    }
  }

  function renderCatalogRow(server: CatalogServer) {
    return (
      <McpCatalogRow
        key={server.name}
        server={server}
        // Resolved off the address rather than the row's identity: a brokerage
        // row is the user's to edit once it exists, so the vendor's constraints
        // follow wherever the URL still points. `undefined` is the registry
        // unanswered -- in flight, or asked and failed -- which is a different
        // thing from resolving to no vendor. Both have to read as unknown: an
        // empty registry says every row here is an ordinary server, and a row
        // that is actually a broker then loses the warning that costs the user a
        // connection elsewhere. Unknown holds the button; wrong spends something
        // on the user's behalf.
        vendor={brokerages ? brokerageForUrl(server.url, brokerages) : undefined}
        registryUnavailable={registryUnavailable}
        workspaces={wsOptions}
        selection={selection}
        connecting={oauth.connectingName === server.name}
        refreshing={oauth.refreshingName === server.name}
        toggling={togglingName === server.name}
        scopeBusy={movingName === server.name || denyBusyName === server.name}
        onOpen={() => detail.open(server.name)}
        onConnect={(vendor) => {
          // One strip at a time: a delete question already on screen belongs to
          // a different row and its Yes is not this one's.
          cancelDelete();
          oauth.connect({
            name: server.name,
            vendor,
            url: server.url ?? null,
            granted: server.remembered_capabilities ?? null,
          });
        }}
        onDisconnect={() => oauth.disconnect(server.name)}
        onRefreshSchemas={() => oauth.refreshSchemas(server.name)}
        onEdit={() => openEdit(server)}
        onRequestDelete={() => {
          oauth.cancelPending();
          requestDelete(server);
        }}
        onToggle={(enabled) => toggle(server, enabled)}
        onSetWorkspaceDisabled={(wsId, disabled) =>
          handleSetWorkspaceDisabled(server.name, wsId, disabled)
        }
        onMove={(toWorkspaceId) => handleAdopt(server.name, toWorkspaceId)}
      />
    );
  }

  function renderBuiltinDeck(
    id: string,
    title: string,
    rows: BuiltinMcpServer[],
    extras: { badge?: ReactNode; action?: ReactNode } = {},
  ) {
    return (
      <GroupDeck
        key={id}
        id={id}
        title={title}
        icon={Server}
        count={rows.length}
        enabledCount={rows.filter(isEffectivelyEnabled).length}
        badge={extras.badge}
        action={extras.action}
        forceExpanded={surface.forceExpanded}
        selection={selection}
        selectionKeys={rows.map((s) => `builtin:${s.name}`)}
      >
        <AnimatePresence initial={false}>
          {rows.map((server) => (
            <BuiltinMcpRow
              key={server.name}
              server={server}
              workspaces={wsOptions}
              busy={
                builtinTogglingName === server.name ||
                denyBusyName === server.name
              }
              selection={selection}
              onOpen={() => detail.open(server.name)}
              onToggle={(enabled) => handleToggleBuiltin(server.name, enabled)}
              onSetWorkspaceDisabled={(wsId, disabled) =>
                handleSetWorkspaceDisabled(server.name, wsId, disabled)
              }
            />
          ))}
        </AnimatePresence>
      </GroupDeck>
    );
  }

  /** Open the package's own card. The deck already knows which one it is, so
   *  the detail ref carries that name -- landing on the bare list instead
   *  leaves the reader to find it again, and there is usually more than one.
   *  The overlay open here belongs to this tab, so it goes; every other param
   *  travels. */
  function openPluginDetail(name: string) {
    const next = withDetail(searchParams, {
      kind: 'plugin',
      name,
      workspaceId: null,
    });
    next.set('tab', 'plugins');
    setSearchParams(next, { replace: true });
  }

  const openPluginButton = (name: string) => (
    <button
      type="button"
      title={t('plugins.groups.openPlugin')}
      aria-label={t('plugins.groups.openPlugin')}
      onClick={() => openPluginDetail(name)}
      className="p-1 rounded transition-colors hover:bg-foreground/10"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      <Blocks className="h-3.5 w-3.5" />
    </button>
  );

  return (
    <div className="flex flex-col gap-3">
      <ListControls
        filter={surface.filter}
        onFilterChange={surface.setFilter}
        stateFilter={surface.stateFilter}
        onStateFilterChange={surface.setStateFilter}
        showAttention
        selecting={selection.selecting}
        onStartSelect={selection.start}
        selectDisabled={
          servers.length === 0 &&
          builtinServers.length === 0 &&
          allWorkspaceServers.length === 0
        }
      />

      {surface.noMatches(visibleTotal) && (
        <ListEmpty>{t('plugins.filter.noMatches')}</ListEmpty>
      )}

      {/* Above every list rather than inside one, because the rows it holds are
          spread across all of them: a plugin-owned server connects through the
          same button as one the user typed. Held rather than let through, since
          a broker the page cannot recognise gets the plain Connect and, for a
          vendor whose consent screen this build cannot reach, a dead end. */}
      {registryUnavailable && (
        <RowNote icon={AlertTriangle} id={REGISTRY_NOTE_ID}>
          {t('plugins.oauth.registryUnavailableNote')}{' '}
          <button
            type="button"
            onClick={() => void refetchBrokerages()}
            className="underline underline-offset-2 hover:text-[var(--color-text-secondary)]"
          >
            {t('common.retry')}
          </button>
        </RowNote>
      )}

      {/* The shipped decks are the only thing this query feeds, so without a
          notice a failed load reads as "this build ships nothing" -- and the
          user's own section renders fine beside it, which makes the page look
          healthy. Same shape as the registry note above: say it, offer the
          retry, leave the rest of the tab alone. */}
      {!!builtinError && (
        <RowNote icon={AlertTriangle}>
          {t('plugins.mcp.builtinLoadFailed')}{' '}
          <button
            type="button"
            onClick={() => void refetchBuiltins()}
            className="underline underline-offset-2 hover:text-[var(--color-text-secondary)]"
          >
            {t('common.retry')}
          </button>
        </RowNote>
      )}

      {bundleSections.map(([bundleName, rows]) =>
        renderBuiltinDeck(`mcp:bundle:${bundleName}`, bundleName, rows, {
          badge: <PluginSuppressedBadge row={rows[0]} />,
          action: openPluginButton(bundleName),
        }),
      )}

      {unownedBuiltins.length > 0 &&
        renderBuiltinDeck(
          'mcp:platform',
          t('plugins.mcp.platform'),
          unownedBuiltins,
        )}

      {/* Filtered-empty drops the whole section: the notice above already says
          why, and a bare header over nothing reads as a glitch. Loading and
          error keep theirs, so the list doesn't vanish while still arriving. */}
      {(isLoading || !!error || surface.keepsSection(ownServers.length)) && (
        <>
          <ListHeader
            icon={Server}
            title={t('plugins.mcp.yours')}
            count={servers.length}
            max={maxServers}
          />

          <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('plugins.servers.inheritHint')}
          </p>

          {error ? (
            <ListError>
              {(error as { message?: string })?.message || t('mcp.list.loadFailed')}
            </ListError>
          ) : isLoading ? (
            <ListSkeleton />
          ) : ownServers.length === 0 ? (
            <EmptyState
              // The count above is every catalog row, because that is what the
              // per-account cap counts; the list below is only the ones the
              // user made by hand. Install a plugin that ships a server and
              // the two disagree, so "4 / 50" ends up sitting directly over
              // "No servers yet" — which reads as the page having lost them.
              message={
                servers.length > 0
                  ? t('plugins.servers.emptyPluginOnly')
                  : t('plugins.servers.empty')
              }
              action={
                <HeaderButton variant="primary" icon={Plus} onClick={openAdd}>
                  {t('mcp.list.addServer')}
                </HeaderButton>
              }
            />
          ) : (
            <div className="flex flex-col [&>*+*]:mt-1.5">
              <AnimatePresence initial={false}>
                {ownServers.map(renderCatalogRow)}
              </AnimatePresence>
            </div>
          )}
        </>
      )}

      {pluginSections.map(([pluginName, rows]) => (
        <GroupDeck
          key={pluginName}
          id={`mcp:plugin:${pluginName}`}
          title={pluginName}
          icon={Blocks}
          count={rows.length}
          enabledCount={rows.filter(isEffectivelyEnabled).length}
          badge={<PluginSuppressedBadge row={rows[0]} />}
          action={openPluginButton(pluginName)}
          forceExpanded={surface.forceExpanded}
          selection={selection}
          selectionKeys={rows.map((s) => `catalog:${s.name}`)}
        >
          <AnimatePresence initial={false}>
            {rows.map(renderCatalogRow)}
          </AnimatePresence>
        </GroupDeck>
      ))}

      {workspaceSections.map(([wsId, wsServers]) => (
        <GroupDeck
          key={wsId}
          id={`mcp:ws:${wsId}`}
          title={t('plugins.scope.inWorkspace', {
            name: wsNameById.get(wsId) ?? t('plugins.scope.unknownWorkspace'),
          })}
          icon={Folder}
          count={wsServers.length}
          enabledCount={wsServers.filter((s) => s.enabled).length}
          forceExpanded={surface.forceExpanded}
          selection={selection}
          selectionKeys={wsServers.map((s) => `ws:${wsId}:${s.name}`)}
        >
          <AnimatePresence initial={false}>
            {wsServers.map((server) => (
              <McpWorkspaceRow
                key={`${wsId}:${server.name}`}
                server={server}
                workspaceId={wsId}
                workspaces={wsOptions}
                selection={selection}
                moving={movingName === server.name}
                toggling={wsTogglingKey === `${wsId}:${server.name}`}
                onOpen={() => detail.open(server.name, wsId)}
                onMoveUp={() => handleMoveUp(wsId, server.name)}
                onSetEnabled={(enabled) =>
                  handleSetWorkspaceRowEnabled(wsId, server.name, enabled)
                }
              />
            ))}
          </AnimatePresence>
        </GroupDeck>
      ))}

      {/* What a brokerage connection may do, and the vendor's own terms, asked
          here as well as on the Brokerages tab: the same row is reachable from
          both, and neither the consent nor a connect that drops the account's
          other AI connection may be one click quieter for having been reached
          through the MCP list. The hook holds the request until this is
          answered, so nothing has happened yet either way. */}
      {oauth.pendingConfirm && (
        <BrokerageConsentDialog
          key={oauth.pendingConfirm.name}
          vendor={oauth.pendingConfirm.vendor}
          name={oauth.pendingConfirm.name}
          granted={oauth.pendingConfirm.granted}
          pending={oauth.connectingName === oauth.pendingConfirm.name}
          onConfirm={oauth.confirmPending}
          onCancel={oauth.cancelPending}
        />
      )}

      {deletingName && (
        <ConfirmStrip
          message={t('plugins.servers.deleteConfirm', { server: deletingName })}
          confirmLabel={deleteMutation.isPending ? t('common.loading') : t('plugins.servers.deleteConfirmYes')}
          cancelLabel={t('plugins.servers.deleteConfirmNo')}
          pending={deleteMutation.isPending}
          onConfirm={confirmDelete}
          onCancel={cancelDelete}
        />
      )}

      {selection.selecting && (
        <BulkActionBar
          count={bulk.count}
          actions={bulk.actions}
          scope={bulk.scope}
          progress={surface.progress}
          onExit={selection.exit}
        />
      )}

      {modalOpen && (
        <McpServerModal
          secretNames={secretNames}
          initial={editing}
          allowDiscover={false}
          onClose={closeModal}
          onSubmit={submit}
          createSecret={createSecretMutation.mutateAsync}
          saving={createMutation.isPending || updateMutation.isPending}
          submitError={submitError}
        />
      )}

      {importOpen && (
        <McpImportModal
          onClose={closeImport}
          onImport={(payload) => importMutation.mutateAsync(payload)}
          onImported={(createdNames) => {
            if (createdNames.length > 0) {
              toast({
                title: t('plugins.import.disabledNudgeTitle'),
                description: t('plugins.import.disabledNudgeDesc'),
              });
            }
          }}
        />
      )}

      <AnimatePresence>
      {detailData && (
        <ServerDetail
          key={`${detailData.origin}:${detailData.server.name}`}
          data={detailData}
          onClose={detail.close}
          workspaceName={
            detailData.origin === 'workspace'
              ? wsNameById.get(detailData.server.workspace_id)
              : undefined
          }
          toggling={
            detailData.origin === 'builtin'
              ? builtinTogglingName === detailData.server.name
              : detailData.origin === 'user'
                ? togglingName === detailData.server.name
                : wsTogglingKey ===
                  `${detailData.server.workspace_id}:${detailData.server.name}`
          }
          onToggle={(enabled) => {
            if (detailData.origin === 'builtin') {
              handleToggleBuiltin(detailData.server.name, enabled);
            } else if (detailData.origin === 'user') {
              toggle(detailData.server, enabled);
            } else {
              handleSetWorkspaceRowEnabled(
                detailData.server.workspace_id,
                detailData.server.name,
                enabled,
              );
            }
          }}
        />
      )}
      </AnimatePresence>
    </div>
  );
}
