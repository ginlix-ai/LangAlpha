import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Blocks, Folder, Plus, Server } from 'lucide-react';
import { toast } from '@/components/ui/use-toast';
import {
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
  type CatalogServer,
} from '@/pages/ChatAgent/utils/api';
import { groupBy, matchesFilter } from '../utils/groupOrigins';
import { isPluginSuppressed } from '../utils/provenance';
import { withDetail } from '../utils/detailParam';
import { useAddIntent } from '../hooks/useAddIntent';
import { useDetailParam } from '../hooks/useDetailParam';
import { useMcpBulkActions } from '../hooks/useMcpBulkActions';
import { useMcpOauthActions } from '../hooks/useMcpOauthActions';
import { usePluginListSurface } from '../hooks/usePluginListSurface';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { BuiltinMcpSection } from './BuiltinMcpSection';
import { BulkActionBar } from './BulkActionBar';
import { EmptyState } from './EmptyState';
import { GroupDeck } from './GroupDeck';
import { ListControls } from './ListControls';
import { McpCatalogRow } from './McpCatalogRow';
import { McpWorkspaceRow } from './McpWorkspaceRow';
import { PluginSuppressedBadge } from './PluginBadges';
import { ServerDetail, type ServerDetailData } from './ServerDetail';

/**
 * The Plugins → MCP tab, grouped by origin: the Platform deck (builtins),
 * `Your servers` (hand-made rows), one deck per plugin's servers, then one
 * deck per workspace. An enabled user-tier row is inherited by EVERY
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
  const { data: builtinData } = useBuiltinMcpServers();
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
      matchesFilter(surface.filter, s.name, s.description) &&
      surface.matchesState(s.enabled),
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
  const detail = useDetailParam<ServerDetailData>(
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
        workspaces={wsOptions}
        selection={selection}
        connecting={oauth.connectingName === server.name}
        refreshing={oauth.refreshingName === server.name}
        toggling={togglingName === server.name}
        scopeBusy={movingName === server.name || denyBusyName === server.name}
        onOpen={() => detail.open(server.name)}
        onConnect={() => oauth.connect(server.name)}
        onDisconnect={() => oauth.disconnect(server.name)}
        onRefreshSchemas={() => oauth.refreshSchemas(server.name)}
        onEdit={() => openEdit(server)}
        onRequestDelete={() => requestDelete(server)}
        onToggle={(enabled) => toggle(server, enabled)}
        onSetWorkspaceDisabled={(wsId, disabled) =>
          handleSetWorkspaceDisabled(server.name, wsId, disabled)
        }
        onMove={(toWorkspaceId) => handleAdopt(server.name, toWorkspaceId)}
      />
    );
  }

  /** Jump to the plugin's card. The overlay open here belongs to this tab, so
   *  it goes; every other param travels. */
  function openPluginsTab() {
    const next = withDetail(searchParams, null);
    next.set('tab', 'plugins');
    setSearchParams(next, { replace: true });
  }

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
        selectDisabled={servers.length === 0 && builtinServers.length === 0}
      />

      {surface.noMatches(visibleTotal) && (
        <ListEmpty>{t('plugins.filter.noMatches')}</ListEmpty>
      )}

      <BuiltinMcpSection
        servers={visibleBuiltins}
        workspaces={wsOptions}
        busyName={builtinTogglingName ?? denyBusyName}
        forceExpanded={surface.forceExpanded}
        selection={selection}
        onOpen={(server) => detail.open(server.name)}
        onToggle={handleToggleBuiltin}
        onSetWorkspaceDisabled={handleSetWorkspaceDisabled}
      />

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
              message={t('plugins.servers.empty')}
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
          enabledCount={rows.filter((s) => !!s.enabled).length}
          badge={<PluginSuppressedBadge row={rows[0]} />}
          action={
            <button
              type="button"
              title={t('plugins.groups.openPlugin')}
              aria-label={t('plugins.groups.openPlugin')}
              onClick={openPluginsTab}
              className="p-1 rounded transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <Blocks className="h-3.5 w-3.5" />
            </button>
          }
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
