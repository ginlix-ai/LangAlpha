import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Link2, Link2Off, Pencil, RefreshCw, Server, Trash2 } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/use-toast';
import {
  useMcpCatalog,
  useCreateMcpCatalogServer,
  useUpdateMcpCatalogServer,
  useDeleteMcpCatalogServer,
  useToggleMcpCatalogServer,
  useImportMcpCatalogServers,
  useDisconnectMcpOauth,
  useRefreshMcpOauthSchemas,
  useSetMcpServerEnabledInWorkspace,
  useAdoptMcpServerToWorkspace,
  usePromoteMcpServerToTemplate,
} from '@/hooks/useMcpServers';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { useUserVaultSecrets, useCreateUserVaultSecret } from '@/hooks/useUserVault';
import { McpServerModal } from '@/pages/ChatAgent/components/mcp/McpServerModal';
import { McpImportModal } from '@/pages/ChatAgent/components/mcp/McpImportModal';
import { McpOauthPill } from '@/pages/ChatAgent/components/mcp/McpStatusPill';
import {
  canDisconnectOauth,
  isPluginOwned,
  isPluginSuppressed,
  needsOauthConnect,
} from '@/pages/ChatAgent/components/mcp/mcpState';
import { useMcpServerList } from '@/pages/ChatAgent/components/mcp/useMcpServerList';
import { BuiltinMcpSection } from './BuiltinMcpSection';
import { ScopeControl } from './ScopeControl';
import type { ScopeWorkspace } from './ScopeControl';
import {
  ConfirmStrip,
  EnabledToggle,
  KebabTrigger,
  ListEmpty,
  ListError,
  ListSkeleton,
  ListToolbar,
  SectionHeader,
  ServerNameLine,
  ServerRowShell,
  TagBadge,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import {
  formatApiErrorDetail,
  startMcpOauth,
  type CatalogServer,
  type WorkspaceScopedMcpServer,
} from '@/pages/ChatAgent/utils/api';

/**
 * The Plugins → MCP tab, `Your servers` section: the user-level MCP server list. An enabled row
 * is inherited by EVERY workspace of the user; a disabled row is an inert
 * template. Remote (http) servers carry the OAuth connect lifecycle — the
 * vendor bearer never leaves the host, so "Connect" here is all a sandbox
 * needs for the server to work.
 *
 * Row anatomy mirrors the workspace MCP tab (`McpServerRow`): identity line
 * (icon + name + transport badge), then the status line (OAuth pill + scope
 * text), then the description — same primitives, same rhythm. The list
 * mechanics (modals, toggle, delete) are the shared `useMcpServerList`; what
 * lives here is the OAuth lifecycle, which the workspace tab has no version of.
 */

export function McpServers() {
  const { t } = useTranslation();
  const { data: catalog, isLoading, error } = useMcpCatalog();
  const { data: vault } = useUserVaultSecrets();
  const createMutation = useCreateMcpCatalogServer();
  const updateMutation = useUpdateMcpCatalogServer();
  const deleteMutation = useDeleteMcpCatalogServer();
  const toggleMutation = useToggleMcpCatalogServer();
  const importMutation = useImportMcpCatalogServers();
  const disconnectMutation = useDisconnectMcpOauth();
  const refreshMutation = useRefreshMcpOauthSchemas();
  const createSecretMutation = useCreateUserVaultSecret();
  const wsEnableMutation = useSetMcpServerEnabledInWorkspace();
  const adoptMutation = useAdoptMcpServerToWorkspace();
  const moveUpMutation = usePromoteMcpServerToTemplate();
  const { data: wsData } = useWorkspaces({ limit: 100 });

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

  const [connectingName, setConnectingName] = useState<string | null>(null);
  const [refreshingName, setRefreshingName] = useState<string | null>(null);
  const [movingName, setMovingName] = useState<string | null>(null);
  const [wsTogglingKey, setWsTogglingKey] = useState<string | null>(null);

  const secretNames = (vault?.secrets ?? []).map((s) => s.name);
  const servers = catalog?.servers ?? [];
  const maxServers = catalog?.max_servers ?? 0;
  const atCap = maxServers > 0 && servers.length >= maxServers;

  const workspaces = (
    (wsData as { workspaces?: { workspace_id: string; name?: string }[] })
      ?.workspaces ?? []
  );
  const wsOptions: ScopeWorkspace[] = workspaces.map((w) => ({
    id: w.workspace_id,
    name: w.name || t('plugins.scope.unknownWorkspace'),
  }));
  const wsNameById = new Map(wsOptions.map((w) => [w.id, w.name]));

  const workspaceServers = catalog?.workspace_servers ?? [];
  const byWorkspace = new Map<string, WorkspaceScopedMcpServer[]>();
  for (const s of workspaceServers) {
    byWorkspace.set(s.workspace_id, [...(byWorkspace.get(s.workspace_id) ?? []), s]);
  }
  const workspaceSections = [...byWorkspace.entries()].sort(([a], [b]) =>
    (wsNameById.get(a) ?? '').localeCompare(wsNameById.get(b) ?? ''),
  );

  async function handleSetWorkspaceDisabled(
    name: string,
    workspaceId: string,
    disabled: boolean,
  ) {
    // Keyed by row so one row's in-flight toggle doesn't lock its siblings.
    setWsTogglingKey(`${workspaceId}:${name}`);
    try {
      await wsEnableMutation.mutateAsync({ workspaceId, name, enabled: !disabled });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
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

  async function handleConnect(name: string) {
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

  async function handleDisconnect(name: string) {
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

  async function handleRefreshSchemas(name: string) {
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

  return (
    <div className="flex flex-col gap-3">
      <BuiltinMcpSection />

      <ListToolbar
        icon={Server}
        title={t('plugins.mcp.yours')}
        count={servers.length}
        max={maxServers}
        atCap={atCap}
        onImport={openImport}
        onAdd={openAdd}
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
      ) : servers.length === 0 ? (
        <ListEmpty>{t('plugins.servers.empty')}</ListEmpty>
      ) : (
        <div className="flex flex-col gap-1.5">
          <AnimatePresence initial={false}>
            {servers.map((server) => {
              const oauthEligible = server.transport === 'http';
              const status = server.oauth_status ?? null;
              return (
                <ServerRowShell
                  key={server.name}
                  testid={`server-row-${server.name}`}
                  main={
                    <>
                      <ServerNameLine icon={Server} name={server.name}>
                        <TagBadge>{server.transport}</TagBadge>
                        {server.plugin_name && (
                          <TagBadge
                            soft
                            title={t('plugins.component.fromPlugin', {
                              plugin: server.plugin_name,
                            })}
                          >
                            {server.plugin_name}
                          </TagBadge>
                        )}
                      </ServerNameLine>

                      {/* Status line: OAuth pill + tool count + inheritance scope */}
                      <div className="flex items-center gap-2 flex-wrap">
                        {status && <McpOauthPill status={status} />}
                        {status === 'connected' && typeof server.tool_count === 'number' && server.tool_count > 0 && (
                          <span
                            className="text-[0.6875rem]"
                            style={{ color: 'var(--color-text-tertiary)' }}
                          >
                            {t('mcp.row.toolCount', { count: server.tool_count })}
                          </span>
                        )}
                        <span
                          className="text-[0.6875rem]"
                          style={{ color: server.enabled ? 'var(--color-text-secondary)' : 'var(--color-text-tertiary)' }}
                        >
                          {server.enabled
                            ? t('plugins.servers.enabledState')
                            : t('plugins.servers.disabledState')}
                        </span>
                        {isPluginSuppressed(server) && (
                          <span
                            className="text-[0.6875rem]"
                            style={{ color: 'var(--color-text-tertiary)' }}
                          >
                            {t('plugins.component.suppressed', {
                              plugin: server.plugin_name,
                            })}
                          </span>
                        )}
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
                          onClick={() => handleConnect(server.name)}
                          disabled={connectingName === server.name}
                          className="inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md transition-colors disabled:opacity-50"
                          style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                        >
                          {connectingName === server.name
                            ? <Loader size={12} className="text-current" />
                            : <Link2 className="h-3 w-3" />}
                          {status ? t('plugins.oauth.reconnect') : t('plugins.oauth.connect')}
                        </button>
                      )}

                      <ScopeControl
                        workspaces={wsOptions}
                        scopeWorkspaceId={null}
                        disabledWorkspaceIds={server.disabled_workspace_ids ?? []}
                        checklistLocked={!server.enabled}
                        busy={
                          movingName === server.name ||
                          // Names cannot contain ':', so the suffix match is
                          // exact per row.
                          !!wsTogglingKey?.endsWith(`:${server.name}`)
                        }
                        moveBlockedReason={
                          // OAuth connections exist only at the user tier, so a
                          // connected server cannot move into a workspace. A
                          // plugin-owned row stays put too: moving it would
                          // orphan the plugin's ownership row.
                          isPluginOwned(server)
                            ? t('plugins.scope.movePluginBlocked', {
                                plugin: server.plugin_name,
                              })
                            : status && status !== 'revoked'
                              ? t('plugins.scope.moveOauthBlocked')
                              : null
                        }
                        onSetWorkspaceDisabled={(wsId, disabled) =>
                          handleSetWorkspaceDisabled(server.name, wsId, disabled)
                        }
                        onMove={(toWorkspaceId) => {
                          if (toWorkspaceId) handleAdopt(server.name, toWorkspaceId);
                        }}
                      />

                      {/* Enabled toggle — fans out to every workspace */}
                      <EnabledToggle
                        enabled={!!server.enabled}
                        name={server.name}
                        disabled={togglingName === server.name}
                        onToggle={() => toggle(server, !server.enabled)}
                      />

                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <KebabTrigger
                            busy={refreshingName === server.name}
                            aria-label={t('mcp.row.actionsAria', { name: server.name })}
                          />
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          {/* Editing a plugin-owned row detaches it from the
                              plugin, so the item says Customize and carries the
                              consequence in its tooltip. The save path is the
                              same PUT; the backend clears ownership and returns
                              the detach warning, surfaced by onSaveWarnings. */}
                          <DropdownMenuItem
                            onSelect={() => openEdit(server)}
                            title={
                              isPluginOwned(server)
                                ? t('plugins.component.customizeHint', {
                                    plugin: server.plugin_name,
                                  })
                                : undefined
                            }
                          >
                            <Pencil className="h-3.5 w-3.5 mr-2" />
                            {isPluginOwned(server)
                              ? t('plugins.component.customize')
                              : t('mcp.row.edit')}
                          </DropdownMenuItem>
                          {oauthEligible && status === 'connected' && (
                            <DropdownMenuItem onSelect={() => handleRefreshSchemas(server.name)}>
                              <RefreshCw className="h-3.5 w-3.5 mr-2" />
                              {t('plugins.oauth.refreshSchemas')}
                            </DropdownMenuItem>
                          )}
                          {oauthEligible && canDisconnectOauth(status) && (
                            <DropdownMenuItem onSelect={() => handleDisconnect(server.name)}>
                              <Link2Off className="h-3.5 w-3.5 mr-2" />
                              {t('plugins.oauth.disconnect')}
                            </DropdownMenuItem>
                          )}
                          <DropdownMenuItem onSelect={() => requestDelete(server)} variant="destructive">
                            <Trash2 className="h-3.5 w-3.5 mr-2" />
                            {t('mcp.row.delete')}
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </>
                  }
                />
              );
            })}
          </AnimatePresence>
        </div>
      )}

      {workspaceSections.map(([wsId, wsServers]) => (
        <div key={wsId} className="flex flex-col gap-1.5">
          <SectionHeader>
            {t('plugins.scope.inWorkspace', {
              name: wsNameById.get(wsId) ?? t('plugins.scope.unknownWorkspace'),
            })}
          </SectionHeader>
          <AnimatePresence initial={false}>
            {wsServers.map((server) => (
              <ServerRowShell
                key={`${wsId}:${server.name}`}
                testid={`ws-server-row-${server.name}`}
                main={
                  <>
                    <ServerNameLine icon={Server} name={server.name}>
                      <TagBadge>{server.transport}</TagBadge>
                      {server.shadows_inherited && (
                        <TagBadge soft title={t('mcp.row.overridesInheritedHint')}>
                          {t('mcp.row.overridesInherited')}
                        </TagBadge>
                      )}
                    </ServerNameLine>
                    {server.description && (
                      <p
                        className="text-[0.6875rem] line-clamp-2"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        {server.description}
                      </p>
                    )}
                  </>
                }
                actions={
                  <>
                    <ScopeControl
                      workspaces={wsOptions}
                      scopeWorkspaceId={wsId}
                      // No cross-workspace move endpoint for MCP servers: the
                      // only destination is the user tier.
                      allowWorkspaceTargets={false}
                      busy={movingName === server.name}
                      moveToAllBlockedReason={
                        // The promote endpoint 409s when the name already
                        // exists at the user tier, so don't advertise a move
                        // that is known to fail for a shadowing row.
                        server.shadows_inherited
                          ? t('plugins.scope.moveShadowBlocked')
                          : null
                      }
                      onMove={(toWorkspaceId) => {
                        if (toWorkspaceId === null) handleMoveUp(wsId, server.name);
                      }}
                    />
                    <EnabledToggle
                      enabled={server.enabled}
                      name={server.name}
                      disabled={wsTogglingKey === `${wsId}:${server.name}`}
                      onToggle={() =>
                        handleSetWorkspaceDisabled(server.name, wsId, server.enabled)
                      }
                    />
                  </>
                }
              />
            ))}
          </AnimatePresence>
        </div>
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
    </div>
  );
}
