import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import { AnimatePresence } from 'framer-motion';
import { Server, Blocks } from 'lucide-react';
import {
  useWorkspaceMcpServers,
  useAddWorkspaceMcpServer,
  useUpdateWorkspaceMcpServer,
  useToggleWorkspaceMcpServer,
  useDeleteWorkspaceMcpServer,
  useDiscoverWorkspaceMcpServer,
  useImportWorkspaceMcpServers,
  usePromoteMcpServerToTemplate,
  useMcpCatalog,
  useDelayedFalse,
} from '@/hooks/useMcpServers';
import {
  useWorkspaceVaultSecrets,
  useCreateWorkspaceVaultSecret,
} from '@/hooks/useWorkspaceVault';
import { toast } from '@/components/ui/use-toast';
import { formatApiErrorDetail, type EffectiveServer, type McpServerInput } from '../../utils/api';
import { McpServerRow } from './McpServerRow';
import { McpServerModal } from './McpServerModal';
import { McpImportModal } from './McpImportModal';
import {
  ConfirmStrip,
  HeaderButton,
  ListEmpty,
  ListError,
  ListSkeleton,
  ListToolbar,
} from './McpPrimitives';
import { needsDiscoveryProbe } from './mcpState';
import { useMcpServerList } from './useMcpServerList';

/**
 * The "MCP" tab in the workspace settings panel — the workspace-scoped view.
 * User-level servers (and their OAuth lifecycle) are managed on /plugins;
 * inherited rows render here with their per-workspace enable toggle (writing
 * the workspace tombstone) plus a "Manage in Plugins" deep link.
 *
 * The list mechanics (modals, toggle, delete) are the shared
 * `useMcpServerList`. Three UX guarantees are this component's own:
 *  - **Live discovery progress.** A freshly-added (or any `pending`) workspace
 *    server doesn't sit on a dead "Pending" pill: when the sandbox is running we
 *    auto-run the synchronous discovery probe (`runDiscover`), so the row shows
 *    "Verifying…" → resolves to Connected (N tools) / Error / Needs secret. Each
 *    pending name is probed at most once per mount (the backend debounces too).
 *    Saving a server also kicks a background warm, so a stopped workspace shows
 *    "Starting workspace…" and the row resolves once it's up — verify happens
 *    regardless of whether the sandbox was already running.
 *  - **Honest apply state.** The backend bumps a `config_version` on every
 *    mutation and applies it to the running agent in the background; the GET
 *    response reports the session's `applied_config_version`. We derive `synced`
 *    (applied ≥ saved) from that — a version-accurate signal that replaces the
 *    old 30s timer guess. While not yet applied, the row's lifecycle shows
 *    "Applying to agent…"; once caught up it reads "Ready". We poll while
 *    anything is still settling (see `useWorkspaceMcpServers`).
 *  - **Stable order.** Display order is frozen on first load (`orderRef`): new
 *    servers append, removed ones drop out, but toggling never reorders a row —
 *    it restyles in place. Order re-sorts only on the next open (remount). This
 *    kills the "row teleports to the bottom when you switch it off" jank.
 *
 * `onOpenVaultTab` deep-links to the Vault tab (optionally prefilling a secret
 * name) for the needs_secret "Set up NAME" affordance.
 */

interface McpTabProps {
  workspaceId: string;
  /** Deep-link into the Vault tab, optionally with a prefilled secret name. */
  onOpenVaultTab?: (prefillSecretName?: string) => void;
}

export function McpTab({ workspaceId, onOpenVaultTab }: McpTabProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  const { data, isLoading, error } = useWorkspaceMcpServers(workspaceId);
  const addMutation = useAddWorkspaceMcpServer(workspaceId);
  const updateMutation = useUpdateWorkspaceMcpServer(workspaceId);
  const toggleMutation = useToggleWorkspaceMcpServer(workspaceId);
  const deleteMutation = useDeleteWorkspaceMcpServer(workspaceId);
  const discoverMutation = useDiscoverWorkspaceMcpServer(workspaceId);
  const importMutation = useImportWorkspaceMcpServers(workspaceId);
  const promoteMutation = usePromoteMcpServerToTemplate();

  // Template names drive the promote flow: an existing name needs an overwrite
  // confirm before clobbering. Cheap (60s staleTime), often already warm.
  const { data: catalogData } = useMcpCatalog();
  const templateNames = React.useMemo(
    () => new Set((catalogData?.servers ?? []).map((t) => t.name)),
    [catalogData],
  );

  // Vault secret names for the picker. The create mutation invalidates the
  // vault query, so a secret made inline in the modal (or auto-extracted by an
  // import) shows up here without anything to refetch by hand.
  const { data: vaultSecrets } = useWorkspaceVaultSecrets(workspaceId);
  const createSecretMutation = useCreateWorkspaceVaultSecret(workspaceId);
  const secretNames = useMemo(
    () => (vaultSecrets ?? []).map((s) => s.name),
    [vaultSecrets],
  );

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
  } = useMcpServerList<EffectiveServer>({
    create: addMutation.mutateAsync,
    update: updateMutation.mutateAsync,
    toggle: toggleMutation.mutateAsync,
    remove: deleteMutation.mutateAsync,
    onSaveWarnings: (warnings) =>
      toast({ title: t('mcp.tab.savedWithWarnings'), description: warnings.join('\n') }),
  });

  // Set when "Save as template" hits an existing template name → confirm overwrite.
  const [promoteConfirm, setPromoteConfirm] = useState<string | null>(null);

  // Memoized so the `?? []` fallback doesn't allocate a fresh array each render
  // (which would re-fire the order memo + auto-discover effect needlessly).
  const servers = useMemo(() => data?.servers ?? [], [data]);
  const sandboxRunning = data?.sandbox_running ?? false;
  // The sandbox is coming up (a proactive apply / workspace entry kicked a warm).
  // Drives the "Starting workspace…" copy + lets rows show in-progress instead
  // of "stopped" through the gap.
  const sandboxWarming = data?.sandbox_warming ?? false;
  const maxServers = data?.max_servers ?? 20;
  const workspaceCount = servers.filter((s) => s.origin === 'workspace').length;
  const atCap = workspaceCount >= maxServers;

  // Apply axis: the running session has loaded the saved config when its applied
  // version has caught up to the workspace's config version. Version-accurate —
  // replaces the old 30s "not synced" timer with the real apply state. Only
  // meaningful while the sandbox is running (nothing is "live" when it's down).
  const appliedVersion = data?.applied_config_version ?? null;
  const configVersion = data?.config_version ?? 0;
  const syncedNow = sandboxRunning && appliedVersion !== null && appliedVersion >= configVersion;
  // Anti-flicker: every toggle/add/edit bumps the workspace-wide config_version,
  // so the apply axis dips out-of-sync for the frame until the background apply
  // lands — which would flash "Applying to agent…" on EVERY connected row the
  // instant you toggle one (and churn the toggled row). Hold the synced state
  // across a fast apply (≈ one poll cycle); a genuinely lagging apply still shows.
  const synced = useDelayedFalse(syncedNow, 2600);

  // Frozen display order. The backend re-sorts disabled workspace servers to the
  // bottom, so a naive render makes a row teleport the instant you toggle it
  // off. We pin each name to the position it first appeared in this mount; new
  // servers append, removed ones drop, but a toggle only restyles in place. The
  // order re-sorts on the next open (remount resets the ref).
  const orderRef = useRef<string[]>([]);
  const orderedServers = useMemo(() => {
    const byName = new Map(servers.map((s) => [s.name, s]));
    const kept = orderRef.current.filter((n) => byName.has(n));
    const keptSet = new Set(kept);
    const appended = servers.map((s) => s.name).filter((n) => !keptSet.has(n));
    const order = [...kept, ...appended];
    orderRef.current = order;
    return order.map((n) => byName.get(n)!);
  }, [servers]);

  // Names with a discovery probe currently in flight → row shows "Checking…".
  const [checkingNames, setCheckingNames] = useState<Set<string>>(new Set());
  // Pending names we've already auto-probed this mount (probe once, not on every
  // refetch). Reset when the workspace changes (panel stays mounted across a
  // workspace switch) or on unmount.
  const autoCheckedRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    autoCheckedRef.current = new Set();
    setCheckingNames(new Set());
  }, [workspaceId]);

  const discoverAsync = discoverMutation.mutateAsync;
  const runDiscover = useCallback(
    async (name: string) => {
      setCheckingNames((prev) => new Set(prev).add(name));
      try {
        await discoverAsync(name);
      } catch {
        // A probe that reached the backend reports its own failure as the row's
        // status on the next refetch. A transport-level throw leaves the row on
        // 'pending'; the workspace poll's verify-stall cap (useMcpServers) stops
        // it rather than spinning. Either way, no toast for a silent inline probe.
      } finally {
        setCheckingNames((prev) => {
          const next = new Set(prev);
          next.delete(name);
          return next;
        });
      }
    },
    [discoverAsync],
  );

  // Auto-resolve pending servers: instead of leaving a freshly-added server on a
  // static "Pending", probe it once so the user sees Checking → Connected/Error.
  // Only when the sandbox is running (discovery needs it) and only for the rows
  // `needsDiscoveryProbe` admits — the same gate the list query polls on, so a
  // row can't be polled-for-but-never-probed. The backend's 15s debounce backs
  // up the mount guard.
  useEffect(() => {
    if (!sandboxRunning) return;
    for (const s of servers) {
      if (needsDiscoveryProbe(s) && !autoCheckedRef.current.has(s.name)) {
        autoCheckedRef.current.add(s.name);
        void runDiscover(s.name);
      }
    }
  }, [servers, sandboxRunning, runDiscover]);

  const handleDiscoverRow = useCallback(
    (server: EffectiveServer) => runDiscover(server.name),
    [runDiscover],
  );

  const handleSetupSecret = useCallback(
    (name: string) => onOpenVaultTab?.(name),
    [onOpenVaultTab],
  );

  const promoteAsync = promoteMutation.mutateAsync;
  const doPromote = useCallback(
    async (name: string, overwrite: boolean) => {
      try {
        await promoteAsync({ workspaceId, name, overwrite });
        toast({
          title: overwrite ? t('mcp.tab.promoteUpdatedTitle') : t('mcp.tab.promotedTitle'),
          description: t('mcp.tab.promotedDesc', { name }),
        });
      } catch (err) {
        toast({
          variant: 'destructive',
          title: t('mcp.tab.promoteFailed'),
          description: formatApiErrorDetail(err),
        });
      }
    },
    [promoteAsync, workspaceId, t],
  );

  const handlePromote = useCallback(
    (server: EffectiveServer) => {
      // Existing template → confirm overwrite; new name → promote straight away.
      if (templateNames.has(server.name)) {
        setPromoteConfirm(server.name);
      } else {
        void doPromote(server.name, false);
      }
    },
    [templateNames, doPromote],
  );

  const handleManageInPlugins = useCallback(() => {
    navigate('/plugins?tab=mcp');
  }, [navigate]);

  async function handleDiscoverFromModal(body: McpServerInput) {
    // "Test saved config" is offered only when editing an existing row (the
    // modal gates it on isEdit), so we probe the PERSISTED server by name. The
    // name field is locked on edit, so body.name is the saved server; unsaved
    // edits in the form aren't tested until they're saved (the button label says
    // as much). Discovery has no ad-hoc-definition endpoint by design.
    return discoverMutation.mutateAsync(body.name);
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-3">
          <ListToolbar
            icon={Server}
            title={t('mcp.list.title')}
            count={workspaceCount}
            max={maxServers}
            atCap={atCap}
            onImport={openImport}
            onAdd={openAdd}
          >
            <HeaderButton variant="ghost" icon={Blocks} onClick={handleManageInPlugins} title={t('mcp.tab.pluginsHint')}>
              {t('mcp.tab.plugins')}
            </HeaderButton>
          </ListToolbar>

          {!sandboxRunning && sandboxWarming && (
            <div className="text-[0.6875rem] p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}>
              {t('mcp.tab.startingWorkspace')}
            </div>
          )}

          {!sandboxRunning && !sandboxWarming && (
            <div className="text-[0.6875rem] p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}>
              {t('mcp.tab.workspaceStopped')}
            </div>
          )}

          {promoteConfirm && (
            <ConfirmStrip
              message={
                <>
                  {t('mcp.tab.promoteExistsBefore')}
                  <span className="font-medium">{promoteConfirm}</span>
                  {t('mcp.tab.promoteExistsAfter')}
                </>
              }
              confirmLabel={t('mcp.tab.overwrite')}
              confirmVariant="primary"
              cancelLabel={t('common.cancel')}
              pending={promoteMutation.isPending}
              onConfirm={async () => {
                const name = promoteConfirm;
                setPromoteConfirm(null);
                await doPromote(name, true);
              }}
              onCancel={() => setPromoteConfirm(null)}
            />
          )}

          {error ? (
            <ListError>
              {(error as { message?: string })?.message || t('mcp.list.loadFailed')}
            </ListError>
          ) : isLoading ? (
            <ListSkeleton />
          ) : servers.length === 0 ? (
            <ListEmpty>{t('mcp.list.empty')}</ListEmpty>
          ) : (
            <div className="flex flex-col [&>*+*]:mt-1.5">
              <AnimatePresence initial={false}>
                {orderedServers.map((server) => (
                  <McpServerRow
                    key={server.name}
                    server={server}
                    toggling={togglingName === server.name}
                    deleting={deletingName === server.name}
                    checking={checkingNames.has(server.name)}
                    synced={synced}
                    sandboxRunning={sandboxRunning}
                    sandboxWarming={sandboxWarming}
                    onToggle={toggle}
                    onEdit={openEdit}
                    onDiscover={handleDiscoverRow}
                    onDelete={requestDelete}
                    onPromoteToTemplate={server.origin === 'workspace' ? handlePromote : undefined}
                    onSetupSecret={handleSetupSecret}
                    onManageInPlugins={server.origin === 'user' ? handleManageInPlugins : undefined}
                  />
                ))}
              </AnimatePresence>
            </div>
          )}
      </div>

      {modalOpen && (
        <McpServerModal
          secretNames={secretNames}
          initial={editing}
          allowDiscover={!!editing && sandboxRunning}
          onClose={closeModal}
          onSubmit={submit}
          onDiscover={editing ? handleDiscoverFromModal : undefined}
          createSecret={createSecretMutation.mutateAsync}
          saving={addMutation.isPending || updateMutation.isPending}
          submitError={submitError}
        />
      )}

      {importOpen && (
        <McpImportModal
          onClose={closeImport}
          onImport={(payload) => importMutation.mutateAsync(payload)}
        />
      )}

    </div>
  );
}
