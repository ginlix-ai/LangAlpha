import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { AnimatePresence } from '@/lib/framer';
import { Server, Blocks } from 'lucide-react';
import {
  useMcpCatalog,
  useWorkspaceMcpServers,
  useAddWorkspaceMcpServer,
  useUpdateWorkspaceMcpServer,
  useToggleWorkspaceMcpServer,
  useDiscoverWorkspaceMcpServer,
  useImportWorkspaceMcpServers,
  useDelayedFalse,
} from '@/hooks/useMcpServers';
import { toast } from '@/components/ui/use-toast';
import { secretSetupHref } from '@/pages/Plugins/utils/secretParam';
import { connectRequestFor } from '@/pages/Plugins/hooks/useMcpOauthActions';
import { probeMcpServer, type EffectiveServer, type McpServerInput } from '../../utils/api';
import { McpServerRow } from './McpServerRow';
import { McpServerModal } from './McpServerModal';
import { McpImportModal } from './McpImportModal';
import {
  HeaderButton,
  ListEmpty,
  ListSkeleton,
  ListToolbar,
} from '@/components/mcp/McpPrimitives';
import { LoadFailed } from '@/pages/Plugins/components/LoadFailed';
import { needsDiscoveryProbe } from './mcpState';
import { useMcpServerList } from './useMcpServerList';
import { toastConnectorAdded } from './mcpAddedToast';

/**
 * The "MCP" tab in the workspace settings panel: which of the account's
 * servers this workspace uses. Servers live on the account and are selected
 * per workspace, so "Add server" creates an account server that is on here
 * only, an edit changes the account server everywhere it is on, and the
 * toggle is this workspace's on/off. Removal and the OAuth lifecycle belong
 * to /plugins, which account rows link to, with one exception: a server that
 * is saved, or switched on, only by its sign-in starts that sign-in from here,
 * since that is where the user asked for it. The vendor's round trip lands on
 * /plugins; Back from the vendor's page lands on this workspace's page, where
 * the app shell explains a sign-in that never came back whether or not this
 * panel is open.
 *
 * The list mechanics (modals, toggle) are the shared `useMcpServerList`.
 * Three UX guarantees are this component's own:
 *  - **Live discovery progress.** A freshly-added (or any `pending`) server
 *    doesn't sit on a dead "Pending" pill: when the sandbox is running we
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
 */

interface McpTabProps {
  workspaceId: string;
}

export function McpTab({ workspaceId }: McpTabProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  const { data, isLoading, error, refetch } = useWorkspaceMcpServers(workspaceId);
  const addMutation = useAddWorkspaceMcpServer(workspaceId);
  const updateMutation = useUpdateWorkspaceMcpServer(workspaceId);
  const toggleMutation = useToggleWorkspaceMcpServer(workspaceId);
  const discoverMutation = useDiscoverWorkspaceMcpServer(workspaceId);
  const importMutation = useImportWorkspaceMcpServers(workspaceId);

  // The server cap counts every server on the account, including the ones
  // switched off account-wide that this workspace's list leaves out, so the
  // count comes from the catalog.
  const { data: catalog } = useMcpCatalog();
  const cap = catalog ? { used: catalog.servers.length, max: catalog.max_servers } : undefined;

  const { togglingName, openAdd, openEdit, openImport, toggle, modal, importer } =
    useMcpServerList<EffectiveServer>({
      create: addMutation.mutateAsync,
      update: updateMutation.mutateAsync,
      toggle: toggleMutation.mutateAsync,
      onSaveWarnings: (warnings) =>
        toast({ title: t('mcp.tab.savedWithWarnings'), description: warnings.join('\n') }),
      // No Open here: the new row lands in this same list, in view.
      onCreated: ({ name, toolCount }) => toastConnectorAdded({ name, toolCount, t }),
      // A sign-in lands as the same save would have: an add switched on here
      // only, an edit with the switch it already had.
      pendingFor: (server, editing) =>
        editing
          ? { kind: 'server_edit', name: editing.name, server, workspaceId, expectedUrl: editing.url }
          : { kind: 'workspace_server', workspaceId, server },
      // A switch refused for want of a sign-in offers the account row's, which
      // knows what the user last granted. Any other refusal is silent: the
      // switch is optimistic and rolls back.
      connectFor: (server) =>
        connectRequestFor(catalog?.servers.find((s) => s.name === server.name) ?? server),
    });

  // Memoized so the `?? []` fallback doesn't allocate a fresh array each render
  // (which would re-fire the order memo + auto-discover effect needlessly).
  const servers = useMemo(() => data?.servers ?? [], [data]);
  const sandboxRunning = data?.sandbox_running ?? false;
  // The sandbox is coming up (a proactive apply / workspace entry kicked a warm).
  // Drives the "Starting workspace…" copy + lets rows show in-progress instead
  // of "stopped" through the gap.
  const sandboxWarming = data?.sandbox_warming ?? false;

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

  // Frozen display order. The backend re-sorts disabled servers to the bottom,
  // so a naive render makes a row teleport the instant you toggle it off. We
  // pin each name to the position it first appeared in this mount; new servers
  // append, removed ones drop, but a toggle only restyles in place. The order
  // re-sorts on the next open (remount resets the ref).
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
    (name: string) => navigate(secretSetupHref(name)),
    [navigate],
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
            count={servers.length}
            cap={cap}
            onImport={openImport}
            onAdd={openAdd}
          >
            <HeaderButton variant="ghost" icon={Blocks} onClick={handleManageInPlugins} title={t('mcp.tab.pluginsHint')}>
              {t('mcp.tab.plugins')}
            </HeaderButton>
          </ListToolbar>

          <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('mcp.tab.accountHint')}
          </p>

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

          {error ? (
            <LoadFailed what={t('plugins.loadFailed.mcp')} error={error} onRetry={() => void refetch()} />
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
                    checking={checkingNames.has(server.name)}
                    synced={synced}
                    sandboxRunning={sandboxRunning}
                    sandboxWarming={sandboxWarming}
                    onToggle={toggle}
                    onEdit={openEdit}
                    onDiscover={handleDiscoverRow}
                    onSetupSecret={handleSetupSecret}
                    onManageInPlugins={handleManageInPlugins}
                  />
                ))}
              </AnimatePresence>
            </div>
          )}
      </div>

      <AnimatePresence>
        {modal && (
          <McpServerModal
            {...modal}
            allowDiscover={!!modal.initial && sandboxRunning}
            onDiscover={modal.initial ? handleDiscoverFromModal : undefined}
            onProbe={probeMcpServer}
            note={modal.initial ? undefined : t('mcp.tab.addNote')}
            editLabels={{ title: t('mcp.tab.editTitle'), save: t('mcp.tab.editSave') }}
            saving={addMutation.isPending || updateMutation.isPending}
          />
        )}
      </AnimatePresence>

      <AnimatePresence>
        {importer && (
          <McpImportModal
            {...importer}
            onImport={(payload) => importMutation.mutateAsync(payload)}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
