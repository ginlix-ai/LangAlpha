import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence } from 'framer-motion';
import { Plus, ServerCog, Download } from 'lucide-react';
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
import { toast } from '@/components/ui/use-toast';
import { formatApiErrorDetail, getVaultSecrets, type EffectiveServer, type McpServerInput } from '../../utils/api';
import { McpServerRow } from './McpServerRow';
import { RobinhoodConnectCard } from './RobinhoodConnectCard';
import { McpServerModal } from './McpServerModal';
import { McpImportModal } from './McpImportModal';
import { TemplatesView } from './TemplatesView';

/**
 * The "MCP" tab in the workspace settings panel. Segmented control switches
 * between the effective per-workspace list and the user's template catalog.
 *
 * Three UX guarantees this component owns:
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

type SubView = 'workspace' | 'templates';

interface McpTabProps {
  workspaceId: string;
  /** Deep-link into the Vault tab, optionally with a prefilled secret name. */
  onOpenVaultTab?: (prefillSecretName?: string) => void;
}

export function McpTab({ workspaceId, onOpenVaultTab }: McpTabProps) {
  const [view, setView] = useState<SubView>('workspace');

  const { data, isLoading, error } = useWorkspaceMcpServers(workspaceId);
  const addMutation = useAddWorkspaceMcpServer(workspaceId);
  const updateMutation = useUpdateWorkspaceMcpServer(workspaceId);
  const toggleMutation = useToggleWorkspaceMcpServer(workspaceId);
  const deleteMutation = useDeleteWorkspaceMcpServer(workspaceId);
  const discoverMutation = useDiscoverWorkspaceMcpServer(workspaceId);
  const importMutation = useImportWorkspaceMcpServers(workspaceId);
  const promoteMutation = usePromoteMcpServerToTemplate(workspaceId);

  // Template names drive the promote flow: an existing name needs an overwrite
  // confirm before clobbering. Cheap (60s staleTime), often already warm.
  const { data: catalogData } = useMcpCatalog();
  const templateNames = React.useMemo(
    () => new Set((catalogData?.servers ?? []).map((t) => t.name)),
    [catalogData],
  );

  // Vault secret names for the picker (loaded lazily; failure is non-fatal).
  const [secretNames, setSecretNames] = useState<string[]>([]);
  React.useEffect(() => {
    let cancelled = false;
    getVaultSecrets(workspaceId)
      .then((secrets: Array<{ name: string }>) => {
        if (!cancelled) setSecretNames(secrets.map((s) => s.name));
      })
      .catch(() => { if (!cancelled) setSecretNames([]); });
    return () => { cancelled = true; };
  }, [workspaceId]);

  function refetchSecretNames() {
    getVaultSecrets(workspaceId)
      .then((secrets: Array<{ name: string }>) => setSecretNames(secrets.map((s) => s.name)))
      .catch(() => {});
  }

  // Modal state
  const [modalOpen, setModalOpen] = useState(false);
  const [importOpen, setImportOpen] = useState(false);
  const [editing, setEditing] = useState<EffectiveServer | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [deletingName, setDeletingName] = useState<string | null>(null);
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
  const workspaceServerNames = new Set(servers.map((s) => s.name));

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
        // The error surfaces as the row's status (error) after the refetch;
        // no toast needed for an inline probe.
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
  // Only when the sandbox is running (discovery needs it) and only enabled
  // workspace servers (disabled rows read as "Disabled"; builtins are always
  // connected). The backend's 15s debounce backs up the once-per-mount guard.
  useEffect(() => {
    if (!sandboxRunning) return;
    for (const s of servers) {
      if (
        s.origin === 'workspace' &&
        s.enabled &&
        s.status === 'pending' &&
        !autoCheckedRef.current.has(s.name)
      ) {
        autoCheckedRef.current.add(s.name);
        void runDiscover(s.name);
      }
    }
  }, [servers, sandboxRunning, runDiscover]);

  async function handleSubmit(body: McpServerInput) {
    setSubmitError(null);
    try {
      if (editing) {
        await updateMutation.mutateAsync({ name: editing.name, body });
      } else {
        await addMutation.mutateAsync(body);
      }
      setModalOpen(false);
      setEditing(null);
    } catch (err) {
      setSubmitError(formatApiErrorDetail(err));
    }
  }

  // Row handlers are stable `useCallback`s (and take the row's `server` at call
  // time) so each row gets the SAME prop references every render — that's the
  // referential stability `React.memo(McpServerRow)` needs to skip a re-render
  // during the settling poll or when a sibling row toggles.
  const toggleAsync = toggleMutation.mutateAsync;
  const handleToggle = useCallback(
    async (server: EffectiveServer, enabled: boolean) => {
      setTogglingName(server.name);
      try {
        await toggleAsync({ name: server.name, enabled });
      } finally {
        setTogglingName(null);
      }
    },
    [toggleAsync],
  );

  const deleteAsync = deleteMutation.mutateAsync;
  const handleDelete = useCallback(
    async (server: EffectiveServer) => {
      setDeletingName(server.name);
      try {
        await deleteAsync(server.name);
      } finally {
        setDeletingName(null);
      }
    },
    [deleteAsync],
  );

  const handleEdit = useCallback((server: EffectiveServer) => {
    setEditing(server);
    setSubmitError(null);
    setModalOpen(true);
  }, []);

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
        await promoteAsync({ name, overwrite });
        toast({
          title: overwrite ? 'Template updated' : 'Saved as template',
          description: `"${name}" is now in your Templates — add it to any workspace.`,
        });
      } catch (err) {
        toast({
          variant: 'destructive',
          title: 'Could not save template',
          description: formatApiErrorDetail(err),
        });
      }
    },
    [promoteAsync],
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

  async function handleAddFromTemplate(templateName: string) {
    try {
      await addMutation.mutateAsync({ from_template: templateName });
      setView('workspace');
    } catch (err) {
      toast({
        variant: 'destructive',
        title: 'Could not add template',
        description: formatApiErrorDetail(err),
      });
    }
  }

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
      {/* Segmented control */}
      <div
        className="inline-flex self-start gap-1 p-0.5 rounded-md"
        style={{ backgroundColor: 'var(--color-bg-card)' }}
      >
        {([['workspace', 'This Workspace'], ['templates', 'Templates']] as const).map(([key, label]) => (
          <button
            key={key}
            type="button"
            onClick={() => setView(key)}
            className="px-3 py-1.5 text-xs font-medium rounded"
            style={{
              color: view === key ? 'var(--color-text-on-accent)' : 'var(--color-text-tertiary)',
              backgroundColor: view === key ? 'var(--color-accent-primary)' : 'transparent',
            }}
          >
            {label}
          </button>
        ))}
      </div>

      {view === 'workspace' ? (
        <div className="flex flex-col gap-3">
          <RobinhoodConnectCard workspaceId={workspaceId} />
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <ServerCog className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                MCP servers
              </span>
              <span className="text-xs px-1.5 py-0.5 rounded" style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}>
                {workspaceCount} / {maxServers}
              </span>
            </div>
            <div className="flex items-center gap-1.5">
              <button
                type="button"
                onClick={() => setImportOpen(true)}
                disabled={atCap}
                title={atCap ? `At ${maxServers}/${maxServers} — remove one first` : 'Paste a standard mcpServers JSON config'}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
                style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
              >
                <Download className="h-3 w-3" />
                Import JSON
              </button>
              <button
                type="button"
                onClick={() => { setEditing(null); setSubmitError(null); setModalOpen(true); }}
                disabled={atCap}
                title={atCap ? `At ${maxServers}/${maxServers} — remove one first` : undefined}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
                style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
              >
                <Plus className="h-3 w-3" />
                Add server
              </button>
            </div>
          </div>

          {!sandboxRunning && sandboxWarming && (
            <div className="text-[11px] p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}>
              Starting workspace — your servers are checked automatically as soon as it&apos;s up.
            </div>
          )}

          {!sandboxRunning && !sandboxWarming && (
            <div className="text-[11px] p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}>
              Workspace is stopped — saving a server starts it back up and checks the server automatically.
            </div>
          )}

          {promoteConfirm && (
            <div
              className="flex items-center justify-between gap-3 text-[11px] p-2 rounded"
              style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
            >
              <span className="min-w-0">
                Template <span className="font-medium">{promoteConfirm}</span> already exists. Overwrite it with this server&apos;s current config?
              </span>
              <div className="flex items-center gap-1.5 flex-shrink-0">
                <button
                  type="button"
                  onClick={async () => {
                    const name = promoteConfirm;
                    setPromoteConfirm(null);
                    await doPromote(name, true);
                  }}
                  disabled={promoteMutation.isPending}
                  className="px-2 py-1 rounded disabled:opacity-50"
                  style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
                >
                  Overwrite
                </button>
                <button
                  type="button"
                  onClick={() => setPromoteConfirm(null)}
                  className="px-2 py-1 rounded hover:bg-foreground/10"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {error ? (
            <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
              {(error as { message?: string })?.message || 'Failed to load MCP servers'}
            </div>
          ) : isLoading ? (
            <div className="flex flex-col gap-2">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-14 rounded-lg animate-pulse" style={{ backgroundColor: 'var(--color-bg-card)' }} />
              ))}
            </div>
          ) : servers.length === 0 ? (
            <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
              No MCP servers. Add one or copy a template.
            </div>
          ) : (
            <div className="flex flex-col gap-1.5">
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
                    onToggle={handleToggle}
                    onEdit={handleEdit}
                    onDiscover={handleDiscoverRow}
                    onDelete={handleDelete}
                    onPromoteToTemplate={server.origin === 'workspace' ? handlePromote : undefined}
                    onSetupSecret={handleSetupSecret}
                  />
                ))}
              </AnimatePresence>
            </div>
          )}
        </div>
      ) : (
        <TemplatesView
          workspaceId={workspaceId}
          secretNames={secretNames}
          onAddToWorkspace={handleAddFromTemplate}
          workspaceServerNames={workspaceServerNames}
        />
      )}

      {modalOpen && (
        <McpServerModal
          workspaceId={workspaceId}
          secretNames={secretNames}
          initial={editing}
          allowDiscover={!!editing && sandboxRunning}
          onClose={() => { setModalOpen(false); setEditing(null); }}
          onSubmit={handleSubmit}
          onDiscover={editing ? handleDiscoverFromModal : undefined}
          onSecretCreated={refetchSecretNames}
          saving={addMutation.isPending || updateMutation.isPending}
          submitError={submitError}
        />
      )}

      {importOpen && (
        <McpImportModal
          onClose={() => setImportOpen(false)}
          onImport={(payload) => importMutation.mutateAsync(payload)}
          onImported={(_createdNames, secretsCreated) => {
            if (secretsCreated.length > 0) refetchSecretNames();
          }}
        />
      )}

    </div>
  );
}
