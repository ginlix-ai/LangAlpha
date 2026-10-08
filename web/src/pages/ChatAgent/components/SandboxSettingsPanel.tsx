import { useCallback, useEffect, useId, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { api } from '@/api/client';
import { useBackdropDismiss, useDialogA11y } from '@/hooks/useDialogA11y';
import { useWorkspace } from '@/hooks/useWorkspace';
import { queryKeys } from '@/lib/queryKeys';
import {
  formatApiErrorDetail, getSandboxStats, refreshWorkspace,
  startComputer, stopComputer,
} from '../utils/api';
import { patchComputerStatusInCaches, useComputers } from '../hooks/useComputers';
import { denialMessage } from '../utils/denialMessage';
import { ListEmpty, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { McpTab } from './mcp/McpTab';
import { SkillsTab } from './SkillsTab';
import { OverviewTab } from './sandbox/OverviewTab';
import { PackagesTab } from './sandbox/PackagesTab';
import { StorageTab } from './sandbox/StorageTab';
import { ToolsTab } from './sandbox/ToolsTab';
import type { RefreshResult, SandboxStats } from './sandbox/sandboxTypes';

interface SandboxSettingsPanelProps {
  onClose: () => void;
  workspaceId: string;
}

/**
 * SandboxSettingsContent -- sandbox settings tabs and content, usable inline or in a modal.
 */
export function SandboxSettingsContent({ workspaceId }: { workspaceId: string }) {
  const [activeTab, setActiveTab] = useState('overview');
  const [stats, setStats] = useState<SandboxStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Storage expand
  const [showDirBreakdown, setShowDirBreakdown] = useState(false);

  // Tools refresh
  const [refreshing, setRefreshing] = useState(false);
  const [refreshResult, setRefreshResult] = useState<RefreshResult | null>(null);

  // Start/stop
  const [actionLoading, setActionLoading] = useState(false);

  const queryClient = useQueryClient();
  const { t } = useTranslation();

  // The machine this workspace lives on. Start/stop act on it, and its name
  // is what the panel has to say out loud before the user stops five projects.
  const { data: workspace } = useWorkspace(workspaceId);
  const computerId = workspace?.computer_id ?? null;
  const { data: computerData } = useComputers({ enabled: !!computerId });
  const computer = computerId
    ? computerData?.computers.find((c) => c.computer_id === computerId) ?? null
    : null;

  // Only the newest stats request may commit. Refresh is deliberately never
  // disabled, so a slow full-path read (~15s of probes) can still be in flight
  // when a faster post-action read lands — without this the older response wins
  // by arriving last and resurrects a stopped sandbox as running.
  const statsRequestRef = useRef(0);

  const loadStats = useCallback(async () => {
    const requestId = ++statsRequestRef.current;
    setLoading(true);
    setError(null);
    try {
      const data = await getSandboxStats(workspaceId);
      if (requestId !== statsRequestRef.current) return;
      setStats(data);
    } catch (err) {
      if (requestId !== statsRequestRef.current) return;
      setError(formatApiErrorDetail(err));
    } finally {
      if (requestId === statsRequestRef.current) setLoading(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    if (!workspaceId) return;
    // Drop the outgoing workspace's stats before reading the new one. A refresh
    // now keeps the panel on screen rather than blanking it, so without this a
    // workspace switch would render the old sandbox under the new id.
    setStats(null);
    void loadStats();
  }, [workspaceId, computer?.status, loadStats]);

  // Start and stop belong to the machine: its sandbox is what runs, and every
  // workspace on it moves together. Archive, and start on a row that names no
  // machine, still go through the workspace alias; such a row cannot be stopped.
  async function handleStartStop(action: string) {
    setActionLoading(true);
    try {
      if (computerId && (action === 'start' || action === 'stop')) {
        const res = action === 'start'
          ? await startComputer(computerId, { lazy: true })
          : await stopComputer(computerId);
        patchComputerStatusInCaches(queryClient, computerId, res.status);
        void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
      } else {
        await api.post(`/api/v1/workspaces/${workspaceId}/${action}`);
      }
      await loadStats();
    } catch (err) {
      setError(denialMessage(err, t));
    } finally {
      setActionLoading(false);
    }
  }

  async function handleRefresh() {
    setRefreshing(true);
    setRefreshResult(null);
    try {
      const result = await refreshWorkspace(workspaceId);
      setRefreshResult(result);
      // Reload stats to get updated MCP list
      loadStats();
    } catch (err) {
      setRefreshResult({ status: 'error', message: formatApiErrorDetail(err) });
    } finally {
      setRefreshing(false);
    }
  }

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'mcp', label: 'MCP' },
    { key: 'skills', label: 'Skills' },
    { key: 'storage', label: 'Storage' },
    { key: 'packages', label: 'Packages' },
    { key: 'tools', label: 'Runtime' },
  ];

  // Canonical value only. Provider synonyms are the API's job to translate — see
  // _DISPLAY_STATE_SYNONYMS server-side.
  const displayStats = stats && computer ? { ...stats, state: computer.status } : stats;
  const isRunning = displayStats?.state === 'running';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Tabs */}
      <div className="flex flex-wrap gap-1 mb-4 border-b" style={{ borderColor: 'var(--color-border-muted)' }}>
        {tabs.map(t => (
          <button
            key={t.key}
            type="button"
            onClick={() => setActiveTab(t.key)}
            className="px-3 py-2 text-sm font-medium"
            style={{
              color: activeTab === t.key ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
              borderBottom: activeTab === t.key ? '2px solid var(--color-accent-primary)' : '2px solid transparent',
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div style={{ flex: 1, minHeight: 0, overflowY: 'auto' }}>
      {/* Skeleton only before the first load. Refresh reads through the same
          path, and blanking the panel would discard the status the user is
          watching — and unmount the button they just pressed. */}
      {loading && !stats ? (
        <ListSkeleton rows={4} />
      ) : error ? (
        <ErrorState message={error} onRetry={loadStats} />
      ) : (
        <>
          {activeTab === 'overview' && (
            <OverviewTab
              stats={displayStats!}
              isRunning={isRunning}
              actionLoading={actionLoading}
              refreshing={loading}
              onStartStop={handleStartStop}
              canStop={computerId !== null}
              onRefresh={loadStats}
              computerName={computer?.name ?? null}
              recoverableCreating={computer?.status === 'creating'}
              dirName={workspace?.dir_name ?? null}
            />
          )}
          {activeTab === 'mcp' && <McpTab workspaceId={workspaceId} />}
          {activeTab === 'skills' && <SkillsTab workspaceId={workspaceId} />}
          {activeTab === 'storage' && (
            isRunning ? (
              <StorageTab
                stats={displayStats!}
                showDirBreakdown={showDirBreakdown}
                onToggleBreakdown={() => setShowDirBreakdown(!showDirBreakdown)}
              />
            ) : (
              <OfflineTabPlaceholder tabName="storage" />
            )
          )}
          {activeTab === 'packages' && (
            isRunning ? (
              <PackagesTab
                workspaceId={workspaceId}
                packages={stats!.packages ?? []}
                defaultPackages={stats!.default_packages ?? []}
                onInstalled={loadStats}
              />
            ) : (
              <OfflineTabPlaceholder tabName="packages" />
            )
          )}
          {activeTab === 'tools' && (
            isRunning ? (
              <ToolsTab
                stats={displayStats!}
                refreshing={refreshing}
                refreshResult={refreshResult}
                onRefresh={handleRefresh}
              />
            ) : (
              <OfflineTabPlaceholder tabName="runtime" />
            )
          )}
        </>
      )}
      </div>
    </div>
  );
}

/**
 * SandboxSettingsPanel -- full-screen overlay showing sandbox details.
 */
export default function SandboxSettingsPanel({ onClose, workspaceId }: SandboxSettingsPanelProps) {
  const titleId = useId();
  const dialogRef = useDialogA11y<HTMLDivElement>(onClose);
  const backdrop = useBackdropDismiss<HTMLDivElement>(onClose);
  return (
    <div
      className="fixed inset-0 z-1010 flex items-center justify-center"
      style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
      {...backdrop}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className="relative w-full max-w-2xl rounded-lg p-4 sm:p-6"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
          height: 'min(80vh, 650px)',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        {/* Close button */}
        <button
          onClick={onClose}
          className="absolute top-4 right-4 p-1 rounded-full transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-primary)' }}
          aria-label="Close"
        >
          <X className="h-5 w-5" />
        </button>

        {/* Title */}
        <h2 id={titleId} className="text-xl font-semibold mb-6" style={{ color: 'var(--color-text-primary)' }}>
          Sandbox Settings
        </h2>

        <SandboxSettingsContent workspaceId={workspaceId} />
      </div>
    </div>
  );
}

/** Panel-level load failure. Not `ListError`: this one owns the retry that is
 *  the only way back — nothing polls the stats endpoint. */
function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="flex flex-col items-center gap-4 py-8">
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{message}</p>
      <button
        onClick={onRetry}
        className="px-4 py-2 text-sm rounded-md transition-colors hover:bg-foreground/10"
        style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-elevated)' }}
      >
        Retry
      </button>
    </div>
  );
}

function OfflineTabPlaceholder({ tabName }: { tabName: string }) {
  return <ListEmpty>Start the workspace to view {tabName}</ListEmpty>;
}
