import { useCallback, useEffect, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { api } from '@/api/client';
import {
  formatApiErrorDetail, getSandboxStats, refreshWorkspace,
} from '../utils/api';
import { ListEmpty, ListSkeleton } from './mcp/McpPrimitives';
import { McpTab } from './mcp/McpTab';
import { SkillsTab } from './SkillsTab';
import { OverviewTab } from './sandbox/OverviewTab';
import { PackagesTab } from './sandbox/PackagesTab';
import { StorageTab } from './sandbox/StorageTab';
import { ToolsTab } from './sandbox/ToolsTab';
import type { RefreshResult, SandboxStats } from './sandbox/sandboxTypes';
import { WorkspaceSecretsTab } from './vault/WorkspaceSecretsTab';

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

  // Only the newest stats request may commit. Refresh is deliberately never
  // disabled, so a slow full-path read (~15s of probes) can still be in flight
  // when a faster post-action read lands — without this the older response wins
  // by arriving last and resurrects a stopped sandbox as running.
  const statsRequestRef = useRef(0);

  // Vault deep-link: an MCP "Set up NAME" affordance switches to the Vault tab
  // and prefills the add form with that secret name. The tab acknowledges it so
  // the prefill fires once and doesn't replay when the tab is reopened.
  const [vaultPrefillSecret, setVaultPrefillSecret] = useState<string | null>(null);
  const consumeVaultPrefill = useCallback(() => setVaultPrefillSecret(null), []);

  useEffect(() => {
    if (!workspaceId) return;
    // Drop the outgoing workspace's stats before reading the new one. A refresh
    // now keeps the panel on screen rather than blanking it, so without this a
    // workspace switch would render the old sandbox under the new id.
    setStats(null);
    loadStats();
  }, [workspaceId]);

  async function loadStats() {
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
  }

  async function handleStartStop(action: string) {
    setActionLoading(true);
    try {
      await api.post(`/api/v1/workspaces/${workspaceId}/${action}`);
      await loadStats();
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setError(detail || `Failed to ${action} workspace`);
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
    { key: 'vault', label: 'Vault' },
    { key: 'mcp', label: 'MCP' },
    { key: 'skills', label: 'Skills' },
    { key: 'storage', label: 'Storage' },
    { key: 'packages', label: 'Packages' },
    { key: 'tools', label: 'Runtime' },
  ];

  function openVaultTab(prefillSecretName?: string) {
    setVaultPrefillSecret(prefillSecretName ?? null);
    setActiveTab('vault');
  }

  // Canonical value only. Provider synonyms are the API's job to translate — see
  // _DISPLAY_STATE_SYNONYMS server-side.
  const isRunning = stats?.state === 'running';

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
              stats={stats!}
              isRunning={isRunning}
              actionLoading={actionLoading}
              refreshing={loading}
              onStartStop={handleStartStop}
              onRefresh={loadStats}
            />
          )}
          {activeTab === 'vault' && (
            <WorkspaceSecretsTab
              workspaceId={workspaceId}
              prefillSecretName={vaultPrefillSecret}
              onPrefillConsumed={consumeVaultPrefill}
            />
          )}
          {activeTab === 'mcp' && (
            <McpTab workspaceId={workspaceId} onOpenVaultTab={openVaultTab} />
          )}
          {activeTab === 'skills' && <SkillsTab workspaceId={workspaceId} />}
          {activeTab === 'storage' && (
            isRunning ? (
              <StorageTab
                stats={stats!}
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
                stats={stats!}
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
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
      onClick={onClose}
    >
      <div
        className="relative w-full max-w-2xl rounded-lg p-4 sm:p-6"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
          height: 'min(80vh, 650px)',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Close button */}
        <button
          onClick={onClose}
          className="absolute top-4 right-4 p-1 rounded-full transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-primary)' }}
        >
          <X className="h-5 w-5" />
        </button>

        {/* Title */}
        <h2 className="text-xl font-semibold mb-6" style={{ color: 'var(--color-text-primary)' }}>
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
