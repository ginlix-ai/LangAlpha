import React, { useState, useEffect, useMemo } from 'react';
import {
  X, Cpu, MemoryStick, HardDrive, MonitorCog, Play, Square,
  Package, Search, RefreshCw, ChevronDown, ChevronRight,
  Server, Loader2, BookOpen, Archive, KeyRound,
  Plus, Trash2, Pencil, Eye, EyeOff,
} from 'lucide-react';
import { getSandboxStats, installSandboxPackages, refreshWorkspace, getVaultSecrets, createVaultSecret, updateVaultSecret, deleteVaultSecret, revealVaultSecret } from '../utils/api';
import { api } from '@/api/client';

interface SandboxSettingsPanelProps {
  onClose: () => void;
  workspaceId: string;
}

interface SandboxPackage {
  name: string;
  version: string;
}

interface DirBreakdownEntry {
  path: string;
  size: string;
}

interface DiskUsage {
  used: string;
  available: string;
  total: string;
  use_percent: string;
}

interface SandboxSkill {
  name: string;
  description?: string;
}

interface SandboxStats {
  state: string;
  sandbox_id?: string;
  created_at?: string;
  auto_stop_interval?: number;
  resources: {
    cpu?: number;
    memory?: number;
    disk?: number;
    gpu?: number;
  };
  disk_usage?: DiskUsage;
  directory_breakdown?: DirBreakdownEntry[];
  packages?: SandboxPackage[];
  default_packages?: string[];
  mcp_servers?: string[];
  skills?: SandboxSkill[];
}

interface InstallResult {
  success: boolean;
  output: string;
  error?: string;
  installed: string[];
}

interface RefreshResult {
  status: string;
  message?: string;
  refreshed_tools?: boolean;
  skills_uploaded?: boolean;
  servers?: string[];
}

/**
 * SandboxSettingsContent -- sandbox settings tabs and content, usable inline or in a modal.
 */
export function SandboxSettingsContent({ workspaceId }: { workspaceId: string }) {
  const [activeTab, setActiveTab] = useState('overview');
  const [stats, setStats] = useState<SandboxStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Package install state
  const [installInput, setInstallInput] = useState('');
  const [installing, setInstalling] = useState(false);
  const [installResult, setInstallResult] = useState<InstallResult | null>(null);

  // Package search
  const [pkgSearch, setPkgSearch] = useState('');

  // Storage expand
  const [showDirBreakdown, setShowDirBreakdown] = useState(false);

  // Tools refresh
  const [refreshing, setRefreshing] = useState(false);
  const [refreshResult, setRefreshResult] = useState<RefreshResult | null>(null);

  // Start/stop
  const [actionLoading, setActionLoading] = useState(false);

  useEffect(() => {
    if (!workspaceId) return;
    loadStats();
  }, [workspaceId]);

  async function loadStats() {
    setLoading(true);
    setError(null);
    try {
      const data = await getSandboxStats(workspaceId);
      setStats(data);
    } catch (err: any) { // TODO: type properly
      setError(err?.response?.data?.detail || err.message || 'Failed to load sandbox stats');
    } finally {
      setLoading(false);
    }
  }

  async function handleStartStop(action: string) {
    setActionLoading(true);
    try {
      await api.post(`/api/v1/workspaces/${workspaceId}/${action}`);
      await loadStats();
    } catch (err: any) { // TODO: type properly
      setError(err?.response?.data?.detail || `Failed to ${action} workspace`);
    } finally {
      setActionLoading(false);
    }
  }

  async function handleInstall() {
    const packages = installInput.split(/[\s,]+/).filter(Boolean);
    if (!packages.length) return;
    setInstalling(true);
    setInstallResult(null);
    try {
      const result = await installSandboxPackages(workspaceId, packages);
      setInstallResult(result);
      if (result.success) {
        setInstallInput('');
        // Refresh stats to show new packages
        loadStats();
      }
    } catch (err: any) { // TODO: type properly
      setInstallResult({
        success: false,
        output: '',
        error: err?.response?.data?.detail || err.message,
        installed: [],
      });
    } finally {
      setInstalling(false);
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
    } catch (err: any) { // TODO: type properly
      setRefreshResult({ status: 'error', message: err?.response?.data?.detail || err.message });
    } finally {
      setRefreshing(false);
    }
  }

  // Filter packages by search
  const filteredPackages = useMemo(() => {
    if (!stats?.packages) return [];
    if (!pkgSearch.trim()) return stats.packages;
    const q = pkgSearch.toLowerCase();
    return stats.packages.filter(p => p.name.toLowerCase().includes(q));
  }, [stats?.packages, pkgSearch]);

  const defaultPkgSet = useMemo(
    () => new Set((stats?.default_packages || []).map(p => p.split(/[<>=!~]/)[0].toLowerCase())),
    [stats?.default_packages],
  );

  const tabs = [
    { key: 'overview', label: 'Overview' },
    { key: 'vault', label: 'Vault' },
    { key: 'storage', label: 'Storage' },
    { key: 'packages', label: 'Packages' },
    { key: 'tools', label: 'Tools & Skills' },
  ];

  const isRunning = stats?.state === 'started';

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
      {loading ? (
        <LoadingSkeleton />
      ) : error ? (
        <ErrorState message={error} onRetry={loadStats} />
      ) : (
        <>
          {activeTab === 'overview' && (
            <OverviewTab
              stats={stats!}
              isRunning={isRunning!}
              actionLoading={actionLoading}
              onStartStop={handleStartStop}
            />
          )}
          {activeTab === 'vault' && (
            <SecretsTab workspaceId={workspaceId} />
          )}
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
                filteredPackages={filteredPackages}
                defaultPkgSet={defaultPkgSet}
                pkgSearch={pkgSearch}
                onSearchChange={setPkgSearch}
                installInput={installInput}
                onInstallInputChange={setInstallInput}
                installing={installing}
                installResult={installResult}
                onInstall={handleInstall}
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
              <OfflineTabPlaceholder tabName="tools & skills" />
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


// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function LoadingSkeleton() {
  return (
    <div className="flex flex-col gap-4">
      {[1, 2, 3, 4].map(i => (
        <div
          key={i}
          className="h-16 rounded-lg animate-pulse"
          style={{ backgroundColor: 'var(--color-bg-card)' }}
        />
      ))}
    </div>
  );
}

interface ErrorStateProps {
  message: string;
  onRetry: () => void;
}

function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="flex flex-col items-center gap-4 py-8">
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{message}</p>
      <button
        onClick={onRetry}
        className="px-4 py-2 text-sm rounded-md transition-colors hover:bg-foreground/10"
        style={{ color: 'var(--color-accent-primary)', border: '1px solid var(--color-accent-primary)' }}
      >
        Retry
      </button>
    </div>
  );
}


interface OfflineTabPlaceholderProps {
  tabName: string;
}

function OfflineTabPlaceholder({ tabName }: OfflineTabPlaceholderProps) {
  return (
    <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
      Start the workspace to view {tabName}
    </div>
  );
}


// ---------------------------------------------------------------------------
// Overview Tab
// ---------------------------------------------------------------------------

const TRANSITIONAL_STATES = new Set(['archiving', 'stopping', 'starting']);

interface OverviewTabProps {
  stats: SandboxStats;
  isRunning: boolean;
  actionLoading: boolean;
  onStartStop: (action: string) => void;
}

function OverviewTab({ stats, isRunning, actionLoading, onStartStop }: OverviewTabProps) {
  const isTransitioning = actionLoading || TRANSITIONAL_STATES.has(stats?.state);
  const resourceCards = [
    { icon: Cpu, label: 'CPU', value: stats.resources.cpu != null ? `${stats.resources.cpu} vCPU` : '---' },
    { icon: MemoryStick, label: 'Memory', value: stats.resources.memory != null ? `${stats.resources.memory} GiB` : '---' },
    { icon: HardDrive, label: 'Disk', value: stats.resources.disk != null ? `${stats.resources.disk} GiB` : '---' },
    { icon: MonitorCog, label: 'GPU', value: stats.resources.gpu != null ? `${stats.resources.gpu} GPU` : '---' },
  ];

  return (
    <div className="flex flex-col gap-5">
      {/* Resource cards -- 2x2 grid */}
      <div className="grid grid-cols-2 gap-3">
        {resourceCards.map(({ icon: Icon, label, value }) => (
          <div
            key={label}
            className="flex items-center gap-3 p-3 rounded-lg"
            style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
          >
            <Icon className="h-5 w-5 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
            <div>
              <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{label}</div>
              <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{value}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Status + metadata */}
      <div
        className="flex items-center justify-between p-3 rounded-lg"
        style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
      >
        <div className="flex items-center gap-3">
          {isTransitioning ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          ) : (
            <div
              className="w-2.5 h-2.5 rounded-full flex-shrink-0"
              style={{ backgroundColor: isRunning ? 'var(--color-profit)' : 'var(--color-loss)' }}
            />
          )}
          <div>
            <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {isTransitioning
                ? (actionLoading ? 'Updating...' : stats.state.charAt(0).toUpperCase() + stats.state.slice(1) + '...')
                : isRunning ? 'Running' : stats.state ? stats.state.charAt(0).toUpperCase() + stats.state.slice(1) : 'Unknown'}
            </div>
            {stats.created_at && (
              <div className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                Created {new Date(stats.created_at).toLocaleDateString()}
              </div>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {stats.auto_stop_interval != null && (
            <span className="text-xs px-2 py-1 rounded" style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}>
              Auto-stop: {stats.auto_stop_interval}m
            </span>
          )}
          {!isRunning && stats.state === 'stopped' && (
            <button
              onClick={() => onStartStop('archive')}
              disabled={isTransitioning}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' }}
            >
              <Archive className="h-3 w-3" />
              Archive
            </button>
          )}
          {isRunning ? (
            <button
              onClick={() => onStartStop('stop')}
              disabled={isTransitioning}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-loss)', border: '1px solid var(--color-border-loss)' }}
            >
              <Square className="h-3 w-3" />
              Stop
            </button>
          ) : (
            <button
              onClick={() => onStartStop('start')}
              disabled={isTransitioning}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-profit)', border: '1px solid var(--color-profit-border)' }}
            >
              <Play className="h-3 w-3" />
              Start
            </button>
          )}
        </div>
      </div>

      {/* Sandbox ID */}
      {stats.sandbox_id && (
        <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          Sandbox ID: <span className="font-mono">{stats.sandbox_id}</span>
        </div>
      )}
    </div>
  );
}


// ---------------------------------------------------------------------------
// Storage Tab
// ---------------------------------------------------------------------------

interface StorageTabProps {
  stats: SandboxStats;
  showDirBreakdown: boolean;
  onToggleBreakdown: () => void;
}

function StorageTab({ stats, showDirBreakdown, onToggleBreakdown }: StorageTabProps) {
  const disk = stats.disk_usage;

  if (!disk) {
    return (
      <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
        Disk usage information unavailable
      </div>
    );
  }

  // Parse use_percent for the progress bar
  const pct = parseInt(disk.use_percent, 10) || 0;

  return (
    <div className="flex flex-col gap-5">
      {/* Usage bar */}
      <div className="flex flex-col gap-2">
        <div className="flex justify-between text-sm" style={{ color: 'var(--color-text-primary)' }}>
          <span>{disk.used} used</span>
          <span>{disk.available} available</span>
        </div>
        <div className="h-3 rounded-full overflow-hidden" style={{ backgroundColor: 'var(--color-bg-card)' }}>
          <div
            className="h-full rounded-full transition-all"
            style={{
              width: `${pct}%`,
              backgroundColor: pct > 80 ? 'var(--color-loss)' : 'var(--color-accent-primary)',
            }}
          />
        </div>
        <div className="flex justify-between text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          <span>{disk.use_percent} used</span>
          <span>{disk.total} total</span>
        </div>
      </div>

      {/* Directory breakdown toggle */}
      {stats.directory_breakdown && stats.directory_breakdown.length > 0 && (
        <div>
          <button
            onClick={onToggleBreakdown}
            className="flex items-center gap-1.5 text-sm transition-colors hover:opacity-80"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {showDirBreakdown ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            Details ({stats.directory_breakdown.length} directories)
          </button>

          {showDirBreakdown && (
            <div className="mt-3 flex flex-col gap-1">
              {stats.directory_breakdown.map((d) => (
                <div
                  key={d.path}
                  className="flex justify-between py-1.5 px-3 rounded text-sm"
                  style={{ backgroundColor: 'var(--color-bg-card)' }}
                >
                  <span className="font-mono truncate" style={{ color: 'var(--color-text-primary)' }}>{d.path}/</span>
                  <span className="flex-shrink-0 ml-4" style={{ color: 'var(--color-text-tertiary)' }}>{d.size}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}


// ---------------------------------------------------------------------------
// Packages Tab
// ---------------------------------------------------------------------------

interface PackagesTabProps {
  filteredPackages: SandboxPackage[];
  defaultPkgSet: Set<string>;
  pkgSearch: string;
  onSearchChange: (value: string) => void;
  installInput: string;
  onInstallInputChange: (value: string) => void;
  installing: boolean;
  installResult: InstallResult | null;
  onInstall: () => void;
}

function PackagesTab({
  filteredPackages, defaultPkgSet, pkgSearch, onSearchChange,
  installInput, onInstallInputChange, installing, installResult, onInstall,
}: PackagesTabProps) {
  return (
    <div className="flex flex-col gap-4">
      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
        <input
          type="text"
          value={pkgSearch}
          onChange={e => onSearchChange(e.target.value)}
          placeholder="Filter packages..."
          className="w-full pl-9 pr-3 py-2 text-sm rounded-md bg-transparent outline-none"
          style={{
            color: 'var(--color-text-primary)',
            border: '1px solid var(--color-border-muted)',
          }}
        />
      </div>

      {/* Package list */}
      <div
        className="flex flex-col gap-0.5 overflow-y-auto"
        style={{ maxHeight: '320px' }}
      >
        {filteredPackages.length === 0 ? (
          <div className="py-6 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            {pkgSearch ? 'No matching packages' : 'No packages installed'}
          </div>
        ) : (
          filteredPackages.map(p => {
            const isDefault = defaultPkgSet.has(p.name.toLowerCase());
            return (
              <div
                key={p.name}
                className="flex justify-between items-center py-1.5 px-3 rounded text-sm"
                style={{ backgroundColor: 'var(--color-bg-card)' }}
              >
                <div className="flex items-center gap-2">
                  <span style={{ color: isDefault ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)' }}>
                    {p.name}
                  </span>
                  {isDefault && (
                    <span
                      className="text-[10px] px-1.5 py-0.5 rounded"
                      style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}
                    >
                      default
                    </span>
                  )}
                </div>
                <span className="font-mono text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                  {p.version}
                </span>
              </div>
            );
          })
        )}
      </div>

      {/* Install section */}
      <div
        className="flex flex-col gap-2 pt-3 border-t"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex gap-2">
          <input
            type="text"
            value={installInput}
            onChange={e => onInstallInputChange(e.target.value)}
            placeholder="Package names (e.g. torch transformers>=4.0)"
            onKeyDown={e => e.key === 'Enter' && !installing && onInstall()}
            className="flex-1 px-3 py-2 text-sm rounded-md bg-transparent outline-none"
            style={{
              color: 'var(--color-text-primary)',
              border: '1px solid var(--color-border-muted)',
            }}
          />
          <button
            onClick={onInstall}
            disabled={installing || !installInput.trim()}
            className="flex items-center gap-1.5 px-4 py-2 text-sm rounded-md transition-colors disabled:opacity-50"
            style={{
              color: 'var(--color-text-on-accent)',
              backgroundColor: 'var(--color-accent-primary)',
            }}
          >
            {installing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Package className="h-3.5 w-3.5" />}
            Install
          </button>
        </div>

        {/* Install result */}
        {installResult && (
          <div
            className="text-xs p-2 rounded font-mono whitespace-pre-wrap max-h-32 overflow-y-auto"
            style={{
              backgroundColor: 'var(--color-bg-card)',
              color: installResult.success ? 'var(--color-text-secondary)' : 'var(--color-loss)',
            }}
          >
            {installResult.error || installResult.output || 'Done'}
          </div>
        )}
      </div>
    </div>
  );
}


// ---------------------------------------------------------------------------
// Tools & Skills Tab
// ---------------------------------------------------------------------------

interface ToolsTabProps {
  stats: SandboxStats;
  refreshing: boolean;
  refreshResult: RefreshResult | null;
  onRefresh: () => void;
}

function ToolsTab({ stats, refreshing, refreshResult, onRefresh }: ToolsTabProps) {
  return (
    <div className="flex flex-col gap-5">
      {/* MCP Servers list */}
      <div>
        <h3 className="text-sm font-medium mb-3" style={{ color: 'var(--color-text-primary)' }}>
          Connected MCP Servers
        </h3>
        {stats.mcp_servers && stats.mcp_servers.length > 0 ? (
          <div className="flex flex-col gap-1">
            {stats.mcp_servers.map(name => (
              <div
                key={name}
                className="flex items-center gap-2.5 py-2 px-3 rounded text-sm"
                style={{ backgroundColor: 'var(--color-bg-card)' }}
              >
                <Server className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                <span style={{ color: 'var(--color-text-primary)' }}>{name}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="py-4 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            No MCP servers connected
          </div>
        )}
      </div>

      {/* Skills list */}
      <div>
        <h3 className="text-sm font-medium mb-3" style={{ color: 'var(--color-text-primary)' }}>
          Available Skills
        </h3>
        {stats.skills && stats.skills.length > 0 ? (
          <div className="flex flex-col gap-1">
            {stats.skills.map(skill => (
              <div
                key={skill.name}
                className="flex items-start gap-2.5 py-2 px-3 rounded text-sm"
                style={{ backgroundColor: 'var(--color-bg-card)' }}
              >
                <BookOpen className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-accent-primary)' }} />
                <div className="min-w-0">
                  <span style={{ color: 'var(--color-text-primary)' }}>{skill.name}</span>
                  {skill.description && (
                    <p className="text-xs mt-0.5 line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
                      {skill.description}
                    </p>
                  )}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="py-4 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            No skills installed
          </div>
        )}
      </div>

      {/* Sync button */}
      <div
        className="flex flex-col gap-3 pt-3 border-t"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <button
          onClick={onRefresh}
          disabled={refreshing}
          className="flex items-center justify-center gap-2 w-full px-4 py-2.5 text-sm rounded-md transition-colors disabled:opacity-50"
          style={{
            color: 'var(--color-text-primary)',
            border: '1px solid var(--color-border-muted)',
            backgroundColor: 'var(--color-bg-card)',
          }}
        >
          {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          Sync Tools & Skills
        </button>

        {refreshResult && (
          <div
            className="text-xs p-3 rounded"
            style={{
              backgroundColor: 'var(--color-bg-card)',
              color: refreshResult.status === 'error' ? 'var(--color-loss)' : 'var(--color-text-secondary)',
            }}
          >
            {refreshResult.status === 'error' ? (
              refreshResult.message
            ) : (
              <div className="flex flex-col gap-1">
                <span>Tools refreshed: {refreshResult.refreshed_tools ? 'Yes' : 'No'}</span>
                <span>Skills uploaded: {refreshResult.skills_uploaded ? 'Yes' : 'No'}</span>
                {refreshResult.servers && refreshResult.servers.length > 0 && (
                  <span>Servers: {refreshResult.servers.length} connected</span>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}


// ---------------------------------------------------------------------------
// Secrets Tab
// ---------------------------------------------------------------------------

const MAX_SECRETS = 20;
const NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;

interface VaultSecret {
  workspace_vault_secret_id: string;
  name: string;
  description: string;
  masked_value: string;
  created_at: string;
  updated_at: string;
}

function SecretsTab({ workspaceId }: { workspaceId: string }) {
  const [secrets, setSecrets] = useState<VaultSecret[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Add form
  const [showAdd, setShowAdd] = useState(false);
  const [newName, setNewName] = useState('');
  const [newValue, setNewValue] = useState('');
  const [newDesc, setNewDesc] = useState('');
  const [saving, setSaving] = useState(false);

  // Edit state
  const [editingName, setEditingName] = useState<string | null>(null);
  const [editValue, setEditValue] = useState('');
  const [editDesc, setEditDesc] = useState('');
  const [editSaving, setEditSaving] = useState(false);

  // Delete confirmation
  const [deletingName, setDeletingName] = useState<string | null>(null);

  // Visibility toggles
  const [showNewValue, setShowNewValue] = useState(false);
  const [showEditValue, setShowEditValue] = useState(false);
  const [revealedSecrets, setRevealedSecrets] = useState<Record<string, string>>({});
  const [revealingName, setRevealingName] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const data = await getVaultSecrets(workspaceId);
      setSecrets(data);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Failed to load secrets');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, [workspaceId]);

  async function handleCreate() {
    if (!newName || !newValue) return;
    if (!NAME_RE.test(newName)) {
      setError('Name must use letters, digits, underscores; start with letter or underscore');
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await createVaultSecret(workspaceId, {
        name: newName,
        value: newValue,
        description: newDesc,
      });
      setNewName('');
      setNewValue('');
      setNewDesc('');
      setShowNewValue(false);
      setShowAdd(false);
      await load();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Failed to create secret');
    } finally {
      setSaving(false);
    }
  }

  async function handleUpdate(name: string) {
    setEditSaving(true);
    setError(null);
    try {
      const body: { value?: string; description?: string } = {};
      if (editValue) body.value = editValue;
      body.description = editDesc;
      await updateVaultSecret(workspaceId, name, body);
      setEditingName(null);
      setEditValue('');
      setEditDesc('');
      setShowEditValue(false);
      setRevealedSecrets(prev => {
        const next = { ...prev };
        delete next[name];
        return next;
      });
      await load();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Failed to update secret');
    } finally {
      setEditSaving(false);
    }
  }

  const [deleteLoading, setDeleteLoading] = useState(false);

  async function handleDelete(name: string) {
    setDeleteLoading(true);
    setError(null);
    try {
      await deleteVaultSecret(workspaceId, name);
      setDeletingName(null);
      await load();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Failed to delete secret');
    } finally {
      setDeleteLoading(false);
    }
  }

  async function handleRevealToggle(name: string) {
    if (revealedSecrets[name] !== undefined) {
      setRevealedSecrets(prev => {
        const next = { ...prev };
        delete next[name];
        return next;
      });
      return;
    }
    setRevealingName(name);
    try {
      const value = await revealVaultSecret(workspaceId, name);
      setRevealedSecrets(prev => ({ ...prev, [name]: value }));
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Failed to reveal secret');
    } finally {
      setRevealingName(null);
    }
  }

  if (loading) {
    return (
      <div className="flex flex-col gap-3">
        {[1, 2].map(i => (
          <div key={i} className="h-14 rounded-lg animate-pulse" style={{ backgroundColor: 'var(--color-bg-card)' }} />
        ))}
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Header + counter */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <KeyRound className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
          <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            Vault
          </span>
          <span className="text-xs px-1.5 py-0.5 rounded" style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}>
            {secrets.length} / {MAX_SECRETS}
          </span>
        </div>
        {secrets.length < MAX_SECRETS && (
          <button
            onClick={() => { setShowAdd(!showAdd); setError(null); }}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors"
            style={{
              color: 'var(--color-text-on-accent)',
              backgroundColor: 'var(--color-accent-primary)',
            }}
          >
            <Plus className="h-3 w-3" />
            Add Secret
          </button>
        )}
      </div>

      {error && (
        <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
          {error}
        </div>
      )}

      {/* Add form */}
      {showAdd && (
        <div
          className="flex flex-col gap-2 p-3 rounded-lg"
          style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
        >
          <input
            type="text"
            value={newName}
            onChange={e => setNewName(e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, '').replace(/^[0-9]+/, ''))}
            placeholder="SECRET_NAME"
            className="w-full px-3 py-2 text-sm rounded-md bg-transparent outline-none font-mono"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
            maxLength={64}
          />
          <div className="relative">
            <input
              type={showNewValue ? 'text' : 'password'}
              value={newValue}
              onChange={e => setNewValue(e.target.value)}
              placeholder="Secret value"
              className="w-full px-3 py-2 pr-9 text-sm rounded-md bg-transparent outline-none"
              style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
              maxLength={4096}
            />
            <button
              type="button"
              onClick={() => setShowNewValue(!showNewValue)}
              className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
              tabIndex={-1}
            >
              {showNewValue ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
            </button>
          </div>
          <input
            type="text"
            value={newDesc}
            onChange={e => setNewDesc(e.target.value)}
            placeholder="Description (optional)"
            className="w-full px-3 py-2 text-sm rounded-md bg-transparent outline-none"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
            maxLength={256}
          />
          <div className="flex justify-end gap-2 mt-1">
            <button
              onClick={() => { setShowAdd(false); setNewName(''); setNewValue(''); setNewDesc(''); setShowNewValue(false); }}
              className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              Cancel
            </button>
            <button
              onClick={handleCreate}
              disabled={saving || !newName || !newValue}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
            >
              {saving && <Loader2 className="h-3 w-3 animate-spin" />}
              Save
            </button>
          </div>
        </div>
      )}

      {/* Secret list */}
      {secrets.length === 0 ? (
        <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
          No secrets stored. Add API keys or credentials for agent code to use.
        </div>
      ) : (
        <div className="flex flex-col gap-1">
          {secrets.map(secret => (
            <div key={secret.workspace_vault_secret_id}>
              {editingName === secret.name ? (
                /* Edit form */
                <div
                  className="flex flex-col gap-2 p-3 rounded-lg"
                  style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-accent-primary)' }}
                >
                  <div className="text-sm font-mono font-medium" style={{ color: 'var(--color-text-primary)' }}>
                    {secret.name}
                  </div>
                  <div className="relative">
                    <input
                      type={showEditValue ? 'text' : 'password'}
                      value={editValue}
                      onChange={e => setEditValue(e.target.value)}
                      placeholder="New value (leave empty to keep current)"
                      className="w-full px-3 py-2 pr-9 text-sm rounded-md bg-transparent outline-none"
                      style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                      maxLength={4096}
                    />
                    <button
                      type="button"
                      onClick={() => setShowEditValue(!showEditValue)}
                      className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded transition-colors hover:bg-foreground/10"
                      style={{ color: 'var(--color-text-tertiary)' }}
                      tabIndex={-1}
                    >
                      {showEditValue ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
                    </button>
                  </div>
                  <input
                    type="text"
                    value={editDesc}
                    onChange={e => setEditDesc(e.target.value)}
                    placeholder="Description"
                    className="w-full px-3 py-2 text-sm rounded-md bg-transparent outline-none"
                    style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                    maxLength={256}
                  />
                  <div className="flex justify-end gap-2 mt-1">
                    <button
                      onClick={() => { setEditingName(null); setEditValue(''); setEditDesc(''); setShowEditValue(false); }}
                      className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
                      style={{ color: 'var(--color-text-tertiary)' }}
                    >
                      Cancel
                    </button>
                    <button
                      onClick={() => handleUpdate(secret.name)}
                      disabled={editSaving}
                      className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
                      style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
                    >
                      {editSaving && <Loader2 className="h-3 w-3 animate-spin" />}
                      Update
                    </button>
                  </div>
                </div>
              ) : (
                /* Display row */
                <div
                  className="flex items-center justify-between py-2.5 px-3 rounded-lg"
                  style={{ backgroundColor: 'var(--color-bg-card)' }}
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-mono font-medium" style={{ color: 'var(--color-text-primary)' }}>
                        {secret.name}
                      </span>
                      <span className="text-xs font-mono" style={{ color: 'var(--color-text-tertiary)' }}>
                        {revealedSecrets[secret.name] !== undefined ? revealedSecrets[secret.name] : secret.masked_value}
                      </span>
                    </div>
                    {secret.description && (
                      <p className="text-xs mt-0.5 truncate" style={{ color: 'var(--color-text-tertiary)' }}>
                        {secret.description}
                      </p>
                    )}
                  </div>
                  <div className="flex items-center gap-1 flex-shrink-0 ml-2">
                    <button
                      onClick={() => handleRevealToggle(secret.name)}
                      disabled={revealingName === secret.name}
                      className="p-1.5 rounded transition-colors hover:bg-foreground/10 disabled:opacity-50"
                      style={{ color: 'var(--color-text-tertiary)' }}
                      title={revealedSecrets[secret.name] !== undefined ? 'Hide value' : 'Reveal value'}
                    >
                      {revealingName === secret.name ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      ) : revealedSecrets[secret.name] !== undefined ? (
                        <EyeOff className="h-3.5 w-3.5" />
                      ) : (
                        <Eye className="h-3.5 w-3.5" />
                      )}
                    </button>
                    <button
                      onClick={() => {
                        setEditingName(secret.name);
                        setEditValue('');
                        setEditDesc(secret.description);
                        setError(null);
                      }}
                      className="p-1.5 rounded transition-colors hover:bg-foreground/10"
                      style={{ color: 'var(--color-text-tertiary)' }}
                      title="Edit"
                    >
                      <Pencil className="h-3.5 w-3.5" />
                    </button>
                    {deletingName === secret.name ? (
                      <div className="flex items-center gap-1">
                        <button
                          onClick={() => handleDelete(secret.name)}
                          disabled={deleteLoading}
                          className="px-2 py-1 text-xs rounded transition-colors disabled:opacity-50"
                          style={{ color: 'var(--color-loss)', backgroundColor: 'var(--color-bg-card)' }}
                        >
                          {deleteLoading ? 'Deleting…' : 'Confirm'}
                        </button>
                        <button
                          onClick={() => setDeletingName(null)}
                          className="px-2 py-1 text-xs rounded transition-colors hover:bg-foreground/10"
                          style={{ color: 'var(--color-text-tertiary)' }}
                        >
                          Cancel
                        </button>
                      </div>
                    ) : (
                      <button
                        onClick={() => setDeletingName(secret.name)}
                        className="p-1.5 rounded transition-colors hover:bg-foreground/10"
                        style={{ color: 'var(--color-text-tertiary)' }}
                        title="Delete"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    )}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Usage & Security info */}
      <div
        className="flex flex-col gap-2.5 text-xs p-3 rounded-lg mt-1"
        style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}
      >
        <div>
          <span className="font-medium" style={{ color: 'var(--color-text-secondary)' }}>Usage</span>
          <div className="mt-1">
            Access in code: <code className="font-mono" style={{ color: 'var(--color-text-secondary)' }}>{'from vault import get; key = get("SECRET_NAME")'}</code>
          </div>
        </div>
        <div
          className="pt-2 flex flex-col gap-1.5"
          style={{ borderTop: '1px solid var(--color-border-muted)' }}
        >
          <span className="font-medium" style={{ color: 'var(--color-text-secondary)' }}>Security</span>
          <ul className="flex flex-col gap-1 pl-3" style={{ listStyleType: 'disc' }}>
            <li>Secrets are encrypted at rest with AES (pgcrypto) and never stored in plaintext on our servers.</li>
            <li>The AI agent cannot read secret values directly &mdash; it can only call <code className="font-mono" style={{ color: 'var(--color-text-secondary)' }}>vault.get()</code> inside sandboxed code.</li>
            <li>All agent output is scanned by leak detection. Any secret value found in tool results is automatically redacted before reaching the model.</li>
            <li>Direct file access to the internal secret store is blocked by code validation &mdash; only the <code className="font-mono" style={{ color: 'var(--color-text-secondary)' }}>vault</code> API is allowed.</li>
          </ul>
        </div>
      </div>
    </div>
  );
}
