import { useState, useMemo } from 'react';
import { ArrowLeft, Brain, FileText, RefreshCw } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  useUserMemory,
  useWorkspaceMemory,
  useReadUserMemory,
  useReadWorkspaceMemory,
} from '../hooks/useMemory';
import Markdown from './Markdown';
import type { MemoryEntry } from '../utils/api';

type Tier = 'user' | 'workspace';

interface MemoryPanelProps {
  workspaceId: string | null;
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function formatTime(iso: string | null): string {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    const now = new Date();
    const sameDay = d.toDateString() === now.toDateString();
    if (sameDay) {
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }
    return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  } catch {
    return '';
  }
}

function sortEntries(entries: MemoryEntry[]): MemoryEntry[] {
  return [...entries].sort((a, b) => {
    // memory.md first, then alpha
    if (a.key === 'memory.md') return -1;
    if (b.key === 'memory.md') return 1;
    return a.key.localeCompare(b.key);
  });
}

export default function MemoryPanel({ workspaceId }: MemoryPanelProps) {
  const { t } = useTranslation();
  const [tier, setTier] = useState<Tier>('user');
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  const user = useUserMemory(tier === 'user');
  const ws = useWorkspaceMemory(workspaceId, tier === 'workspace');
  const list = tier === 'user' ? user : ws;

  const sorted = useMemo(() => sortEntries(list.entries), [list.entries]);

  const userRead = useReadUserMemory(tier === 'user' ? selectedKey : null);
  const wsRead = useReadWorkspaceMemory(
    tier === 'workspace' ? workspaceId : null,
    tier === 'workspace' ? selectedKey : null,
  );
  const read = tier === 'user' ? userRead : wsRead;

  const handleTierSwitch = (next: Tier) => {
    if (next === tier) return;
    setTier(next);
    setSelectedKey(null);
  };

  const rootLabel =
    tier === 'user' ? '.agents/user/memory/' : '.agents/workspace/memory/';

  // Viewer mode
  if (selectedKey) {
    return (
      <div className="flex flex-col h-full" style={{ backgroundColor: 'var(--color-bg-page)' }}>
        <div className="flex items-center justify-between px-3 py-2 border-b"
             style={{ borderColor: 'var(--color-border-muted)' }}>
          <div className="flex items-center gap-2 min-w-0">
            <button
              onClick={() => setSelectedKey(null)}
              className="file-panel-icon-btn"
              title={t('memoryPanel.backToList')}
            >
              <ArrowLeft className="h-4 w-4" />
            </button>
            <span className="text-sm font-semibold truncate"
                  style={{ color: 'var(--color-text-primary)' }}>
              {rootLabel}{selectedKey}
            </span>
          </div>
        </div>
        <div className="flex-1 overflow-y-auto px-4 py-3">
          {read.loading && (
            <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('memoryPanel.loading')}
            </div>
          )}
          {read.error && (
            <div className="text-xs" style={{ color: 'var(--color-icon-danger)' }}>
              {read.error || t('memoryPanel.readError')}
            </div>
          )}
          {read.data && (
            <Markdown content={read.data.content} variant="panel" />
          )}
        </div>
      </div>
    );
  }

  // List mode
  return (
    <div className="flex flex-col h-full" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      {/* Tier switcher + refresh */}
      <div className="flex items-center justify-between px-3 py-2 border-b"
           style={{ borderColor: 'var(--color-border-muted)' }}>
        <div className="flex gap-1 rounded-md p-0.5"
             style={{ backgroundColor: 'var(--color-border-muted)' }}>
          {(['user', 'workspace'] as Tier[]).map((tierId) => {
            const active = tier === tierId;
            return (
              <button
                key={tierId}
                onClick={() => handleTierSwitch(tierId)}
                className="text-xs font-medium px-2.5 py-1 rounded transition-colors"
                style={{
                  color: active
                    ? 'var(--color-text-primary)'
                    : 'var(--color-text-tertiary)',
                  backgroundColor: active
                    ? 'var(--color-bg-page)'
                    : 'transparent',
                }}
              >
                {tierId === 'user'
                  ? t('memoryPanel.tierUser')
                  : t('memoryPanel.tierWorkspace')}
              </button>
            );
          })}
        </div>
        <button
          onClick={list.refresh}
          className="file-panel-icon-btn"
          title={t('memoryPanel.refresh')}
          disabled={list.loading}
        >
          <RefreshCw className={`h-4 w-4 ${list.loading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* Root path hint */}
      <div className="px-3 py-1.5 text-[11px]"
           style={{ color: 'var(--color-text-tertiary)' }}>
        {rootLabel}
      </div>

      {/* List body */}
      <div className="flex-1 overflow-y-auto">
        {list.loading && sorted.length === 0 && (
          <div className="px-3 py-6 text-xs text-center"
               style={{ color: 'var(--color-text-tertiary)' }}>
            {t('memoryPanel.loadingList')}
          </div>
        )}
        {list.error && (
          <div className="px-3 py-3 text-xs"
               style={{ color: 'var(--color-icon-danger)' }}>
            {list.error || t('memoryPanel.loadError')}
          </div>
        )}
        {!list.loading && !list.error && sorted.length === 0 && (
          <div className="px-4 py-8 flex flex-col items-center gap-3 text-center"
               style={{ color: 'var(--color-text-tertiary)' }}>
            <Brain className="h-8 w-8 opacity-40" />
            <div className="text-xs max-w-[16rem]">
              <div>
                {tier === 'user'
                  ? t('memoryPanel.emptyUser')
                  : t('memoryPanel.emptyWorkspace')}
              </div>
              <div className="mt-1">{t('memoryPanel.emptyHint')}</div>
            </div>
          </div>
        )}
        {sorted.map((entry) => (
          <button
            key={entry.key}
            onClick={() => setSelectedKey(entry.key)}
            className="w-full flex items-center gap-2 px-3 py-2 text-left transition-colors"
            style={{ color: 'var(--color-text-primary)' }}
            onMouseEnter={(e) => {
              (e.currentTarget as HTMLButtonElement).style.backgroundColor =
                'var(--color-border-muted)';
            }}
            onMouseLeave={(e) => {
              (e.currentTarget as HTMLButtonElement).style.backgroundColor = 'transparent';
            }}
          >
            <FileText
              className="h-4 w-4 flex-shrink-0"
              style={{ color: 'var(--color-text-tertiary)' }}
            />
            <div className="flex-1 min-w-0">
              <div className="text-sm truncate">{entry.key}</div>
              <div className="text-[10px]"
                   style={{ color: 'var(--color-text-tertiary)' }}>
                {formatBytes(entry.size)}
                {entry.modified_at && ` · ${formatTime(entry.modified_at)}`}
              </div>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
