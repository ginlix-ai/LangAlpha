import { useCallback, useEffect, useState, useMemo } from 'react';
import { ArrowLeft, Brain, FileText, RefreshCw, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  useUserMemory,
  useWorkspaceMemory,
  useReadUserMemory,
  useReadWorkspaceMemory,
} from '../hooks/useMemory';
import Markdown from './Markdown';
import type { MemoryEntry } from '../utils/api';
import { MEMORY_USER_DIR, MEMORY_WORKSPACE_DIR } from '../utils/agentPaths';

type Tier = 'user' | 'workspace';

interface MemoryPanelProps {
  workspaceId: string | null;
  /** When set, the panel switches to this entry once the list resolves. */
  targetKey?: string | null;
  targetTier?: Tier | null;
  /** Called once the panel has consumed (or rejected) the target. */
  onTargetHandled?: () => void;
  /** Routes a clicked link inside the rendered markdown body through the
   * parent's path-aware router. The panel resolves bare sibling refs
   * (e.g. `feedback_visualization_preference.md`) against the current
   * memory tier's dir before calling. */
  onOpenFile?: (path: string, workspaceId?: string) => void;
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

export default function MemoryPanel({
  workspaceId,
  targetKey,
  targetTier,
  onTargetHandled,
  onOpenFile,
}: MemoryPanelProps) {
  const { t } = useTranslation();
  const [tier, setTier] = useState<Tier>(targetTier ?? 'user');
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [notFoundKey, setNotFoundKey] = useState<string | null>(null);

  // Markdown links inside a memory body usually reference sibling entries by
  // bare filename (`feedback_visualization_preference.md`). Resolve those
  // against the current tier's dir so the parent router classifies them as
  // memory and pre-selects the right entry. Already-qualified hrefs
  // (`.agents/...`, `__wsref__/...`, absolute, or `scheme://`) pass through.
  //
  // Heuristic: only `.md` / `.markdown` hrefs are treated as sibling memory
  // entries (the canonical extension). Other extensions (`.pdf`, `.csv`,
  // `.png`, etc.) are sandbox files referenced from the memory body — pass
  // them through to file routing instead of forcing them into the memory
  // store path (where they'd 404).
  const handleBodyLinkOpen = useCallback(
    (href: string, wsId?: string) => {
      if (!onOpenFile || !href) return;
      const isAlreadyQualified =
        href.startsWith('.agents/') ||
        href.startsWith('__wsref__/') ||
        href.startsWith('/') ||
        /^[a-z][a-z0-9+.-]*:/i.test(href);
      if (isAlreadyQualified) {
        onOpenFile(href, wsId);
        return;
      }
      const clean = href.replace(/^\.\//, '');
      const tail = clean.split('?')[0].split('#')[0];
      const ext = (tail.split('.').pop() || '').toLowerCase();
      const SIBLING_EXTS = new Set(['md', 'markdown']);
      if (!SIBLING_EXTS.has(ext)) {
        // Not a memory entry — let file routing decide where it belongs.
        onOpenFile(clean, wsId);
        return;
      }
      const dir = tier === 'user' ? MEMORY_USER_DIR : MEMORY_WORKSPACE_DIR;
      onOpenFile(`${dir}/${clean}`, wsId);
    },
    [onOpenFile, tier],
  );

  // Both tiers must be active when we're chasing a target so the entry can
  // be located regardless of which tier the user was last viewing.
  const user = useUserMemory(tier === 'user' || (targetKey != null && targetTier === 'user'));
  const ws = useWorkspaceMemory(
    workspaceId,
    tier === 'workspace' || (targetKey != null && targetTier === 'workspace'),
  );
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
    setNotFoundKey(null);
  };

  // Mirror an external targetKey into selected state once the corresponding
  // list resolves. If the list loaded but the target isn't there, fall back
  // to the list view with an inline not-found banner.
  //
  // Both `loading` and `isFetching` defer the not-found decision: `loading`
  // covers the first-ever fetch, `isFetching` covers a background refetch
  // triggered by an invalidation (e.g. the agent just wrote a new entry —
  // we must wait for the refresh before declaring "not found").
  useEffect(() => {
    if (targetKey == null) return;
    if (targetTier && targetTier !== tier) {
      setTier(targetTier);
      setSelectedKey(null);
      // A banner from the prior tier's lookup would mislead after the flip —
      // the new tier's lookup hasn't decided yet.
      setNotFoundKey(null);
      // Wait for the next render after tier switch.
      return;
    }
    if (list.loading || list.isFetching) return;
    const hit = list.entries.find((e) => e.key === targetKey);
    if (hit) {
      setSelectedKey(targetKey);
      setNotFoundKey(null);
    } else {
      setSelectedKey(null);
      setNotFoundKey(targetKey);
    }
    onTargetHandled?.();
  }, [targetKey, targetTier, tier, list.loading, list.isFetching, list.entries, onTargetHandled]);

  // If the agent later writes a new entry, the not-found banner for an old
  // missing key should clear so it doesn't lie about the current list state.
  // Keying on `entries.length` would miss the case where the user deletes
  // the missing entry and the agent writes the previously-missing key in
  // the same refetch (net length unchanged) — a stable signature of the
  // entry keys catches every membership mutation.
  const entriesSig = useMemo(
    () => list.entries.map((e) => e.key).join('|'),
    [list.entries],
  );
  useEffect(() => {
    if (notFoundKey && list.entries.some((e) => e.key === notFoundKey)) {
      setNotFoundKey(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entriesSig, notFoundKey]);

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
            <Markdown
              content={read.data.content}
              variant="panel"
              onOpenFile={onOpenFile ? handleBodyLinkOpen : undefined}
            />
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

      {notFoundKey && (
        <div
          className="flex items-start justify-between gap-2 px-3 py-2 text-xs"
          style={{
            backgroundColor: 'var(--color-loss-soft)',
            color: 'var(--color-loss)',
          }}
        >
          <span className="min-w-0">
            {t('memoryPanel.notFound', { key: notFoundKey })}
          </span>
          <button
            onClick={() => setNotFoundKey(null)}
            className="flex-shrink-0"
            title={t('memoryPanel.dismissNotFound')}
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

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
