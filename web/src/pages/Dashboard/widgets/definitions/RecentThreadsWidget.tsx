import { useMemo } from 'react';
import { MessagesSquare, ArrowUpRight, MessageSquareText, Zap } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { getRecentThreads, getWorkspaceThreads } from '@/pages/ChatAgent/utils/api';
import { clearChatSession } from '@/pages/ChatAgent/hooks/utils/chatSessionRestore';
import type { Thread, ThreadsResponse, Workspace } from '@/types/api';
import { queryKeys } from '@/lib/queryKeys';
import { registerWidget } from '../framework/WidgetRegistry';
import type { WidgetRenderProps } from '../types';

type RecentThreadsConfig = { workspaceId?: 'all' | 'current' | string; limit?: number };

type BucketKey = 'today' | 'week' | 'older';

const BUCKET_LABELS: Record<BucketKey, string> = {
  today: 'Today',
  week: 'Earlier this week',
  older: 'Older',
};

function bucketFor(date: Date): BucketKey {
  const now = new Date();
  const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const dayMs = 86_400_000;
  const t = date.getTime();
  if (t >= startOfToday) return 'today';
  if (t >= startOfToday - 6 * dayMs) return 'week';
  return 'older';
}

function formatThreadTime(date: Date, bucket: BucketKey): string {
  if (bucket === 'today') {
    return date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });
  }
  if (bucket === 'week') {
    return date.toLocaleDateString(undefined, { weekday: 'short' });
  }
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

interface GroupedThreads {
  today: Thread[];
  week: Thread[];
  older: Thread[];
}

function ThreadRow({
  thread,
  onOpen,
  bucket,
  workspaceLabel,
  isFlash,
}: {
  thread: Thread;
  onOpen: () => void;
  bucket: BucketKey;
  workspaceLabel?: string;
  isFlash?: boolean;
}) {
  const title =
    thread.title ||
    ((thread as { first_query_content?: string }).first_query_content as string | undefined) ||
    'Untitled thread';
  const updated = thread.updated_at ? new Date(thread.updated_at) : null;
  const timeStr = updated ? formatThreadTime(updated, bucket) : '';

  return (
    <button
      type="button"
      onClick={onOpen}
      className="group relative w-full text-left flex items-center gap-3 py-2 pr-2 pl-3 rounded-md transition-all duration-150"
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
        const dot = e.currentTarget.querySelector('[data-dot]') as HTMLElement | null;
        if (dot) {
          dot.style.width = '6px';
          dot.style.height = '6px';
          dot.style.backgroundColor = 'var(--color-accent-primary)';
        }
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
        const dot = e.currentTarget.querySelector('[data-dot]') as HTMLElement | null;
        if (dot) {
          dot.style.width = '3px';
          dot.style.height = '3px';
          dot.style.backgroundColor = 'var(--color-text-tertiary)';
        }
      }}
    >
      {isFlash ? (
        <Zap
          className="h-3 w-3 flex-shrink-0"
          style={{ color: 'var(--color-accent-primary)' }}
          fill="currentColor"
        />
      ) : (
        <span
          data-dot
          aria-hidden
          className="flex-shrink-0 rounded-full transition-all duration-200"
          style={{
            width: 3,
            height: 3,
            backgroundColor: 'var(--color-text-tertiary)',
          }}
        />
      )}
      <span className="flex-1 min-w-0 flex flex-col gap-0.5 overflow-hidden">
        <span
          className="text-[13px] truncate leading-tight"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {title}
        </span>
        {workspaceLabel ? (
          <span
            className="text-[10px] uppercase tracking-wider truncate"
            style={{ color: 'var(--color-text-tertiary)', opacity: 0.85 }}
          >
            {workspaceLabel}
          </span>
        ) : null}
      </span>
      {timeStr ? (
        <span
          className="text-[10.5px] dashboard-mono uppercase tracking-wider tabular-nums flex-shrink-0 transition-colors self-start pt-[2px]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {timeStr}
        </span>
      ) : null}
    </button>
  );
}

function BucketSection({
  label,
  threads,
  bucket,
  onOpen,
  workspaceMap,
}: {
  label: string;
  threads: Thread[];
  bucket: BucketKey;
  onOpen: (thread: Thread) => void;
  workspaceMap: Map<string, Workspace>;
}) {
  if (threads.length === 0) return null;
  return (
    <div className="flex flex-col">
      <div
        className="flex items-baseline gap-2 px-3 mb-0.5 sticky top-0 z-10 py-1 -mx-1"
        style={{
          backgroundColor: 'var(--color-bg-card)',
        }}
      >
        <span
          className="text-[9.5px] font-semibold uppercase tracking-[0.16em]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {label}
        </span>
        <span
          className="flex-1 h-px"
          style={{ backgroundColor: 'var(--color-border-muted)' }}
        />
        <span
          className="text-[10px] dashboard-mono tabular-nums"
          style={{ color: 'var(--color-text-tertiary)', opacity: 0.7 }}
        >
          {String(threads.length).padStart(2, '0')}
        </span>
      </div>
      <div className="flex flex-col">
        {threads.map((t) => {
          const wsId = (t as { workspace_id?: string }).workspace_id;
          const ws = wsId ? workspaceMap.get(wsId) : undefined;
          const isFlash = ws?.status === 'flash';
          const workspaceLabel = ws?.name ?? (isFlash ? 'Flash' : undefined);
          return (
            <ThreadRow
              key={t.thread_id}
              thread={t}
              bucket={bucket}
              onOpen={() => onOpen(t)}
              workspaceLabel={workspaceLabel}
              isFlash={isFlash}
            />
          );
        })}
      </div>
    </div>
  );
}

function RecentThreadsWidget({ instance }: WidgetRenderProps<RecentThreadsConfig>) {
  const navigate = useNavigate();
  const limit = instance.config.limit ?? 15;
  const scope = instance.config.workspaceId ?? 'all';
  // 'all' = cross-workspace; 'current' is legacy and also treated as cross-workspace
  // on the dashboard (there is no per-route "current workspace" here). A specific
  // workspace id string still pins the widget to that workspace only.
  const isAllScope = scope === 'all' || scope === 'current';
  const scopedWorkspaceId = !isAllScope ? (scope as string) : undefined;

  const { data: wsListData } = useWorkspaces({ limit: 100 });

  const targetWorkspaceId = scopedWorkspaceId;

  const workspaceMap = useMemo(() => {
    const map = new Map<string, Workspace>();
    (wsListData?.workspaces ?? []).forEach((ws: Workspace) => {
      if (ws.workspace_id) map.set(ws.workspace_id, ws);
    });
    return map;
  }, [wsListData]);

  const { data, isLoading } = useQuery<ThreadsResponse>({
    queryKey: isAllScope
      ? queryKeys.threads.recent(limit)
      : targetWorkspaceId
        ? [...queryKeys.threads.byWorkspace(targetWorkspaceId), limit, 0]
        : ['threads', 'noop'],
    queryFn: () =>
      (isAllScope
        ? getRecentThreads(limit, 0)
        : getWorkspaceThreads(targetWorkspaceId!, limit, 0)) as Promise<ThreadsResponse>,
    enabled: isAllScope || !!targetWorkspaceId,
    staleTime: 30_000,
  });

  const threads = useMemo(() => data?.threads ?? [], [data]);

  const grouped = useMemo<GroupedThreads>(() => {
    const out: GroupedThreads = { today: [], week: [], older: [] };
    for (const t of threads) {
      const d = t.updated_at ? new Date(t.updated_at) : null;
      if (!d || Number.isNaN(d.getTime())) {
        out.older.push(t);
        continue;
      }
      out[bucketFor(d)].push(t);
    }
    return out;
  }, [threads]);

  const handleOpen = (thread: Thread) => {
    navigate(`/chat/t/${thread.thread_id}`);
  };

  const viewAllTarget = isAllScope
    ? '/chat'
    : targetWorkspaceId
      ? `/chat/${targetWorkspaceId}`
      : '/chat';

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <MessagesSquare
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            Recent Threads
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {threads.length}
          </span>
        </div>
        <button
          type="button"
          onClick={() => {
            if (viewAllTarget === '/chat') clearChatSession();
            navigate(viewAllTarget);
          }}
          className="group flex items-center gap-1 text-[11px] uppercase tracking-wider transition-colors"
          style={{ color: 'var(--color-text-tertiary)' }}
          onMouseEnter={(e) => {
            e.currentTarget.style.color = 'var(--color-text-primary)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.color = 'var(--color-text-tertiary)';
          }}
        >
          <span>View all</span>
          <ArrowUpRight className="h-3 w-3 transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
        </button>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto -mx-1 px-1">
        {isLoading && threads.length === 0 ? (
          <div className="space-y-1.5 py-1">
            {Array.from({ length: 6 }).map((_, i) => (
              <div
                key={i}
                className="h-7 rounded animate-pulse"
                style={{
                  backgroundColor: 'var(--color-bg-subtle)',
                  opacity: 1 - i * 0.1,
                }}
              />
            ))}
          </div>
        ) : threads.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center gap-3 py-6">
            <div
              className="h-9 w-9 rounded-full flex items-center justify-center"
              style={{ backgroundColor: 'var(--color-bg-subtle)' }}
            >
              <MessageSquareText className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
            <div className="text-center">
              <div
                className="title-font italic text-sm mb-0.5"
                style={{ color: 'var(--color-text-primary)' }}
              >
                No recent threads
              </div>
              <div
                className="text-[11px]"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                Start a conversation to fill this log.
              </div>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            <BucketSection label={BUCKET_LABELS.today} threads={grouped.today} bucket="today" onOpen={handleOpen} workspaceMap={workspaceMap} />
            <BucketSection label={BUCKET_LABELS.week} threads={grouped.week} bucket="week" onOpen={handleOpen} workspaceMap={workspaceMap} />
            <BucketSection label={BUCKET_LABELS.older} threads={grouped.older} bucket="older" onOpen={handleOpen} workspaceMap={workspaceMap} />
          </div>
        )}
      </div>
    </div>
  );
}

registerWidget<RecentThreadsConfig>({
  type: 'threads.recent',
  title: 'Recent Threads',
  description: 'Latest research conversations.',
  category: 'workspace',
  icon: MessagesSquare,
  component: RecentThreadsWidget,
  defaultConfig: { workspaceId: 'all', limit: 15 },
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 3, h: 15 },
});

export default RecentThreadsWidget;
