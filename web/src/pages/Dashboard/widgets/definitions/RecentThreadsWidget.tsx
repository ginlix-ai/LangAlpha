import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { MessagesSquare, ArrowUpRight, MessageSquareText, Zap } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { getRecentThreads, getWorkspaceThreads } from '@/pages/ChatAgent/utils/api';
import { clearChatSession } from '@/pages/ChatAgent/hooks/utils/chatSessionRestore';
import type { Thread, ThreadsResponse, Workspace } from '@/types/api';
import { queryKeys } from '@/lib/queryKeys';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import { serializeRowsToMarkdown, wrapWidgetContext } from '../framework/snapshotSerializers';
import { RecentThreadsConfigSchema } from '../framework/configSchemas';
import { RowAttachButton } from '../../components/RowAttachButton';
import type { WidgetRenderProps } from '../types';

type RecentThreadsConfig = { workspaceId?: 'all' | 'current' | string; limit?: number };

type BucketKey = 'today' | 'week' | 'older';

const BUCKET_KEY: Record<BucketKey, string> = {
  today: 'dashboard.widgets.recentThreads.bucket_today',
  week: 'dashboard.widgets.recentThreads.bucket_thisWeek',
  older: 'dashboard.widgets.recentThreads.bucket_older',
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
    return date.toLocaleTimeString(i18n.language, { hour: 'numeric', minute: '2-digit' });
  }
  if (bucket === 'week') {
    return date.toLocaleDateString(i18n.language, { weekday: 'short' });
  }
  return date.toLocaleDateString(i18n.language, { month: 'short', day: 'numeric' });
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
  instanceId,
}: {
  thread: Thread;
  onOpen: () => void;
  bucket: BucketKey;
  workspaceLabel?: string;
  isFlash?: boolean;
  instanceId: string;
}) {
  const { t } = useTranslation();
  const title =
    thread.title ||
    ((thread as { first_query_content?: string }).first_query_content as string | undefined) ||
    t('dashboard.widgets.recentThreads.untitled');
  const updated = thread.updated_at ? new Date(thread.updated_at) : null;
  const timeStr = updated ? formatThreadTime(updated, bucket) : '';

  return (
    <div className="row-attach-host relative">
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
      <span className="absolute right-1 top-1/2 -translate-y-1/2">
        <RowAttachButton instanceId={instanceId} rowId={String(thread.thread_id)} />
      </span>
    </div>
  );
}

function BucketSection({
  label,
  threads,
  bucket,
  onOpen,
  workspaceMap,
  instanceId,
}: {
  label: string;
  threads: Thread[];
  bucket: BucketKey;
  onOpen: (thread: Thread) => void;
  workspaceMap: Map<string, Workspace>;
  instanceId: string;
}) {
  const { t } = useTranslation();
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
        {threads.map((thread) => {
          const wsId = (thread as { workspace_id?: string }).workspace_id;
          const ws = wsId ? workspaceMap.get(wsId) : undefined;
          const isFlash = ws?.status === 'flash';
          const workspaceLabel = ws?.name ?? (isFlash ? t('dashboard.widgets.recentThreads.flash') : undefined);
          return (
            <ThreadRow
              key={thread.thread_id}
              thread={thread}
              bucket={bucket}
              onOpen={() => onOpen(thread)}
              workspaceLabel={workspaceLabel}
              isFlash={isFlash}
              instanceId={instanceId}
            />
          );
        })}
      </div>
    </div>
  );
}

function RecentThreadsWidget({ instance }: WidgetRenderProps<RecentThreadsConfig>) {
  const { t } = useTranslation();
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
    for (const thread of threads) {
      const d = thread.updated_at ? new Date(thread.updated_at) : null;
      if (!d || Number.isNaN(d.getTime())) {
        out.older.push(thread);
        continue;
      }
      out[bucketFor(d)].push(thread);
    }
    return out;
  }, [threads]);

  const handleOpen = (thread: Thread) => {
    navigate(`/chat/t/${thread.thread_id}`);
  };

  useWidgetContextExport(instance.id, {
    full: () => {
      const tableRows = threads.map((th) => {
        const wsId = (th as { workspace_id?: string }).workspace_id;
        const ws = wsId ? workspaceMap.get(wsId) : undefined;
        return {
          title:
            th.title ||
            ((th as { first_query_content?: string }).first_query_content as string | undefined) ||
            t('dashboard.widgets.recentThreads.untitled'),
          workspace: ws?.name ?? (ws?.status === 'flash' ? t('dashboard.widgets.recentThreads.flash') : ''),
          when: th.updated_at ?? '',
          thread_id: th.thread_id,
        };
      });
      const body = threads.length
        ? serializeRowsToMarkdown(tableRows, [
            { key: 'title', label: 'title' },
            { key: 'workspace', label: 'workspace' },
            { key: 'when', label: 'when' },
            { key: 'thread_id', label: 'thread_id' },
          ])
        : '_no recent threads_';
      const text = wrapWidgetContext(
        'threads.recent',
        { count: threads.length, scope },
        body,
      );
      return {
        widget_type: 'threads.recent',
        widget_id: instance.id,
        label: `${t('dashboard.widgets.recentThreads.title')} · ${threads.length}`,
        description: threads.length ? `${threads.length} thread${threads.length === 1 ? '' : 's'}` : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: { scope, threads },
      };
    },
    rows: (rowId: string) => {
      const th = threads.find((x) => x.thread_id === rowId);
      if (!th) return null;
      const wsId = (th as { workspace_id?: string }).workspace_id;
      const ws = wsId ? workspaceMap.get(wsId) : undefined;
      const title =
        th.title ||
        ((th as { first_query_content?: string }).first_query_content as string | undefined) ||
        t('dashboard.widgets.recentThreads.untitled');
      const lines: string[] = [`**${title}**`];
      if (ws?.name) lines.push(`Workspace: ${ws.name}`);
      if (th.updated_at) lines.push(`Last active: ${th.updated_at}`);
      lines.push(`Thread ID: ${th.thread_id}`);
      const text = wrapWidgetContext(
        'threads.recent/row',
        { thread_id: th.thread_id },
        lines.join('\n'),
      );
      return {
        widget_type: 'threads.recent/row',
        widget_id: `${instance.id}/${rowId}`,
        label: title,
        description: ws?.name ?? t('dashboard.widgets.recentThreads.title'),
        captured_at: new Date().toISOString(),
        text,
        data: {
          thread: {
            thread_id: th.thread_id,
            title,
            updated_at: th.updated_at,
            workspace_id: wsId,
            workspace_name: ws?.name,
          },
        },
      };
    },
  });

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
            {t('dashboard.widgets.recentThreads.header')}
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
          <span>{t('dashboard.widgets.recentThreads.viewAll')}</span>
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
                className="dashboard-mono text-sm mb-0.5"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('dashboard.widgets.recentThreads.empty')}
              </div>
              <div
                className="text-[11px]"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('dashboard.widgets.recentThreads.emptyHint')}
              </div>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            <BucketSection label={t(BUCKET_KEY.today)} threads={grouped.today} bucket="today" onOpen={handleOpen} workspaceMap={workspaceMap} instanceId={instance.id} />
            <BucketSection label={t(BUCKET_KEY.week)} threads={grouped.week} bucket="week" onOpen={handleOpen} workspaceMap={workspaceMap} instanceId={instance.id} />
            <BucketSection label={t(BUCKET_KEY.older)} threads={grouped.older} bucket="older" onOpen={handleOpen} workspaceMap={workspaceMap} instanceId={instance.id} />
          </div>
        )}
      </div>
    </div>
  );
}

registerWidget<RecentThreadsConfig>({
  type: 'threads.recent',
  titleKey: 'dashboard.widgets.recentThreads.title',
  descriptionKey: 'dashboard.widgets.recentThreads.description',
  category: 'workspace',
  icon: MessagesSquare,
  component: RecentThreadsWidget,
  defaultConfig: { workspaceId: 'all', limit: 15 },
  configSchema: RecentThreadsConfigSchema,
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 3, h: 15 },
});

export default RecentThreadsWidget;
