import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { clockTime, shortMonthDay, shortWeekday } from '@/lib/format';
import { useNow } from '@/hooks/useNow';
import { MessagesSquare, ArrowUpRight, MessageSquareText } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { HomeRowIcon } from '@/hooks/useHomeWorkspace';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { getRecentThreads } from '@/pages/ChatAgent/utils/api';
import { workspaceThreadsQuery } from '@/pages/ChatAgent/utils/threadQueries';
import { clearChatSession } from '@/pages/ChatAgent/hooks/utils/chatSessionRestore';
import type { Thread, ThreadsResponse, Workspace } from '@/types/api';
import { queryKeys } from '@/lib/queryKeys';
import { registerWidget } from '../framework/WidgetRegistry';
import { WidgetCardMeta } from '../framework/widgetCardSlots';
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

function bucketFor(date: Date, now: number): BucketKey {
  const today = new Date(now);
  const startOfToday = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime();
  const dayMs = 86_400_000;
  const t = date.getTime();
  if (t >= startOfToday) return 'today';
  if (t >= startOfToday - 6 * dayMs) return 'week';
  return 'older';
}


function formatThreadTime(date: Date, bucket: BucketKey, locale: string): string {
  if (Number.isNaN(date.getTime())) return '';
  if (bucket === 'today') return clockTime(date, locale);
  if (bucket === 'week') return shortWeekday(date, locale);
  return shortMonthDay(date, locale);
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
  isHome,
  instanceId,
}: {
  thread: Thread;
  onOpen: () => void;
  bucket: BucketKey;
  workspaceLabel?: string;
  isHome?: boolean;
  instanceId: string;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const title =
    thread.title ||
    ((thread as { first_query_content?: string }).first_query_content as string | undefined) ||
    t('dashboard.widgets.recentThreads.untitled');
  const updated = thread.updated_at ? new Date(thread.updated_at) : null;
  const timeStr = updated ? formatThreadTime(updated, bucket, locale) : '';

  return (
    <div className="row-attach-host relative">
    <button
      type="button"
      onClick={onOpen}
      className="group relative w-full text-left flex items-center gap-3 py-2 pr-2 pl-3 rounded-md transition-all"
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
      {isHome ? (
        <HomeRowIcon
          className="h-3 w-3 shrink-0"
          style={{ color: 'var(--color-accent-primary)' }}
        />
      ) : (
        <span
          data-dot
          aria-hidden
          className="shrink-0 rounded-full transition-all"
          style={{
            width: 3,
            height: 3,
            backgroundColor: 'var(--color-text-tertiary)',
          }}
        />
      )}
      <span className="flex-1 min-w-0 flex flex-col gap-0.5 overflow-hidden">
        <span
          className="text-[0.8125rem] truncate leading-tight"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {title}
        </span>
        {workspaceLabel ? (
          <span
            className="text-[0.625rem] uppercase tracking-wider truncate"
            style={{ color: 'var(--color-text-tertiary)', opacity: 0.85 }}
          >
            {workspaceLabel}
          </span>
        ) : null}
      </span>
      {timeStr ? (
        <span
          className="text-[0.6563rem] dashboard-mono uppercase tracking-wider tabular-nums shrink-0 transition-colors self-start pt-[2px]"
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
  const wsLabel = useWorkspaceLabel();
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
          className="text-[0.5938rem] font-semibold uppercase tracking-[0.16em]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {label}
        </span>
        <span
          className="flex-1 h-px"
          style={{ backgroundColor: 'var(--color-border-muted)' }}
        />
        <span
          className="text-[0.625rem] dashboard-mono tabular-nums"
          style={{ color: 'var(--color-text-tertiary)', opacity: 0.7 }}
        >
          {String(threads.length).padStart(2, '0')}
        </span>
      </div>
      <div className="flex flex-col">
        {threads.map((thread) => {
          const wsId = (thread as { workspace_id?: string }).workspace_id;
          const ws = wsId ? workspaceMap.get(wsId) : undefined;
          return (
            <ThreadRow
              key={thread.thread_id}
              thread={thread}
              bucket={bucket}
              onOpen={() => onOpen(thread)}
              workspaceLabel={wsLabel(ws)}
              isHome={isHomeWorkspace(ws)}
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
  const label = useWorkspaceLabel();
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
    (wsListData?.workspaces ?? []).forEach((ws) => {
      if (ws.workspace_id) map.set(ws.workspace_id, ws);
    });
    return map;
  }, [wsListData]);

  const { data, isLoading } = useQuery(
    isAllScope
      ? {
          queryKey: queryKeys.threads.recent(limit),
          queryFn: () => getRecentThreads(limit, 0) as Promise<ThreadsResponse>,
          staleTime: 30_000,
        }
      : { ...workspaceThreadsQuery(targetWorkspaceId ?? '', limit), enabled: !!targetWorkspaceId },
  );

  const threads = useMemo(() => data?.threads ?? [], [data]);
  const now = useNow();

  const grouped = useMemo<GroupedThreads>(() => {
    const out: GroupedThreads = { today: [], week: [], older: [] };
    for (const thread of threads) {
      const d = thread.updated_at ? new Date(thread.updated_at) : null;
      if (!d || Number.isNaN(d.getTime())) {
        out.older.push(thread);
        continue;
      }
      out[bucketFor(d, now)].push(thread);
    }
    return out;
  }, [threads, now]);

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
          workspace: isHomeWorkspace(ws) ? label(ws) : ws?.name ?? '',
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
    <div className="flex flex-col h-full">
      <WidgetCardMeta>
        <span>{threads.length}</span>
        <button
          type="button"
          onClick={() => {
            if (viewAllTarget === '/chat') clearChatSession();
            navigate(viewAllTarget);
          }}
          className="dash-link widget-drag-cancel"
        >
          {t('dashboard.widgets.recentThreads.viewAll')}
          <ArrowUpRight className="h-3 w-3" />
        </button>
      </WidgetCardMeta>
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
                className="text-[0.6875rem]"
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
  cardTitleKey: 'dashboard.widgets.recentThreads.header',
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
