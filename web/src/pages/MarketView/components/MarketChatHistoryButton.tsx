import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import {
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Loader2,
  Plus,
  AlertCircle,
  Info,
  MessageSquare,
} from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { queryKeys } from '@/lib/queryKeys';
import { getWorkspaceThreads } from '../../ChatAgent/utils/api';

interface ThreadRecord {
  thread_id: string;
  title?: string;
  updated_at?: string;
  first_query_content?: string;
  [key: string]: unknown;
}

interface ThreadsResponse {
  threads?: ThreadRecord[];
  total?: number;
}

interface MarketChatHistoryButtonProps {
  workspaceId: string | null;
  activeThreadId: string | null;
  activeTitle: string;
  onSelectThread: (threadId: string) => void;
  onStartNewChat: () => void;
}

function formatRelative(iso: string | undefined): string {
  if (!iso) return '';
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return '';
  const diffMs = Date.now() - then;
  const minutes = Math.floor(diffMs / 60_000);
  if (minutes < 1) return 'now';
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d`;
  if (days < 30) return `${Math.floor(days / 7)}w`;
  return new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function threadLabel(thread: ThreadRecord, untitled: string): string {
  if (thread.title && thread.title.trim()) return thread.title;
  if (thread.first_query_content && thread.first_query_content.trim()) {
    const trimmed = thread.first_query_content.trim();
    return trimmed.length > 60 ? `${trimmed.slice(0, 60)}…` : trimmed;
  }
  return untitled;
}

export default function MarketChatHistoryButton({
  workspaceId,
  activeThreadId,
  activeTitle,
  onSelectThread,
  onStartNewChat,
}: MarketChatHistoryButtonProps): React.ReactElement {
  const { t } = useTranslation();
  const [open, setOpen] = React.useState(false);
  const [page, setPage] = React.useState(0);

  const PLATFORM_PREFIX = 'market_view';
  const PAGE_SIZE = 5;

  React.useEffect(() => {
    if (!open) setPage(0);
  }, [open]);

  const { data, isFetching, error } = useQuery({
    queryKey: workspaceId
      ? [
          ...queryKeys.threads.byWorkspace(workspaceId),
          { platform: PLATFORM_PREFIX, page, pageSize: PAGE_SIZE },
        ]
      : ['threads', 'inactive'],
    queryFn: () =>
      getWorkspaceThreads(workspaceId!, PAGE_SIZE, page * PAGE_SIZE, PLATFORM_PREFIX),
    enabled: !!workspaceId && open,
    staleTime: 30_000,
    retry: (failureCount, err) => {
      const status = (err as { response?: { status?: number } } | null)?.response?.status;
      if (status === 403 || status === 404) return false;
      return failureCount < 2;
    },
  });

  const response = data as ThreadsResponse | undefined;
  const threads = response?.threads ?? [];
  const total = response?.total ?? 0;
  const totalPages = total > 0 ? Math.max(1, Math.ceil(total / PAGE_SIZE)) : 1;
  const hasPagination = total > PAGE_SIZE;
  const hasActive = !!activeThreadId;

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          aria-label={t('marketView.chatHistory.triggerLabel')}
          className="group inline-flex items-center gap-2 rounded-md px-2 py-1 max-w-[260px] transition-colors hover:bg-foreground/[0.06]"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {/* Session status dot — accent when there's an active thread, dim otherwise */}
          <span
            aria-hidden
            className="h-[5px] w-[5px] rounded-full flex-shrink-0 transition-colors"
            style={{
              backgroundColor: hasActive
                ? 'var(--color-accent-primary)'
                : 'var(--color-text-tertiary)',
              opacity: hasActive ? 1 : 0.35,
              boxShadow: hasActive
                ? '0 0 0 2px color-mix(in srgb, var(--color-accent-primary) 22%, transparent)'
                : 'none',
            }}
          />
          <span
            className="text-sm font-medium truncate"
            style={{ minWidth: 0, letterSpacing: '-0.005em' }}
          >
            {activeTitle}
          </span>
          <ChevronDown
            className="h-3 w-3 flex-shrink-0 transition-transform duration-200 group-data-[state=open]:rotate-180"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
        </button>
      </DropdownMenuTrigger>

      <DropdownMenuContent
        align="start"
        side="bottom"
        sideOffset={8}
        className="w-80 p-0 overflow-hidden"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
          boxShadow:
            '0 1px 2px rgba(0,0,0,0.04), 0 8px 24px -8px rgba(0,0,0,0.35)',
        }}
      >
        {/* Primary action — flush, full-width, accent icon */}
        <DropdownMenuItem
          onSelect={(e) => {
            e.preventDefault();
            onStartNewChat();
            setOpen(false);
          }}
          className="flex items-center gap-2.5 px-3 py-2.5 cursor-pointer focus:bg-foreground/[0.06] data-[highlighted]:bg-foreground/[0.06]"
          style={{ color: 'var(--color-text-primary)', borderRadius: 0 }}
        >
          <Plus
            className="h-3.5 w-3.5 flex-shrink-0"
            style={{ color: 'var(--color-accent-primary)' }}
          />
          <span className="text-sm font-medium" style={{ letterSpacing: '-0.005em' }}>
            {t('marketView.chatHistory.newChat')}
          </span>
        </DropdownMenuItem>

        {/* Eyebrow label with hairline */}
        <div className="flex items-center gap-2 px-3 pt-3 pb-1.5">
          <span
            className="text-[10px] font-semibold uppercase"
            style={{
              color: 'var(--color-text-tertiary)',
              letterSpacing: '0.14em',
            }}
          >
            {t('marketView.chatHistory.recent')}
          </span>
          <div
            className="flex-1 h-px"
            style={{ backgroundColor: 'var(--color-border-muted)' }}
          />
          {total > 0 && (
            <span
              className="text-[10px] tabular-nums"
              style={{
                color: 'var(--color-text-tertiary)',
                fontVariantNumeric: 'tabular-nums',
                letterSpacing: '0.04em',
              }}
            >
              {total}
            </span>
          )}
          <span className="relative inline-flex items-center group/hint">
            <button
              type="button"
              aria-label={t('marketView.chatHistory.missingThreadHint')}
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
              }}
              className="inline-flex items-center justify-center h-4 w-4 rounded-full transition-opacity opacity-60 hover:opacity-100 focus:outline-none focus-visible:ring-1 focus-visible:ring-foreground/30"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <Info className="h-3 w-3" />
            </button>
            <span
              role="tooltip"
              className="pointer-events-none absolute right-0 top-full mt-1.5 w-60 rounded-md px-2.5 py-2 text-[11px] leading-snug opacity-0 translate-y-[-2px] transition-all duration-150 group-hover/hint:opacity-100 group-hover/hint:translate-y-0 group-focus-within/hint:opacity-100 group-focus-within/hint:translate-y-0"
              style={{
                backgroundColor: 'var(--color-bg-elevated)',
                color: 'var(--color-text-secondary)',
                border: '1px solid var(--color-border-muted)',
                boxShadow:
                  '0 1px 2px rgba(0,0,0,0.06), 0 8px 20px -8px rgba(0,0,0,0.35)',
                zIndex: 60,
                letterSpacing: '-0.005em',
              }}
            >
              {t('marketView.chatHistory.missingThreadHint')}
            </span>
          </span>
        </div>

        {/* Scrollable thread list — flush rows, no boxy active card */}
        <div className="overflow-y-auto" style={{ maxHeight: '50vh' }}>
          {!workspaceId && (
            <div
              className="px-3 py-6 text-xs text-center"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('marketView.chatHistory.noWorkspace')}
            </div>
          )}

          {workspaceId && isFetching && threads.length === 0 && (
            <div
              className="flex items-center justify-center gap-2 px-3 py-8 text-xs"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <Loader2 className="h-3 w-3 animate-spin" />
              <span style={{ letterSpacing: '0.04em' }}>{t('marketView.chatHistory.loading')}</span>
            </div>
          )}

          {workspaceId && error && (
            <div
              className="flex items-start gap-2 px-3 py-4 text-xs"
              style={{ color: 'var(--color-loss)' }}
            >
              <AlertCircle className="h-3 w-3 mt-0.5 flex-shrink-0" />
              <span>{t('marketView.chatHistory.loadError')}</span>
            </div>
          )}

          {workspaceId && !isFetching && !error && threads.length === 0 && page === 0 && (
            <div
              className="flex flex-col items-center gap-2 px-3 py-10 text-xs"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <MessageSquare className="h-4 w-4" style={{ opacity: 0.45 }} />
              <span>{t('marketView.chatHistory.empty')}</span>
            </div>
          )}

          {workspaceId && !isFetching && !error && threads.length === 0 && page > 0 && (
            <div
              className="flex flex-col items-center gap-2 px-3 py-8 text-xs"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <span>{t('marketView.chatHistory.emptyPage')}</span>
            </div>
          )}

          {threads.map((thread) => {
            const isActive = thread.thread_id === activeThreadId;
            return (
              <DropdownMenuItem
                key={thread.thread_id}
                onSelect={(e) => {
                  e.preventDefault();
                  onSelectThread(thread.thread_id);
                  setOpen(false);
                }}
                className="group/row relative flex items-start gap-3 pl-4 pr-3 py-2 cursor-pointer focus:bg-foreground/[0.05] data-[highlighted]:bg-foreground/[0.05]"
                style={{ borderRadius: 0 }}
              >
                {/* Active accent bar — 2px, full-height, replaces the boxy card */}
                <span
                  aria-hidden
                  className="absolute left-0 top-0 bottom-0 w-[2px] transition-opacity"
                  style={{
                    backgroundColor: 'var(--color-accent-primary)',
                    opacity: isActive ? 1 : 0,
                  }}
                />
                <span
                  className="flex-1 text-sm truncate"
                  style={{
                    color: 'var(--color-text-primary)',
                    fontWeight: isActive ? 600 : 400,
                    letterSpacing: '-0.005em',
                    opacity: isActive ? 1 : 0.88,
                  }}
                >
                  {threadLabel(thread, t('marketView.chatHistory.untitled'))}
                </span>
                {thread.updated_at && (
                  <span
                    className="text-[11px] flex-shrink-0 mt-[1px]"
                    style={{
                      color: 'var(--color-text-tertiary)',
                      fontVariantNumeric: 'tabular-nums',
                      letterSpacing: '0.02em',
                    }}
                  >
                    {formatRelative(thread.updated_at)}
                  </span>
                )}
              </DropdownMenuItem>
            );
          })}
        </div>

        {hasPagination && (
          <div
            className="flex items-center justify-between px-3 py-2"
            style={{
              borderTop: '1px solid var(--color-border-muted)',
              backgroundColor: 'var(--color-bg-elevated)',
            }}
          >
            <button
              type="button"
              aria-label={t('marketView.chatHistory.prevPage')}
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setPage((p) => Math.max(0, p - 1));
              }}
              disabled={page === 0 || isFetching}
              className="inline-flex items-center justify-center h-6 w-6 rounded transition-colors hover:bg-foreground/[0.06] disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </button>

            <span
              className="text-[11px] tabular-nums select-none"
              style={{
                color: 'var(--color-text-tertiary)',
                fontVariantNumeric: 'tabular-nums',
                letterSpacing: '0.04em',
              }}
            >
              {page + 1} / {totalPages}
            </span>

            <button
              type="button"
              aria-label={t('marketView.chatHistory.nextPage')}
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setPage((p) => Math.min(totalPages - 1, p + 1));
              }}
              disabled={page >= totalPages - 1 || isFetching}
              className="inline-flex items-center justify-center h-6 w-6 rounded transition-colors hover:bg-foreground/[0.06] disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </button>
          </div>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
