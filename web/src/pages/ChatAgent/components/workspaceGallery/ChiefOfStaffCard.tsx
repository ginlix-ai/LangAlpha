/**
 * The Chief of Staff, as the card above the workspaces. It is the agent that
 * coordinates the Analysts in every workspace rather than one of them, so it
 * takes a full row of its own: who it is and a way to start a thread on the
 * left, and its latest threads on the right. Before it has any, the right half
 * offers a few questions to start one with instead.
 *
 * Nothing here is a composer: a starter question opens a new thread with the
 * question in its composer, unsent, for the user to edit or send.
 *
 * Beside its actions, the computer it works on: the one Home is on, which
 * is the primary but for the moment a make-primary takes to move it.
 *
 * Its name, description and glyph come from `useWorkspaceText` and
 * `HomeRowIcon`, the one place Home is named, so this card never spells it.
 */
import { useEffect, useMemo } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';
import { ArrowUpRight, ChevronRight, Info, Plus } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';

import { SwapCell } from '@/components/ui/crossfade';
import { SkeletonBar } from '@/components/ui/skeleton';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { HomeRowIcon } from '@/hooks/useHomeWorkspace';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { useWorkspaceText } from '@/hooks/useWorkspaceLabel';
import { relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { Computer, Thread } from '@/types/api';

import { threadGalleryQuery } from '../../utils/threadGalleryQuery';
import { Kicker } from './Kicker';
import { MachineSummary } from './MachineSummary';
import type { WorkspaceRecord } from './types';

const RECENT_COUNT = 3;
const STARTERS = ['workspace.cosStarter1', 'workspace.cosStarter2', 'workspace.cosStarter3'] as const;
// What the card says of Home is the same for every user, so it is drawn
// before Home's row is read.
const HOME = { is_home: true } as const;

interface ChiefOfStaffCardProps {
  /** Home's row; null while it is read, when the card stands with its
   *  threads still to come and nothing to open yet. */
  workspace: WorkspaceRecord | null;
  now: number;
  /** Open the Chief of Staff's threads, as a workspace card opens its own. */
  onOpenThreads: (wsId: string, name?: string, status?: string) => void;
  prefetchThreads?: (wsId: string) => void;
  /** The machine it works on, once the computer list is in. */
  computer?: Computer | null;
  /** Whether that machine is the Mac this desktop app runs on. */
  isThisMac?: boolean;
  /** Open the Computers dialog on that machine. */
  onOpenComputer?: (computerId: string) => void;
}

export function ChiefOfStaffCard({
  workspace,
  now,
  onOpenThreads,
  prefetchThreads,
  computer = null,
  isThisMac = false,
  onOpenComputer,
}: ChiefOfStaffCardProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const isMobile = useIsMobile();
  const navigate = useNavigate();
  const text = useWorkspaceText()(workspace ?? HOME);
  const homeId = workspace?.workspace_id ?? '';

  // The Chief of Staff's threads as its thread list reads them, so following
  // "All threads" finds them already read.
  const { data, isPending, isError, hasNextPage, isFetchingNextPage, isFetchNextPageError, fetchNextPage } =
    useInfiniteQuery({ ...threadGalleryQuery(homeId, false), enabled: !!homeId });
  const rows = useMemo(() => (data?.pages.flatMap((page) => page.threads ?? []) ?? []) as Thread[], [data]);
  // The list puts pinned threads first, so a first page full of old pins can
  // hold back a newer thread. Read on until three unpinned threads are in:
  // every pin is read by then, and so are the newest of the rest.
  const readMore = hasNextPage && !isFetchNextPageError && rows.filter((th) => !th.is_pinned).length < RECENT_COUNT;
  useEffect(() => {
    if (readMore && !isFetchingNextPage) void fetchNextPage();
  }, [readMore, isFetchingNextPage, fetchNextPage]);
  const recent = useMemo(() => {
    // This one is by time alone.
    const at = (th: Thread) => new Date(th.updated_at ?? th.created_at ?? 0).getTime() || 0;
    return [...rows].sort((a, b) => at(b) - at(a)).slice(0, RECENT_COUNT);
  }, [rows]);
  // No threads, or a first read that failed, offers the starters rather than
  // an empty half. A refetch that fails keeps the threads it had: the query
  // reports an error then too, but its data still stands.
  const showStarters = !isPending && recent.length === 0;
  // A first read that failed has not shown there are no threads, so the
  // thread list stays a click away, where the read is tried again.
  const showAllThreads = !isPending && (!showStarters || (isError && !data));

  const route = { workspaceId: homeId, workspaceName: text.name, workspaceStatus: workspace?.status ?? null };
  const newThread = (draftMessage?: string) =>
    navigate('/chat/t/__default__', { state: draftMessage ? { ...route, draftMessage } : route });
  const openThread = (threadId: string) => navigate(`/chat/t/${threadId}`, { state: route });
  const openThreads = () => onOpenThreads(homeId, text.name, workspace?.status);

  // The right half's rows: ruled apart, each a quiet hover that reaches a
  // little past the text so the text keeps the column's edge. Beside the left
  // half they share its height, so the last row ends level with New thread;
  // stacked on a phone each keeps its own.
  const list = 'flex flex-1 flex-col divide-y divide-(--color-border-muted)';
  const item = 'flex flex-1';
  const row = '-mx-2 flex min-h-9 w-[calc(100%+1rem)] min-w-0 items-center gap-2.5 rounded-md px-2 text-left text-sm transition-colors hover:bg-foreground/5';
  const rowStyle = { color: 'var(--color-text-primary)' };

  return (
    <section
      data-testid="chief-of-staff"
      aria-label={text.name}
      onMouseEnter={!isMobile && homeId ? () => prefetchThreads?.(homeId) : undefined}
    >
      <div
        className="grid grid-cols-1 gap-5 rounded-xl px-5 py-5 md:grid-cols-[1fr_1.15fr] md:gap-8 md:px-[22px]"
        style={{
          // Solid, as every card is: the agent stands out by what it holds,
          // not by a tint or a glow.
          backgroundColor: 'var(--color-bg-card)',
          border: '1px solid var(--color-border-default)',
        }}
      >
        {/* Who it is, then the actions, held to the bottom so they sit level
            with the last row on the right. */}
        <div className="flex min-w-0 flex-col">
          <div className="flex items-center gap-3">
            <span
              className="grid h-10 w-10 shrink-0 place-items-center rounded-lg"
              style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-default)' }}
            >
              <HomeRowIcon className="h-5 w-5" style={{ color: 'var(--color-accent-primary)' }} aria-hidden="true" />
            </span>
            <h2 className="title-font text-[17px] font-semibold" style={{ color: 'var(--color-text-primary)' }}>
              {text.name}
            </h2>
          </div>
          <p className="mt-2.5 max-w-md text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
            {text.description}
          </p>
          <div className="mt-auto flex flex-wrap items-center gap-x-4 gap-y-3 pt-4">
            {/* Unavailable only for the moment Home is read. Its look holds,
                so the card does not change as Home lands; the cursor says wait. */}
            <button
              type="button"
              data-testid="chief-of-staff-new-thread"
              disabled={!homeId}
              onClick={() => newThread()}
              className="flex h-8 items-center gap-1.5 rounded-lg px-3.5 text-sm font-medium press hover:opacity-90 disabled:cursor-progress"
              style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
            >
              <Plus className="h-4 w-4 shrink-0" aria-hidden="true" />
              {t('nav.newThread')}
            </button>
            {/* Not until the threads are read: a new account's would only
                show and vanish as the starters arrive. */}
            {showAllThreads && (
              <button
                type="button"
                data-testid="chief-of-staff-threads"
                onClick={openThreads}
                className="group flex items-center gap-1 text-sm text-(--color-text-secondary) transition-colors hover:text-(--color-text-primary)"
              >
                {t('workspace.cosAllThreads')}
                <ChevronRight className="h-3.5 w-3.5 transition-transform duration-quick motion-safe:group-hover:translate-x-0.5" aria-hidden="true" />
              </button>
            )}
            {/* In the gap beside the actions; under them where the row runs out. */}
            {computer && <WorksOn name={text.name} computer={computer} isThisMac={isThisMac} onOpen={onOpenComputer} />}
          </div>
        </div>

        {/* Its threads, or the starters when it has none, take the place of
            three placeholder rows in place, so the card is the same height
            before and after they are read. */}
        <SwapCell
          as="div"
          swapKey={isPending ? 'pending' : showStarters ? 'starters' : 'recent'}
          className="min-w-0"
          layerClassName="flex min-w-0 flex-col"
        >
          {isPending ? (
            <>
              <Kicker className="mb-1"><SkeletonBar className="w-14" /></Kicker>
              <ul className={cn('min-h-[108px]', list)} aria-busy="true">
                {[62, 48, 55].map((width) => (
                  <li key={width} className={item}>
                    <div className="flex min-h-9 w-full items-center gap-2.5 text-sm">
                      <span className="min-w-0 flex-1"><SkeletonBar style={{ width: `${width}%` }} /></span>
                      <SkeletonBar className="w-10 text-xs" />
                    </div>
                  </li>
                ))}
              </ul>
            </>
          ) : showStarters ? (
            <>
              <Kicker className="mb-1">{t('workspace.cosTryAsking')}</Kicker>
              <ul className={list}>
                {STARTERS.map((key) => (
                  <li key={key} className={item}>
                    <button
                      type="button"
                      data-testid="chief-of-staff-starter"
                      onClick={() => newThread(t(key))}
                      className={cn('group', row)}
                      style={rowStyle}
                    >
                      <ChevronRight className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} aria-hidden="true" />
                      {/* A question cut short says nothing, so it wraps where a title truncates. */}
                      <span className="min-w-0 flex-1 py-2 leading-snug">{t(key)}</span>
                      <ArrowUpRight
                        className="h-3.5 w-3.5 shrink-0 transition-transform duration-quick motion-safe:group-hover:-translate-y-px motion-safe:group-hover:translate-x-px"
                        style={{ color: 'var(--color-text-tertiary)' }}
                        aria-hidden="true"
                      />
                    </button>
                  </li>
                ))}
              </ul>
            </>
          ) : (
            <>
              <Kicker className="mb-1">{t('workspace.cosRecent')}</Kicker>
              {/* Held at three rows' height, so fewer threads leave the card
                  the height it had while they were read. */}
              <ul className={cn('min-h-[108px]', list)}>
                {recent.map((thread) => {
                  const when = relativeTime(thread.updated_at ?? thread.created_at, locale, now);
                  return (
                    <li key={thread.thread_id} className={item}>
                      <button
                        type="button"
                        data-testid="chief-of-staff-thread"
                        onClick={() => openThread(thread.thread_id)}
                        className={row}
                        style={rowStyle}
                      >
                        <span className="min-w-0 flex-1 truncate">{thread.title || t('nav.untitledThread')}</span>
                        {when && (
                          <span className="shrink-0 whitespace-nowrap text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                            {when}
                          </span>
                        )}
                      </button>
                    </li>
                  );
                })}
              </ul>
            </>
          )}
        </SwapCell>
      </div>
    </section>
  );
}

/** The computer line, and why it is that one. */
function WorksOn({ name, computer, isThisMac, onOpen }: { name: string; computer: Computer; isThisMac: boolean; onOpen?: (computerId: string) => void }) {
  const { t } = useTranslation();
  const why = t('workspace.cosComputerHint', {
    name,
    defaultValue: '{{name}} works on your primary computer, in a Home folder beside your workspaces. Make another computer primary and it moves there.',
  });
  return (
    <div className="flex min-w-0 items-center gap-1.5 text-xs" data-testid="chief-of-staff-computer" style={{ color: 'var(--color-text-tertiary)' }}>
      <span className="shrink-0">{t('workspace.cosWorksOn', 'Works on')}</span>
      <div className="min-w-0">
        <MachineSummary computer={computer} isThisMac={isThisMac} onOpen={onOpen} compact />
      </div>
      <TooltipProvider delayDuration={200}>
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              aria-label={why}
              className="-m-1 grid shrink-0 place-items-center rounded p-1 transition-colors hover:text-(--color-text-secondary)"
            >
              <Info className="h-3 w-3" aria-hidden="true" />
            </button>
          </TooltipTrigger>
          <TooltipContent side="top" className="max-w-72 leading-relaxed">
            {why}
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </div>
  );
}

export default ChiefOfStaffCard;
