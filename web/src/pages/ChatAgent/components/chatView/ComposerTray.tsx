import { useId, useLayoutEffect, useRef, useState, type ReactNode, type Ref, type RefObject } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight, ChevronUp, Folder } from 'lucide-react';
import { motion, type Easing } from '@/lib/framer';
import { Popover, PopoverClose, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import PulseDot from '@/components/ui/pulse-dot';
import { useLocale } from '@/hooks/useLocale';
import { mediumTime } from '@/lib/format';
import { useNow } from '@/hooks/useNow';
import { DURATION } from '@/lib/motion';
import { cn } from '@/lib/utils';
import { DispatchStatusGlyph, DispatchStatusIndicator } from '../dispatchStatusUi';
import { RunGlyph } from '../messageList/runCard';
import type { PTCDispatchStatus } from '../../hooks/usePTCDispatchStatus';
import { useOpenDispatchedThread, type DispatcherContext } from '../../hooks/useOpenDispatchedThread';
import type { CommandRun } from '@/types/commands';
import type { WakeupList } from '@/types/wakeups';
import type { MarketWatchState } from '../../session/marketWatchEvents';
import { isTerminalStatus, sidebarAgentRowLabel, type SubagentTerminalStatus } from '../../session/subagents/subagentStatus';
import { chipLabel } from '../wakeupLabels';
import type { RosterTask } from './delegatedWork';
import type { RosterAnalyst } from './useAnalystRoster';
import { SHAPE_RUN, SHAPE_STAGGER, liquidRunMs, type TrayFoldPhase } from './useTrayFold';
import { trayChipCount, trayWorking, type TrayContents, type TrayTasks } from './useTrayContents';
import { WakeupListRows } from '../wakeups/WakeupList';
import { CommandListRows } from '../commands/CommandListRows';
import { TRAY_ROW } from './trayRow';

// Solid, not a translucent tint: the transcript scrolls under the tray. The
// liquid and the dock are the chips' own fill, so a chip, its shape and the
// drop it becomes read as one body.
const TRAY_FILL = 'bg-(--color-bg-tag)';
const CHIP = cn('inline-flex max-w-full items-center gap-2 rounded-full px-3 py-1 text-xs text-(--color-text-tertiary)', TRAY_FILL);
const CHIP_BUTTON = cn(
  CHIP,
  'cursor-pointer outline-hidden transition-colors hover:bg-(--color-bg-elevated) hover:text-(--color-text-secondary) focus-visible:ring-2 focus-visible:ring-ring',
  'data-[state=open]:bg-(--color-bg-elevated) data-[state=open]:text-(--color-text-primary)',
);

interface ComposerTrayProps {
  contents: TrayContents;
  phase: TrayFoldPhase;
  /** The tray's own row, which the fold measures. */
  trayRef: RefObject<HTMLDivElement | null>;
  /** The dock in the composer's toolbar that the chips fold into. */
  dockRef: RefObject<HTMLElement | null>;
  /** This thread, which an Analyst's thread opened from here links back to. */
  from: DispatcherContext | null;
  onOpenTask: (agentId: string) => void;
  /** Opens the market watch's live prices. */
  onOpenWatch: () => void;
  /** Opens a background command's tool card, by the call it started from. */
  onOpenCommand: (toolCallId: string) => void;
  /** The thread the wakeups chip cancels in. */
  threadId: string | null;
}

/**
 * The tray's chips, above the composer. Each chip that names more than one
 * thing opens a list whose rows jump to it; a chip that names one jumps
 * straight there, unless its list is where a control lives (a wakeup's
 * Cancel, a command's Stop). The tray stays while a turn runs: what it lists outlives the
 * turn, and work handed off stays a few turns after it went quiet.
 *
 * While the reader is up in the history the chips fold into the composer, and
 * they run there as liquid: their shapes flow together into one drop that
 * falls into the composer's toolbar and becomes the dock, which pours them
 * back out the same way.
 */
export function ComposerTray({
  contents, phase, trayRef, dockRef, from, onOpenTask, onOpenWatch, onOpenCommand, threadId,
}: ComposerTrayProps) {
  const chipsRef = useRef<HTMLDivElement>(null);
  const [run, setRun] = useState<LiquidRun | null>(null);
  const liquid = phase === 'folding' || phase === 'unfolding';

  // Measured once the phase is in the DOM and before it paints, so the first
  // frame that drops the chips' fill already has their shapes under them.
  useLayoutEffect(() => {
    if (!liquid) return;
    const geometry = measureRun(trayRef.current, chipsRef.current, dockRef.current);
    setRun(geometry && { phase, ...geometry });
  }, [liquid, phase, trayRef, dockRef]);

  if (phase === 'folded') return null;
  const { watch, wakeups, commands, tasks, analysts, reportBackLabel } = contents;
  // Unfolding, the chips are back in full as the last shape lands, when the
  // liquid under them goes.
  const unfoldFadeDelay = liquidRunMs(trayChipCount(contents)) / 1000 - DURATION.quick;

  return (
    // Inset as the to-do tab is, so the chips line up with it over the composer.
    <div ref={trayRef} className="relative px-4">
      {liquid && run?.phase === phase && <LiquidLayer key={run.phase} run={run} />}
      <motion.div
        ref={chipsRef}
        inert={liquid}
        // Over the liquid, which starts and ends in their shapes and fill: they
        // fade out from over it and back in onto it. Their own fills stay put,
        // since a chip's color transition would blink one in late.
        className={cn('flex flex-wrap items-center gap-1.5', liquid && 'relative z-30')}
        initial={{ opacity: phase === 'unfolding' ? 0 : 1 }}
        animate={{ opacity: phase === 'folding' ? 0 : 1 }}
        transition={phase === 'unfolding'
          ? { delay: unfoldFadeDelay, duration: DURATION.quick }
          : { duration: DURATION.exit }}
      >
        {watch && <WatchChip watch={watch} onOpen={onOpenWatch} />}
        {wakeups && <WakeupsChip list={wakeups} threadId={threadId} />}
        {commands && <CommandsChip rows={commands} onOpen={onOpenCommand} />}
        {tasks && <TasksChip {...tasks} onOpen={onOpenTask} />}
        {analysts && <AnalystsChip threads={analysts} from={from} />}
        {reportBackLabel && <StatusChip label={reportBackLabel} />}
      </motion.div>
    </div>
  );
}

interface Box { x: number; y: number; w: number; h: number }

interface LiquidRun {
  phase: TrayFoldPhase;
  /** The layer's box in the tray row; everything else is in the layer. */
  box: Box;
  chips: Box[];
  dock: Box;
}

/** Room around every shape for the blur that joins them. */
const BLUR_ROOM = 24;

function measureRun(
  row: HTMLElement | null,
  chips: HTMLElement | null,
  dock: HTMLElement | null,
): Omit<LiquidRun, 'phase'> | null {
  if (!row || !chips || !dock) return null;
  const rects = [...chips.children].map((el) => el.getBoundingClientRect());
  const dockRect = dock.getBoundingClientRect();
  if (rects.length === 0 || dockRect.width === 0) return null;
  const all = [...rects, dockRect];
  const left = Math.min(...all.map((r) => r.left)) - BLUR_ROOM;
  const top = Math.min(...all.map((r) => r.top)) - BLUR_ROOM;
  const right = Math.max(...all.map((r) => r.right)) + BLUR_ROOM;
  const bottom = Math.max(...all.map((r) => r.bottom)) + BLUR_ROOM;
  const origin = row.getBoundingClientRect();
  const local = (r: DOMRect): Box => ({ x: r.left - left, y: r.top - top, w: r.width, h: r.height });
  return {
    box: { x: left - origin.left, y: top - origin.top, w: right - left, h: bottom - top },
    chips: rects.map(local),
    dock: local(dockRect),
  };
}

/**
 * The chips' fills, drawn through a goo filter: a blur, then an alpha cut
 * that turns the blur's soft edge back into a hard one. Two shapes whose blurs
 * overlap cross the cut together, so they read as one body of liquid, joined
 * by a neck that thins and snaps as they part. Above the composer, so the drop
 * can fall into its toolbar: it lands in the dock's own shape, and the dock
 * takes its place without a seam.
 */
function LiquidLayer({ run }: { run: LiquidRun }) {
  // React's ids are word characters only (`_r_1_`), so one is valid in
  // url(#…) as it is.
  const filterId = `tray-goo-${useId()}`;
  const { box, chips, dock, phase } = run;
  // Nearest the dock first: the pills beside it reach it first and the rest
  // run into them, so they merge one by one on the way, and split that way.
  const dockX = dock.x + dock.w / 2;
  const rank: number[] = [];
  chips
    .map((chip, i) => ({ i, distance: Math.abs(chip.x + chip.w / 2 - dockX) }))
    .sort((a, b) => a.distance - b.distance)
    .forEach(({ i }, place) => {
      rank[i] = place;
    });

  return (
    <div
      aria-hidden="true"
      className="pointer-events-none absolute z-20"
      style={{ left: box.x, top: box.y, width: box.w, height: box.h }}
    >
      <svg className="absolute h-0 w-0">
        <filter id={filterId} colorInterpolationFilters="sRGB">
          <feGaussianBlur stdDeviation="5" />
          <feColorMatrix values="1 0 0 0 0  0 1 0 0 0  0 0 1 0 0  0 0 0 18 -7" />
        </filter>
      </svg>
      <div className="absolute inset-0" style={{ filter: `url(#${filterId})` }}>
        {chips.map((chip, i) => (
          <LiquidShape key={i} chip={chip} dock={dock} unfolding={phase === 'unfolding'} delay={rank[i] * SHAPE_STAGGER} />
        ))}
      </div>
    </div>
  );
}

interface ShapeFrame { cx: number; cy: number; w: number; h: number }

/** The goo cut sits outside a shape's edge; this much in, it matches the dock. */
const CUT_SPREAD = 1;

function LiquidShape({ chip, dock, unfolding, delay }: { chip: Box; dock: Box; unfolding: boolean; delay: number }) {
  const cx = chip.x + chip.w / 2;
  const cy = chip.y + chip.h / 2;
  const dx = dock.x + dock.w / 2;
  const dy = dock.y + dock.h / 2;
  const ballY = cy + chip.h * 0.4;
  // The pill, narrowing as it runs toward the dock's column; a ball there,
  // just under the tray; a drop drawn long by its fall; the dock.
  const pill: ShapeFrame = { cx, cy, w: chip.w, h: chip.h };
  const running: ShapeFrame = { cx: cx + (dx - cx) * 0.8, cy: cy + 1, w: chip.h + (chip.w - chip.h) * 0.25, h: chip.h };
  const ball: ShapeFrame = { cx: dx, cy: ballY, w: chip.h * 0.92, h: chip.h * 0.92 };
  const falling: ShapeFrame = { cx: dx, cy: (ballY + dy) / 2, w: chip.h * 0.7, h: chip.h * 1.2 };
  const landed: ShapeFrame = { cx: dx, cy: dy, w: dock.w - CUT_SPREAD * 2, h: dock.h - CUT_SPREAD * 2 };
  const frames = unfolding ? [landed, falling, ball, running, pill] : [pill, running, ball, falling, landed];
  // Folding: slow off the tray, faster as it falls, and a splash as it lands.
  // Unfolding: drawn up out of the dock, and the last stretch overshoots a
  // little and settles.
  const times = unfolding ? [0, 0.24, 0.48, 0.64, 1] : [0, 0.34, 0.5, 0.74, 1];
  const ease: Easing[] = unfolding
    ? ['easeIn', 'easeOut', 'easeIn', [0.3, 1.35, 0.5, 1]]
    : ['easeInOut', 'easeOut', 'easeIn', [0.3, 1.4, 0.5, 1]];
  const at = (frame: ShapeFrame) => ({ left: frame.cx - frame.w / 2, top: frame.cy - frame.h / 2, width: frame.w, height: frame.h });

  return (
    <motion.span
      className={cn('absolute rounded-full', TRAY_FILL)}
      initial={at(frames[0])}
      animate={{
        left: frames.map((f) => at(f).left),
        top: frames.map((f) => at(f).top),
        width: frames.map((f) => f.w),
        height: frames.map((f) => f.h),
      }}
      transition={{ duration: SHAPE_RUN, times, ease, delay }}
    />
  );
}

interface TrayDockProps {
  ref?: Ref<HTMLButtonElement>;
  phase: TrayFoldPhase;
  contents: TrayContents;
  onOpen: () => void;
}

/**
 * The folded tray, in the composer's toolbar: the drop the chips ran into,
 * which pours them back out when clicked. In the layout but unseen while the
 * liquid runs, so the drop has a place to land and to rise from; the swap is
 * a single frame, since the drop ends in the dock's shape and fill.
 */
export function TrayDock({ ref, phase, contents, onOpen }: TrayDockProps) {
  const { t } = useTranslation();
  const label = t('chat.tray.show');
  const folded = phase === 'folded';
  const working = trayWorking(contents);
  return (
    <button
      ref={ref}
      type="button"
      aria-label={label}
      title={label}
      inert={!folded}
      onClick={onOpen}
      className={cn(
        phase === 'open' ? 'hidden' : 'inline-flex',
        'h-6 flex-none items-center gap-1.5 rounded-full px-2 text-(--color-text-tertiary) outline-hidden transition-colors',
        TRAY_FILL,
        'hover:text-(--color-text-secondary) focus-visible:ring-2 focus-visible:ring-ring',
        !folded && 'opacity-0',
      )}
    >
      {/* What it holds surfaces once the drop has settled. */}
      <span
        className={cn(
          'inline-flex items-center gap-1.5',
          folded ? 'opacity-100 transition-opacity duration-quick motion-reduce:transition-none' : 'opacity-0',
        )}
      >
        {contents.watch && <PulseDot />}
        {working && <RunGlyph kind="live" />}
        <ChevronUp aria-hidden="true" className="h-3 w-3 shrink-0" />
      </span>
    </button>
  );
}

/**
 * The tickers the agent is live-stamping, opening their live prices. The label
 * is the polite live region, not the button: a live region should not be the
 * control itself.
 */
function WatchChip({ watch, onOpen }: { watch: MarketWatchState; onOpen: () => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const title = watch.timestamp
    ? t('chat.marketWatch.chipTitleUpdated', { time: mediumTime(watch.timestamp * 1000, locale) })
    : t('chat.marketWatch.chipTitle');
  return (
    <button type="button" className={CHIP_BUTTON} title={title} aria-label={t('chat.marketWatch.chipAction')} onClick={onOpen}>
      <PulseDot />
      <span role="status" aria-live="polite" className="tabular-nums">
        {t('chat.marketWatch.watching', { symbols: watch.symbols.join(', ') })}
      </span>
    </button>
  );
}

/**
 * The wakeups the agent set in this thread: how many are pending and when the
 * next fires, opening a list where each can be cancelled. Not a status line,
 * so no live region: it changes when the agent sets or a wakeup fires.
 */
function WakeupsChip({ list, threadId }: { list: WakeupList; threadId: string | null }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  return (
    <ListChip
      label={chipLabel(list.items, t, locale, now, list.userTimezone)}
      heading={t('chat.wakeups.panelTitle')}
      title={t('chat.wakeups.chipTitle')}
      working={false}
    >
      <WakeupListRows threadId={threadId} wakeups={list.items} now={now} userTimezone={list.userTimezone} />
    </ListChip>
  );
}

/**
 * A reconnect in progress, floated above the composer stack at the tray's
 * inset rather than mounted in it: a reconnect that catches up takes it away
 * again, and a row leaving the flow would drop everything under it.
 */
export function ReconnectingChip() {
  const { t } = useTranslation();
  // The gap is a translate: the stack's sibling gap zeroes a bottom margin.
  return (
    <div className={cn(CHIP, 'absolute bottom-full left-4 -translate-y-2')} role="status" aria-live="polite">
      <RunGlyph kind="live" />
      {t('chat.reconnecting', 'Reconnecting…')}
    </div>
  );
}

function StatusChip({ label }: { label: string }) {
  return (
    <div className={CHIP} role="status" aria-live="polite">
      <RunGlyph kind="live" />
      <span className="truncate">{label}</span>
    </div>
  );
}

/**
 * A chip that opens a list, whose rows close it as they open what they name.
 * A chip whose lone row would only jump somewhere mounts it only while it
 * stands for more than one thing, so the open state goes with it: a list
 * that shrinks to one while open does not spring back open when a second
 * arrives. It spins while any of them is working.
 */
function ListChip({ label, heading, title, working, children }: { label: string; heading: string; title?: string; working: boolean; children: ReactNode }) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button type="button" className={cn(CHIP_BUTTON, 'group')} title={title}>
          {working && <RunGlyph kind="live" />}
          <span>{label}</span>
          <ChevronUp aria-hidden="true" className="h-3 w-3 shrink-0 transition-transform group-data-[state=open]:rotate-180" />
        </button>
      </PopoverTrigger>
      <TrayList heading={heading}>{children}</TrayList>
    </Popover>
  );
}

/** A settled task in the Analysts' glyphs, so the two lists read alike. */
const SETTLED_TASK_LOOK: Record<SubagentTerminalStatus, PTCDispatchStatus> = {
  completed: 'completed',
  error: 'failed',
  cancelled: 'stopped',
};

/**
 * A roster task's glyph: the spinner only while it runs. A status alone can
 * read live after the run ended unseen, so a stopped task without a final
 * status shows none.
 */
function TaskGlyph({ row }: { row: RosterTask }) {
  if (row.running) return <DispatchStatusGlyph status="running" />;
  return isTerminalStatus(row.status) ? <DispatchStatusGlyph status={SETTLED_TASK_LOOK[row.status]} /> : null;
}

function TasksChip({ rows, unnamedRunning, onOpen }: TrayTasks & { onOpen: (agentId: string) => void }) {
  const { t } = useTranslation();

  // Running, but nothing the roster names.
  if (rows.length === 0) return <StatusChip label={t('chat.backgroundTasksRunning')} />;

  // A lone row is the chip, unless the chip also spins for a task it cannot name.
  if (rows.length === 1 && !unnamedRunning) {
    const [row] = rows;
    const label = sidebarAgentRowLabel(row);
    return (
      <button type="button" className={CHIP_BUTTON} title={label} onClick={() => onOpen(row.id)}>
        <TaskGlyph row={row} />
        <span className="max-w-64 truncate">{label}</span>
        <ArrowUpRight aria-hidden="true" className="h-3 w-3 shrink-0" />
      </button>
    );
  }

  const running = rows.filter((row) => row.running).length;
  return (
    <ListChip
      label={running > 0 ? t('chat.tray.tasksRunning', { count: rows.length, running }) : t('chat.tray.tasks', { count: rows.length })}
      heading={t('chat.tray.tasksHeading')}
      working={running > 0 || unnamedRunning}
    >
      {rows.map((row) => {
        const label = sidebarAgentRowLabel(row);
        return (
          <li key={row.id}>
            <PopoverClose asChild>
              <button type="button" className={TRAY_ROW} onClick={() => onOpen(row.id)}>
                <span className="flex w-3.5 shrink-0 justify-center">
                  <TaskGlyph row={row} />
                </span>
                <span className="flex min-w-0 flex-1 flex-col gap-0.5">
                  <span className="truncate text-[0.8125rem]" style={{ color: 'var(--color-text-primary)' }} title={label}>
                    {label}
                  </span>
                  {label !== row.name && (
                    <span className="truncate text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{row.name}</span>
                  )}
                </span>
                <ArrowUpRight aria-hidden="true" className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-quaternary)' }} />
              </button>
            </PopoverClose>
          </li>
        );
      })}
    </ListChip>
  );
}

/** The commands still running in the background, as a list even for one,
 *  since the Stop lives there. */
function CommandsChip({ rows, onOpen }: { rows: CommandRun[]; onOpen: (toolCallId: string) => void }) {
  const { t } = useTranslation();
  return (
    <ListChip label={t('chat.commands.chip', { count: rows.length })} heading={t('chat.tray.commandsHeading')} working>
      <CommandListRows rows={rows} onOpen={onOpen} />
    </ListChip>
  );
}

function AnalystsChip({ threads, from }: { threads: RosterAnalyst[]; from: DispatcherContext | null }) {
  const { t } = useTranslation();
  const openDispatchedThread = useOpenDispatchedThread(from);
  const openThread = (thread: RosterAnalyst) => openDispatchedThread(thread.threadId, thread.workspaceId);

  if (threads.length === 1) {
    const [thread] = threads;
    const name = thread.workspaceName || thread.title;
    const label = thread.running
      ? t('agents.tray.waitingOn', { name: name ?? t('agents.tray.analysts', { count: 1 }) })
      : (name ?? t('agents.tray.analysts', { count: 1 }));
    return (
      <button type="button" className={CHIP_BUTTON} title={thread.title ?? undefined} onClick={() => openThread(thread)}>
        {thread.running ? <RunGlyph kind="live" /> : thread.status && <DispatchStatusGlyph status={thread.status} />}
        <span className="max-w-64 truncate">{label}</span>
        <ArrowUpRight aria-hidden="true" className="h-3 w-3 shrink-0" />
      </button>
    );
  }

  const running = threads.filter((thread) => thread.running).length;
  return (
    <ListChip
      label={running > 0 ? t('agents.tray.analystsRunning', { count: threads.length, running }) : t('agents.tray.analysts', { count: threads.length })}
      heading={t('agents.tray.analystsHeading')}
      working={running > 0}
    >
      {threads.map((thread) => (
        <li key={thread.threadId}>
          <AnalystRow thread={thread} onOpen={() => openThread(thread)} />
        </li>
      ))}
    </ListChip>
  );
}

function AnalystRow({ thread, onOpen }: { thread: RosterAnalyst; onOpen: () => void }) {
  const { t } = useTranslation();
  const title = thread.title || t('nav.untitledThread');
  return (
    <PopoverClose asChild>
      <button type="button" className={TRAY_ROW} onClick={onOpen}>
        <span
          aria-hidden="true"
          className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md"
          style={{ background: 'var(--color-bg-hover)', color: 'var(--color-text-tertiary)' }}
        >
          <Folder className="h-3.5 w-3.5" />
        </span>
        <span className="flex min-w-0 flex-1 flex-col gap-0.5">
          <span className="truncate text-[0.8125rem]" style={{ color: 'var(--color-text-primary)' }} title={title}>
            {title}
          </span>
          <span className="flex min-w-0 items-center gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {thread.workspaceName && <span className="truncate">{thread.workspaceName}</span>}
            {thread.workspaceName && thread.status && <span aria-hidden="true">·</span>}
            {thread.status && <DispatchStatusIndicator status={thread.status} startedAt={thread.startedAt} />}
          </span>
        </span>
        <ArrowUpRight aria-hidden="true" className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-quaternary)' }} />
      </button>
    </PopoverClose>
  );
}

function TrayList({ heading, children }: { heading: string; children: ReactNode }) {
  return (
    <PopoverContent
      side="top"
      align="start"
      sideOffset={8}
      aria-label={heading}
      className="w-[22.5rem] max-w-[calc(100vw-2rem)] rounded-xl p-1.5 shadow-lg"
      style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}
    >
      <div className="px-2.5 pt-1.5 pb-1 text-[0.6875rem] font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
        {heading}
      </div>
      <ul className="flex max-h-72 flex-col gap-0.5 overflow-y-auto">{children}</ul>
    </PopoverContent>
  );
}
