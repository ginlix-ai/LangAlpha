/**
 * The tray's chips as controls: each chip's face (`trayFaces`) inside what
 * it does. Each chip that names more than one thing opens a list whose rows
 * jump to it; a chip that names one jumps straight there, unless its list is
 * where a control lives (a wakeup's Cancel, a command's Stop).
 */
import type { ComponentProps, ComponentType, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight } from 'lucide-react';
import { Popover, PopoverClose, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { cn } from '@/lib/utils';
import { useOpenDispatchedThread } from '../../hooks/useOpenDispatchedThread';
import { CommandListRows } from '../commands/CommandListRows';
import { AgentPlan } from '../messageList/AgentPlan';
import { planHeading } from '../messageList/requestPlans';
import { RunGlyph } from '../messageList/runCard';
import { WakeupListRows } from '../wakeups/WakeupList';
import { AnalystRow, InTrayList, TaskRow } from './statusRows';
import { useThreadStatus, workSection, type StatusSectionId, type ThreadStatus } from './threadStatus';
import type { TrayDensity } from './trayDensity';
import { CHIP, chipFace, openStatusFace, type ChipFace, type FaceContext } from './trayFaces';
import type { RosterAnalyst } from './useAnalystRoster';
import type { TrayChipId, TrayContents, TrayEntry } from './useTrayContents';

interface ChipProps<K extends TrayChipId> {
  value: NonNullable<TrayContents[K]>;
  face: ChipFace;
  status: ThreadStatus;
  ctx: FaceContext;
}

/** Each chip's control. The owed report has none: its face only ever says what runs. */
const CHIPS: { [K in TrayChipId]?: ComponentType<ChipProps<K>> } = {
  plan: PlanChip,
  watch: WatchChip,
  wakeups: WakeupsChip,
  commands: CommandsChip,
  tasks: TasksChip,
  analysts: AnalystsChip,
};

/** One of the tray's chips, at the row's density: the control its face asks for (`ChipFace.kind`). */
export function TrayChip<K extends TrayChipId>({ entry, density, status, ctx }: {
  entry: TrayEntry<K>;
  density: TrayDensity;
  status: ThreadStatus;
  ctx: FaceContext;
}) {
  const face = chipFace(entry.id, entry.value, density, ctx);
  const Chip: ComponentType<ChipProps<K>> | undefined = CHIPS[entry.id];
  if (face.kind === 'status' || !Chip) return <StatusChip face={face} />;
  return <Chip value={entry.value} face={face} status={status} ctx={ctx} />;
}

/** A face drawn as the chip's button. */
function FaceButton({ face, ...props }: { face: ChipFace } & ComponentProps<'button'>) {
  return (
    <button type="button" className={face.className} title={face.title} aria-label={face.name} {...props}>
      {face.content}
    </button>
  );
}

/**
 * The way to the Status panel from wherever the tray is: with the panel
 * closed, or a lone chip that opens its own thread rather than a list.
 * Opens it at the top.
 */
export function OpenStatusButton({ more, ctx, onOpen }: {
  /** Chips the row had no room for, which the panel still lists. */
  more: number;
  ctx: Pick<FaceContext, 't' | 'locale'>;
  onOpen: () => void;
}) {
  return <FaceButton face={openStatusFace(more, ctx)} onClick={onOpen} />;
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

/** A chip that only says what is running: a polite live region, no control. */
function StatusChip({ face }: { face: ChipFace }) {
  return (
    <div className={face.className} role="status" aria-live="polite" title={face.title}>
      {face.content}
    </div>
  );
}

/**
 * A chip that opens a list, whose rows close it as they open what they name.
 * A chip whose lone row would only jump somewhere mounts it only while it
 * stands for more than one thing, so the open state goes with it: a list
 * that shrinks to one while open does not spring back open when a second
 * arrives.
 */
function ListChip({ face, heading, section, children }: {
  face: ChipFace;
  heading: string;
  section: StatusSectionId;
  children: ReactNode;
}) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <FaceButton face={face} />
      </PopoverTrigger>
      <TrayList heading={heading} section={section}>{children}</TrayList>
    </Popover>
  );
}

/**
 * The running turn's plan: the step in progress and how far along, opening
 * the whole list. First in the row, since it is what the turn is doing now;
 * the other chips are what runs beside it. It leaves when the turn ends.
 */
function PlanChip({ value: steps, face }: ChipProps<'plan'>) {
  const { t } = useTranslation();
  return (
    <Popover>
      <PopoverTrigger asChild>
        <FaceButton face={face} />
      </PopoverTrigger>
      {/* The chip stands only while the turn runs, so its plan is live. */}
      <TrayList heading={t('chat.plan.heading', { progress: planHeading(steps, 'live', t) })} section="plan" list={false}>
        <AgentPlan steps={steps} state="live" dense />
      </TrayList>
    </Popover>
  );
}

/** The tickers the agent is live-stamping, opening their live prices in the Status panel. */
function WatchChip({ face, status }: ChipProps<'watch'>) {
  return <FaceButton face={face} onClick={() => status.onOpenStatus('watch')} />;
}

/**
 * The wakeups the agent set in this thread, opening a list where each can be
 * cancelled. Not a status line, so no live region: it changes when the agent
 * sets or a wakeup fires.
 */
function WakeupsChip({ value: list, face, status, ctx }: ChipProps<'wakeups'>) {
  return (
    <ListChip face={face} heading={ctx.t('chat.wakeups.panelTitle')} section="wakeups">
      <WakeupListRows threadId={status.threadId} wakeups={list.items} now={ctx.now} userTimezone={list.userTimezone} />
    </ListChip>
  );
}

/** The commands still running in the background, as a list even for one,
 *  since the Stop lives there. */
function CommandsChip({ value: rows, face, status }: ChipProps<'commands'>) {
  const { t } = useTranslation();
  return (
    <ListChip face={face} heading={t('chat.tray.commandsHeading')} section="working">
      <CommandListRows rows={rows} onOpen={status.onOpenCommand} />
    </ListChip>
  );
}

function TasksChip({ value: { rows }, face, status }: ChipProps<'tasks'>) {
  const { t } = useTranslation();
  // The face names a lone task.
  if (face.kind === 'button') return <FaceButton face={face} onClick={() => status.onOpenTask(rows[0].id)} />;
  const section = workSection(status.work, 'tasks');
  return (
    <ListChip face={face} heading={t('chat.tray.tasksHeading')} section={section}>
      {rows.map((row) => (
        <li key={row.id}>
          <TaskRow row={row} onOpen={status.onOpenTask} />
        </li>
      ))}
    </ListChip>
  );
}

function AnalystsChip({ value: threads, face, status }: ChipProps<'analysts'>) {
  const { t } = useTranslation();
  const openDispatchedThread = useOpenDispatchedThread(status.from);
  const openThread = (thread: RosterAnalyst) => openDispatchedThread(thread.threadId, thread.workspaceId);
  // The face names a lone Analyst's thread.
  if (face.kind === 'button') return <FaceButton face={face} onClick={() => openThread(threads[0])} />;
  const section = workSection(status.work, 'analysts');
  return (
    <ListChip face={face} heading={t('agents.tray.analystsHeading')} section={section}>
      {threads.map((thread) => (
        <li key={thread.threadId}>
          <AnalystRow thread={thread} onOpen={() => openThread(thread)} />
        </li>
      ))}
    </ListChip>
  );
}

/**
 * A chip's list, above the tray: rows, or with `list` off one block (the
 * plan). Its foot opens the Status panel at the section that holds the same
 * things in full, beside the rest of the thread.
 */
function TrayList({ heading, section, list = true, children }: {
  heading: string;
  section: StatusSectionId;
  list?: boolean;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const status = useThreadStatus();
  return (
    <PopoverContent
      side="top"
      align="start"
      sideOffset={8}
      aria-label={heading}
      // The chips' own look, grown: the page's color inside an outline, lifted
      // off the reply behind it by the shadow, since the fill alone matches
      // the text around it.
      className="w-[19rem] max-w-[calc(100vw-2rem)] rounded-xl p-1 shadow-lg"
      style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-elevated)' }}
    >
      <div className="px-2 pt-1 pb-0.5 text-[0.6875rem] font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
        {heading}
      </div>
      {list ? (
        <InTrayList.Provider value>
          <ul className="flex max-h-64 flex-col overflow-y-auto">{children}</ul>
        </InTrayList.Provider>
      ) : (
        <div className="max-h-64 overflow-y-auto">{children}</div>
      )}
      {status && (
        <PopoverClose asChild>
          <button
            type="button"
            className="mt-0.5 flex w-full cursor-pointer items-center gap-1.5 rounded-none border-t border-(--color-border-muted) px-2 pt-1.5 pb-1 text-left text-xs text-(--color-text-tertiary) outline-hidden transition-colors hover:text-(--color-text-secondary) focus-visible:text-(--color-text-secondary)"
            onClick={() => status.onOpenStatus(section)}
          >
            {t('chat.status.openInPanel')}
            <ArrowUpRight aria-hidden="true" className="h-3 w-3 shrink-0" />
          </button>
        </PopoverClose>
      )}
    </PopoverContent>
  );
}
