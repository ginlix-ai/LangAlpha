import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useStableArray } from '@/hooks/useStableArray';
import type { ChatMessage } from '@/types/chat';
import type { CommandRun } from '@/types/commands';
import type { WakeupList } from '@/types/wakeups';
import { useThreadWakeups } from '../../hooks/useWakeups';
import type { MarketWatchState } from '../../session/marketWatchEvents';
import type { OwedReportKind, ReportBackOwedThread } from '../../utils/api';
import {
  analystCandidates,
  countedTurnStarts,
  rosterTasksEqual,
  taskRoster,
  type RosterTask,
} from './delegatedWork';
import { useAnalystRoster, type RosterAnalyst } from './useAnalystRoster';
import { pendingWakeups } from '../wakeupLabels';
import { runningCommands } from '../commands/commandWords';

interface TrayInputs {
  marketWatch: MarketWatchState | null;
  /** The thread's background commands, which the host reads once for the
   *  tray and the tool cards alike. */
  commands: readonly CommandRun[];
  /** The thread whose wakeups the tray counts, and whether its chat is in front. */
  threadId: string | null;
  isActive: boolean;
  /** Live task cards, keyed `subagent-<agentId>`. */
  cards: Parameters<typeof taskRoster>[0]['cards'];
  /** The tasks the page's history loaded, by agent id. */
  taskHistory: Parameters<typeof taskRoster>[0]['history'];
  /** The main agent's transcript. */
  messages: ChatMessage[];
  isLoading: boolean;
  hasActiveSubagents: boolean;
  reportBackOwed: boolean;
  owedThreads: ReportBackOwedThread[] | null;
  /** Whose reports are owed, when the server said. */
  owedKinds: OwedReportKind[] | null;
  /** Who usually reports back here, for a read that names no kinds:
   *  Analysts to Home, subagents to a workspace thread. */
  isHome: boolean;
}

/** The tasks chip: the rows it lists, and whether a task no row names runs
 *  (a workflow's own steps, or a row not loaded yet). */
export interface TrayTasks {
  rows: RosterTask[];
  unnamedRunning: boolean;
}

/** What the tray holds: one chip per entry that is set. */
export interface TrayContents {
  /** The market watch, while the agent watches tickers. */
  watch: MarketWatchState | null;
  /** The wakeups still pending, while there are any. */
  wakeups: WakeupList | null;
  /** The background commands still running, oldest first, while any is. */
  commands: CommandRun[] | null;
  /** The tasks this thread handed off, while one is listed or runs. */
  tasks: TrayTasks | null;
  /** The Analysts this thread handed work to (Home only). */
  analysts: RosterAnalyst[] | null;
  reportBackLabel: string | null;
}

const NO_CANDIDATES: ReturnType<typeof analystCandidates> = [];

/**
 * What an owed report is waiting on, named by whose it is. A background
 * command's notice needs no words: the command has ended, its tool card says
 * so, and its turn starts on its own. A read that named no kinds falls back to
 * who usually reports back here.
 */
export function owedReportKey(kinds: readonly OwedReportKind[] | null, isHome: boolean): string | null {
  if (!kinds?.length) return isHome ? 'agents.reportBackPending' : 'chat.taskReportBackPending';
  if (kinds.includes('analyst')) return 'agents.reportBackPending';
  if (kinds.includes('task')) return 'chat.taskReportBackPending';
  return null;
}

/** How many chips the tray draws, which is how many shapes its liquid runs. */
export function trayChipCount(contents: TrayContents | null): number {
  if (!contents) return 0;
  const { watch, wakeups, commands, tasks, analysts, reportBackLabel } = contents;
  return [watch, wakeups, commands, tasks, analysts, reportBackLabel].filter((entry) => entry !== null).length;
}

/** Whether anything in the tray is still working, which the folded dock spins for. */
export function trayWorking({ commands, tasks, analysts, reportBackLabel }: TrayContents): boolean {
  return !!(
    commands
    || (tasks && (tasks.unnamedRunning || tasks.rows.some((task) => task.running)))
    || analysts?.some((analyst) => analyst.running)
    || reportBackLabel
  );
}

/**
 * What the thread is waiting on or handed off, which the tray shows as chips:
 * the market watch, its pending wakeups, the commands still running in the
 * background, the tasks and Analysts it delegated, and an owed report
 * neither names. Null when there is none. Read once by the host, since the
 * tray and its folded dock show the same thing.
 */
export function useTrayContents({
  marketWatch, commands, threadId, isActive, cards, taskHistory, messages, isLoading, hasActiveSubagents, reportBackOwed, owedThreads,
  owedKinds, isHome,
}: TrayInputs): TrayContents | null {
  const { t } = useTranslation();

  const watch = marketWatch && marketWatch.symbols.length ? marketWatch : null;
  const wakeups = useThreadWakeups(threadId, isActive);
  // A wakeup fires server-side between turns, so it keeps the chip up.
  const pending = useMemo(() => {
    const items = pendingWakeups(wakeups?.items);
    return items.length ? { items, userTimezone: wakeups?.userTimezone ?? null } : null;
  }, [wakeups]);
  // Only a running command is waiting on anything: one that ended has
  // reported, or will at the next turn, and its tool card says how.
  // The listing keeps its identity until a read changes it, unlike the
  // transcript, so this needs no equality of its own.
  const liveCommands = useMemo(() => runningCommands(commands), [commands]);
  const commandChip = liveCommands.length > 0 ? liveCommands : null;
  const turnStarts = countedTurnStarts(messages);
  // A task's status can read live after its run ended unseen; one runs only
  // while this turn does or a task channel is open.
  const tasks = useStableArray(
    taskRoster({ cards, history: taskHistory, messages, turnStarts, workRunning: hasActiveSubagents || isLoading }),
    rosterTasksEqual,
  );
  const analysts = useAnalystRoster(isHome ? analystCandidates(messages, owedThreads) : NO_CANDIDATES, turnStarts);

  // An open task channel that no running row accounts for still shows, beside
  // the rows the tray keeps.
  const unnamedRunning = hasActiveSubagents && !tasks.some((task) => task.running);
  const taskChip = tasks.length > 0 || unnamedRunning ? { rows: tasks, unnamedRunning } : null;
  const analystChip = analysts.length > 0 ? analysts : null;
  // A report the tray can only name: an owed report whose sender is no
  // longer working, so no chip is already waiting on it. An Analyst waiting
  // on the user has not finished, and its row says so.
  const working = hasActiveSubagents || tasks.some((task) => task.running)
    || analysts.some((analyst) => analyst.running || analyst.status === 'needs_input');
  const labelKey = reportBackOwed && !working ? owedReportKey(owedKinds, isHome) : null;
  const reportBackLabel = labelKey ? t(labelKey) : null;

  if (!watch && !pending && !commandChip && !taskChip && !analystChip && !reportBackLabel) return null;
  return { watch, wakeups: pending, commands: commandChip, tasks: taskChip, analysts: analystChip, reportBackLabel };
}
