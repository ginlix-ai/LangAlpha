import { useMemo } from 'react';
import { useStableArray } from '@/hooks/useStableArray';
import type { ChatMessage, PlanStep } from '@/types/chat';
import type { CommandRun } from '@/types/commands';
import type { WakeupList } from '@/types/wakeups';
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
  /** The thread's wakeups as read, ended ones included. */
  wakeups: WakeupList | null;
  /** Live task cards, keyed `subagent-<agentId>`. */
  cards: Parameters<typeof taskRoster>[0]['cards'];
  /** The tasks the page's history loaded, by agent id. */
  taskHistory: Parameters<typeof taskRoster>[0]['history'];
  /** The main agent's transcript. */
  messages: ChatMessage[];
  isLoading: boolean;
  hasActiveSubagents: boolean;
  owedThreads: ReportBackOwedThread[] | null;
  /** The thread is Home, where Analysts report back and are listed;
   *  subagents report to a workspace thread. */
  isHome: boolean;
}

/** The tasks chip: the rows it lists, and whether a task no row names runs
 *  (a workflow's own steps, or a row not loaded yet). */
export interface TrayTasks {
  rows: RosterTask[];
  unnamedRunning: boolean;
}

/** What the thread is waiting on or handed off, beside its plan. */
export interface TrayWork {
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
  /** An owed report no other chip is waiting on, in words. */
  reportBack: string | null;
}

/** The work's chips before the owed report, which is named only once the
 *  work is sorted (`threadWork`): it stands when no other chip waits on it. */
export type WorkChips = Omit<TrayWork, 'reportBack'>;

/** What the tray holds: one chip per entry that is set. */
export interface TrayContents extends TrayWork {
  /** The running request's steps while one is left (`threadPlans`). */
  plan: PlanStep[] | null;
}

/** The tray's chips in row order, one per field of `TrayContents`. */
export const CHIP_ORDER = ['plan', 'watch', 'wakeups', 'commands', 'tasks', 'analysts', 'reportBack'] as const;

export type TrayChipId = (typeof CHIP_ORDER)[number];

// Every field of `TrayContents` has its place in the order, and nothing else does.
type Unordered = Exclude<keyof TrayContents, TrayChipId> | Exclude<TrayChipId, keyof TrayContents>;
const _ordered: [Unordered] extends [never] ? true : Unordered = true;

/** One chip the tray holds: its id, and its entry. */
export interface TrayEntry<K extends TrayChipId = TrayChipId> {
  id: K;
  value: NonNullable<TrayContents[K]>;
}

function entryOf<K extends TrayChipId>(contents: TrayContents, id: K): TrayEntry<K> | null {
  const value = contents[id];
  return value == null ? null : { id, value };
}

/** The chips the tray holds, in row order. */
export function trayEntries(contents: TrayContents): TrayEntry[] {
  return CHIP_ORDER.flatMap((id) => entryOf(contents, id) ?? []);
}

/** The tray's contents: the work, and the plan's chip in front of it. Null
 *  while no chip is set. */
export function trayContentsOf(work: TrayWork, plan: PlanStep[] | null): TrayContents | null {
  const contents: TrayContents = { ...work, plan };
  return CHIP_ORDER.some((id) => contents[id] != null) ? contents : null;
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

/**
 * What the thread is waiting on or handed off, which the tray shows as chips:
 * the market watch, its pending wakeups, the commands still running in the
 * background, and the tasks and Analysts it delegated, each null while there
 * is none. The plan is not read here, so whether the plan waits can be read
 * from this before the plan is built. The value keeps its identity until a
 * chip changes, so the status built on it holds across the chat's renders.
 */
export function useTrayWork({
  marketWatch, commands, wakeups, cards, taskHistory, messages, isLoading, hasActiveSubagents, owedThreads, isHome,
}: TrayInputs): WorkChips {
  const watch = marketWatch && marketWatch.symbols.length ? marketWatch : null;
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
  const taskChip = useMemo(
    () => (tasks.length > 0 || unnamedRunning ? { rows: tasks, unnamedRunning } : null),
    [tasks, unnamedRunning],
  );
  const analystChip = analysts.length > 0 ? analysts : null;
  return useMemo(
    () => ({ watch, wakeups: pending, commands: commandChip, tasks: taskChip, analysts: analystChip }),
    [watch, pending, commandChip, taskChip, analystChip],
  );
}
