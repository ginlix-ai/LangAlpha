import { createContext, useContext } from 'react';
import type { CommandRun } from '@/types/commands';
import type { WakeupList } from '@/types/wakeups';
import type { DispatcherContext } from '../../hooks/useOpenDispatchedThread';
import type { RosterTask } from './delegatedWork';
import type { RosterAnalyst } from './useAnalystRoster';
import type { ShownPlan } from '../messageList/requestPlans';
import type { TrayContents, TrayTasks, TrayWork } from './useTrayContents';

/** The Status panel's sections, which a tray chip opens it scrolled to. */
export type StatusSectionId = 'needs' | 'plan' | 'working' | 'watch' | 'wakeups' | 'done';

/**
 * What the thread is doing, for the Status panel: the tray's contents read in
 * full, beside what the tray leaves out (a finished plan, ended wakeups, a
 * question waiting on the reader). The chat view provides it around both the
 * composer and the file panel, so the tray and the panel read one value.
 */
export interface ThreadStatus {
  contents: TrayContents | null;
  /** The handed-off work sorted once, for the panel and the chips alike. */
  work: ThreadWork;
  /** The thread's latest plan, running or finished; a new one replaces it.
   *  The tray's `contents.plan` is only the steps a running turn has left. */
  latestPlan: ShownPlan | null;
  /** The thread's wakeups as read, ended ones included, which the panel
   *  lists in full where the tray's `contents.wakeups` keeps only the pending. */
  allWakeups: WakeupList | null;
  /** The section a chip asked the panel to show, null for its top; `seq` tells two asks apart. */
  focus: { section: StatusSectionId | null; seq: number } | null;
  from: DispatcherContext | null;
  /** The thread the tray's wakeups cancel in. */
  threadId: string | null;
  onOpenTask: (agentId: string) => void;
  onOpenCommand: (toolCallId: string) => void;
  /** Brings the transcript to its latest turn, where a question waits. */
  onJumpToLatest: () => void;
  /** Opens the Status panel at a section, or at its top. */
  onOpenStatus: (section: StatusSectionId | null) => void;
}

export const ThreadStatusContext = createContext<ThreadStatus | null>(null);

export function useThreadStatus(): ThreadStatus | null {
  return useContext(ThreadStatusContext);
}

/** The thread's handed-off work by where the Status panel lists it. */
export interface ThreadWork {
  /** The turn waits on the reader's answer. */
  awaitingAnswer: boolean;
  /** Analysts waiting on the reader. */
  needs: RosterAnalyst[];
  working: { tasks: RosterTask[]; analysts: RosterAnalyst[]; commands: CommandRun[] };
  done: { tasks: RosterTask[]; analysts: RosterAnalyst[] };
  counts: { needs: number; working: number; done: number };
  /** A task or an Analyst still runs, a task no row names among them. */
  delegatedRunning: boolean;
}

/** The handed-off work `threadWork` sorts, as the tray holds it. */
interface HandedOff {
  tasks: TrayTasks | null;
  analysts: RosterAnalyst[] | null;
  commands: CommandRun[] | null;
}

/**
 * Sorts the thread's tasks, Analysts and commands into what waits on the
 * reader, what runs, and what ended. The one reading of whether handed-off
 * work is still going: the panel, the chips, the tray and the plan's waiting
 * all read it, so a chip opens the panel where its rows are.
 */
export function threadWork(work: HandedOff | null, awaitingAnswer: boolean): ThreadWork {
  const tasks = work?.tasks?.rows ?? [];
  const analysts = work?.analysts ?? [];
  const commands = work?.commands ?? [];
  // An Analyst waiting on the reader never reads as running (`useAnalystRoster`).
  const needs = analysts.filter((thread) => thread.status === 'needs_input');
  const working = {
    tasks: tasks.filter((row) => row.running),
    analysts: analysts.filter((thread) => thread.running),
    commands,
  };
  const done = {
    tasks: tasks.filter((row) => !row.running),
    analysts: analysts.filter((thread) => !thread.running && thread.status !== 'needs_input'),
  };
  return {
    awaitingAnswer,
    needs,
    working,
    done,
    counts: {
      needs: needs.length + (awaitingAnswer ? 1 : 0),
      working: working.tasks.length + working.analysts.length + commands.length,
      done: done.tasks.length + done.analysts.length,
    },
    delegatedRunning: !!work?.tasks?.unnamedRunning || working.tasks.length > 0 || working.analysts.length > 0,
  };
}

/** A message just sent is timed by the browser's clock, the work it set by
 *  the server's, so the request's start is read this much early. */
const CLOCK_SLACK_MS = 10 * 60_000;

/** Whether something will bring the agent back to the thread once its turn
 *  ends: handed-off work still running or waiting on the reader, a command
 *  running, a question the reader has not answered, a wakeup still to fire,
 *  or a report owed. A wakeup or a command set before the request began
 *  (`since`, epoch ms) is left out, as the close-out leaves it out: it brings
 *  the agent back to its own request, not to this one's plan. */
export function awaitsReport(
  work: ThreadWork,
  tray: Pick<TrayWork, 'wakeups' | 'reportBack'>,
  since: number | null = null,
): boolean {
  const setSince = (at: string) => since == null || !(Date.parse(at) < since - CLOCK_SLACK_MS);
  return work.delegatedRunning || work.counts.needs > 0
    || work.working.commands.some((command) => setSince(command.started_at))
    || !!tray.wakeups?.items.some((wakeup) => setSince(wakeup.created_at)) || !!tray.reportBack;
}

/** Whether anything in the tray is still working, which the folded dock spins for. */
export function trayWorking(contents: TrayContents, work: ThreadWork): boolean {
  return !!(contents.plan || contents.commands || contents.reportBack || work.delegatedRunning);
}

/** The section that lists a chip's rows: the first of them the panel shows,
 *  most pressing first. */
export function workSection(work: ThreadWork, of: 'tasks' | 'analysts'): StatusSectionId {
  if (of === 'analysts' && work.needs.length > 0) return 'needs';
  return work.working[of].length > 0 ? 'working' : 'done';
}
