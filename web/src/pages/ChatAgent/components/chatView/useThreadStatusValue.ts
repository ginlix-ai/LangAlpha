import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useStableHandler } from '@/hooks/useStableHandler';
import { useThreadWakeups } from '../../hooks/useWakeups';
import type { DispatcherContext } from '../../hooks/useOpenDispatchedThread';
import type { OwedReportKind } from '../../utils/api';
import type { SubagentInfo } from '../messageList/types';
import { requestBegan, threadPlans, type ShownPlan, type ThreadPlans } from '../messageList/requestPlans';
import { owedReportKey, trayContentsOf, useTrayWork } from './useTrayContents';
import { awaitsReport, threadWork, type StatusSectionId, type ThreadStatus } from './threadStatus';

type TrayWorkInputs = Omit<Parameters<typeof useTrayWork>[0], 'wakeups'>;

interface ThreadStatusInputs extends TrayWorkInputs {
  /** The thread the tray and the panel read, and whether its chat is in front. */
  threadId: string | null;
  isActive: boolean;
  /** The turn waits on the reader's answer. */
  awaitingAnswer: boolean;
  reportBackOwed: boolean;
  /** Whose reports are owed, when the server said. */
  owedKinds: OwedReportKind[] | null;
  from: DispatcherContext | null;
  /** Shows a task the main view has a card for. */
  onSelectAgent: (agentId: string) => void;
  /** Opens a task with no card yet, from what the tray knows of it. */
  onOpenSubagentTask: (info: SubagentInfo) => void;
  onOpenCommand: (toolCallId: string) => void;
  onOpenStatusPanel: () => void;
  onJumpToLatest: () => void;
}

/**
 * What the thread is doing, built once for the tray, the Status panel and the
 * transcript: its handed-off work, every plan it shows and where, and the
 * handlers the chips and the panel act through. Each part keeps its identity
 * until it changes, so the status does too, and its readers render only when
 * the thread's status moved, not with every render of the chat.
 */
export function useThreadStatusValue({
  threadId, isActive, awaitingAnswer, reportBackOwed, owedKinds, from, onSelectAgent, onOpenSubagentTask, onOpenCommand,
  onOpenStatusPanel, onJumpToLatest, ...trayInputs
}: ThreadStatusInputs): { status: ThreadStatus; plansUnderMessages: ThreadPlans['underMessages'] } {
  const { t } = useTranslation();
  const { messages, isLoading, cards, isHome } = trayInputs;
  const allWakeups = useThreadWakeups(threadId, isActive);
  const chips = useTrayWork({ ...trayInputs, wakeups: allWakeups });
  const work = useMemo(() => threadWork(chips, awaitingAnswer), [chips, awaitingAnswer]);
  // A report the tray can only name: an owed report whose sender is no
  // longer working, so no chip is already waiting on it. An Analyst waiting
  // on the user has not finished, and its row says so.
  const owedKey = reportBackOwed && !work.delegatedRunning && work.needs.length === 0 ? owedReportKey(owedKinds, isHome) : null;
  const reportBack = owedKey ? t(owedKey) : null;
  // A plan the turn left unfinished waits, rather than reads as not done,
  // while something will bring the agent back to it. Read from the work
  // alone: the plan's own chip stands only while the turn runs, when waiting
  // decides nothing. Only what the plan's own request set counts.
  const began = useMemo(() => requestBegan(messages), [messages]);
  const waiting = awaitsReport(work, { wakeups: chips.wakeups, reportBack }, began);
  const plans = useMemo(() => threadPlans(messages, !!isLoading, waiting), [messages, isLoading, waiting]);
  const trayPlan = plans.tray;
  const contents = useMemo(() => trayContentsOf({ ...chips, reportBack }, trayPlan), [chips, reportBack, trayPlan]);
  // The plans are read again with every transcript change; the latest one
  // holds while its steps and state do.
  const latestSteps = plans.latest?.steps ?? null;
  const latestState = plans.latest?.state ?? null;
  const latestPlan = useMemo<ShownPlan | null>(
    () => (latestSteps && latestState ? { steps: latestSteps, state: latestState } : null),
    [latestSteps, latestState],
  );

  // A roster task the page only knows from history or its report has no card
  // yet: open it the way its inline card would, which loads its transcript,
  // with the words and status the tray shows rather than a card's defaults.
  const onOpenTask = useStableHandler((agentId: string) => {
    if (cards[`subagent-${agentId}`]) return onSelectAgent(agentId);
    const row = contents?.tasks?.rows.find((task) => task.id === agentId);
    onOpenSubagentTask({ subagentId: agentId, description: row?.description, status: row?.status });
  });
  const [focus, setFocus] = useState<ThreadStatus['focus']>(null);
  const onOpenStatus = useStableHandler((section: StatusSectionId | null) => {
    setFocus((prev) => ({ section, seq: (prev?.seq ?? 0) + 1 }));
    onOpenStatusPanel();
  });
  const jumpToLatest = useStableHandler(onJumpToLatest);
  const openCommand = useStableHandler(onOpenCommand);

  const status = useMemo<ThreadStatus>(() => ({
    contents,
    work,
    latestPlan,
    allWakeups,
    focus,
    from,
    threadId,
    onOpenTask,
    onOpenCommand: openCommand,
    onJumpToLatest: jumpToLatest,
    onOpenStatus,
  }), [contents, work, latestPlan, allWakeups, focus, from, threadId, onOpenTask, openCommand, jumpToLatest, onOpenStatus]);
  return { status, plansUnderMessages: plans.underMessages };
}
