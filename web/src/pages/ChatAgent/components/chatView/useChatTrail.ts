import { useTranslation } from 'react-i18next';
import { usePageTabTitle } from '@/components/PageTabs/usePageTabTitle';
import { sidebarAgentRowLabel, toSidebarAgentRow, type SidebarAgentRow } from '../../session/subagents/subagentStatus';
import { useLiveThread } from '../../utils/threadQueries';
import { threadName } from '../messageList/sentMessage';
import type { Crumb } from './ChatBreadcrumb';
import type { AgentInfo } from './types';

interface ChatTrailInput {
  /** The thread in view; null or `__default__` before it exists. */
  threadId: string | null | undefined;
  workspaceName: string;
  isActive: boolean;
  activeAgentId: string;
  activeAgent: AgentInfo | null;
  agentRows: SidebarAgentRow[];
  onSelectAgent: (agentId: string) => void;
  /** The thread that handed this one its turn, when it was opened from there. */
  fromThreadId: string | undefined;
  onOpenDispatcher: (threadId: string) => void;
  /** Leaves for the workspace's thread list. */
  onExit: () => void;
}

/**
 * Where the chat on screen sits, and one step up from it.
 *
 * `up` is not read off the trail: the thread's crumb waits for its name and
 * a workflow run's for its row, but a subagent always steps up to its owning
 * run or the main chat, and a handed-off thread back to its dispatcher, a
 * step the trail does not show.
 */
export function useChatTrail({
  threadId, workspaceName, isActive, activeAgentId, activeAgent, agentRows,
  onSelectAgent, fromThreadId, onOpenDispatcher, onExit,
}: ChatTrailInput): { crumbs: Crumb[]; up: () => void } {
  const { t } = useTranslation();
  const threadLabel = threadName(useLiveThread(threadId));
  // The tab is named for the thread, as a browser's is for its page; the
  // trail says which workspace it is in. Only the view on screen names it,
  // since the kept-mounted ones are in no tab.
  usePageTabTitle(isActive ? threadLabel || workspaceName : null);

  const inAgent = activeAgentId !== 'main';
  const ownerTaskId = activeAgent?.ownerTaskId;
  const ownerRow = ownerTaskId ? agentRows.find((row) => row.id === ownerTaskId) : undefined;
  const crumbs: Crumb[] = [
    { label: workspaceName || t('thread.workspace'), onOpen: onExit },
    ...(threadLabel ? [{ label: threadLabel, onOpen: () => onSelectAgent('main') }] : []),
    ...(ownerRow ? [{ label: sidebarAgentRowLabel(ownerRow), onOpen: () => onSelectAgent(ownerRow.id) }] : []),
    ...(activeAgent && inAgent ? [{ label: sidebarAgentRowLabel(toSidebarAgentRow(activeAgent)) }] : []),
  ];

  const up = inAgent
    ? () => onSelectAgent(ownerTaskId ?? 'main')
    : fromThreadId
      ? () => onOpenDispatcher(fromThreadId)
      : onExit;

  return { crumbs, up };
}
