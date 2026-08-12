import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useStableHandler } from '@/hooks/useStableHandler';
import { useStableArray } from '@/hooks/useStableArray';
import { countToolCalls } from '../../session/subagents/subagentMetrics';
import {
  deriveSubagentStatus,
  isTerminalStatus,
  normalizeWireStatus,
  sidebarAgentRowsEqual,
  toSidebarAgentRow,
} from '../../session/subagents/subagentStatus';
import { type SubagentTokenUsage, ZERO_USAGE } from '../../utils/tokenUsage';
import {
  resolveSubagentTelemetry as resolveSubagentTelemetryPure,
  type SubagentDataLike,
  type SubagentHistoryLike,
} from '../../session/subagents/resolveSubagentTelemetry';
import {
  resolveWorkflowRun as resolveWorkflowRunPure,
  type WorkflowRunState,
} from '../../session/subagents/workflowRunState';
import { getSubagentTaskStatus } from '../../utils/api';
import { taskIdFromAgentId } from '../../utils/agentId';
import type { useCardState } from '../../hooks/useCardState';
import type { useChatMessages } from '../../hooks/useChatMessages';
import { MAIN_AGENT } from './mainAgent';
import type { AgentInfo, SubagentInfo, SubagentMessage, SubagentUpdateData } from './types';

type CardStateAPI = ReturnType<typeof useCardState>;
type ChatMessagesAPI = ReturnType<typeof useChatMessages>;

/** Subagent tab registry (carved out of ChatView, 5.9c): the sidebar agents
 * list, active-tab switching with URL sync, card refresh/hydration on open,
 * and the per-subagent telemetry resolver for MessageList. */
export function useSubagentTabs({
  threadId,
  workspaceId,
  initialTaskId,
  isLoadingHistory,
  activeAgentId,
  setActiveAgentId,
  cards,
  updateSubagentCard,
  getSubagentHistory,
  resolveSubagentIdToAgentId,
  hydrateTaskTranscript,
  saveScrollPosition,
  scrollPositionsRef,
  skipSubagentAutoScrollRef,
  activeAgentIdRef,
  resolvedThreadIdRef,
}: {
  threadId: string;
  workspaceId: string;
  initialTaskId: string | undefined;
  isLoadingHistory: boolean;
  activeAgentId: string;
  setActiveAgentId: Dispatch<SetStateAction<string>>;
  cards: CardStateAPI['cards'];
  updateSubagentCard: CardStateAPI['updateSubagentCard'];
  getSubagentHistory: ChatMessagesAPI['getSubagentHistory'];
  resolveSubagentIdToAgentId: ChatMessagesAPI['resolveSubagentIdToAgentId'];
  hydrateTaskTranscript: ChatMessagesAPI['hydrateTaskTranscript'];
  saveScrollPosition: () => void;
  scrollPositionsRef: { current: Record<string, number> };
  skipSubagentAutoScrollRef: { current: boolean };
  activeAgentIdRef: { current: string };
  resolvedThreadIdRef: { current: string };
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();

  // Track hidden agents (removed from sidebar, but not from state)
  const [hiddenAgentIds, setHiddenAgentIds] = useState<Set<string>>(new Set());

  // Switch agent tab with scroll position preservation
  const switchAgent = useCallback((newAgentId: string, opts?: { push?: boolean }) => {
    if (newAgentId === activeAgentIdRef.current) return;
    const wasMain = activeAgentIdRef.current === 'main';
    saveScrollPosition();
    // If destination has a saved position, skip auto-scroll so restore wins
    if (scrollPositionsRef.current[newAgentId] != null) {
      skipSubagentAutoScrollRef.current = true;
    }
    setActiveAgentId(newAgentId);

    // Sync URL with active agent
    const tid = resolvedThreadIdRef.current || threadId;
    if (newAgentId === 'main') {
      // Replace: removes the subagent entry so browser back goes to thread gallery
      navigate(`/chat/t/${tid}`, { replace: true, state: { workspaceId } });
    } else {
      const taskSlug = taskIdFromAgentId(newAgentId) ?? newAgentId;
      // Push from main → subagent (back returns to main)
      // Replace from subagent → subagent (back still returns to main),
      // EXCEPT drill-downs (opts.push — e.g. workflow → its child), where
      // browser back should return to the parent view just left.
      navigate(`/chat/t/${tid}/${taskSlug}`, { replace: !wasMain && !opts?.push, state: { workspaceId } });
    }
  }, [saveScrollPosition, threadId, workspaceId, navigate, activeAgentIdRef, scrollPositionsRef, skipSubagentAutoScrollRef, resolvedThreadIdRef, setActiveAgentId]);

  // Ensure new active agents are visible (remove from hidden list)
  useEffect(() => {
    Object.entries(cards).forEach(([cardId, card]) => {
      if (cardId.startsWith('subagent-')) {
        const agentId = cardId.replace('subagent-', '');
        const isNewActiveAgent = card.subagentData?.isActive !== false && !card.subagentData?.isHistory;

        // If this is a new active agent, remove it from hidden list
        if (isNewActiveAgent && hiddenAgentIds.has(agentId)) {
          setHiddenAgentIds((prev) => {
            const newSet = new Set(prev);
            newSet.delete(agentId);
            return newSet;
          });
        }
      }
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cards]);

  // Convert cards to agents array for sidebar (memoized to avoid re-renders)
  const { allSubagents, subagentAgents, excessSubagents } = useMemo(() => {
    const maxSubagents = 11;
    const all = Object.entries(cards)
      .filter(([cardId]) => cardId.startsWith('subagent-'))
      .map(([cardId, card]): AgentInfo => {
        const sd = card.subagentData;
        return {
          id: cardId.replace('subagent-', ''),
          name: sd?.displayId || t('chat.worker'),
          taskId: sd?.taskId || sd?.agentId || '',
          description: sd?.description || '',
          prompt: sd?.prompt || '',
          type: sd?.type || 'general-purpose',
          // Missing status = not-yet-known → 'initializing' (deriveSubagentStatus
          // promotes it to running once messages exist); never default to a live
          // 'active' that would paint a status-less card as Running.
          status: sd?.status || 'initializing',
          error: sd?.error,
          toolCalls: countToolCalls(sd?.messages),
          tokenUsage: sd?.tokenUsage ?? ZERO_USAGE,
          currentTool: sd?.currentTool || '',
          messages: sd?.messages || [],
          isActive: sd?.isActive !== false,
          isMainAgent: false,
          ownerTaskId: sd?.ownerTaskId,
          workflowRun: sd?.workflowRun,
        };
      })
      .reverse();
    // Workflow-owned children stay out of the sidebar — the run's card is
    // their surface; they open via its drill-in (activeAgent falls back to
    // the unfiltered list so a hidden child can still be the active tab).
    const visible = all.filter(
      agent => !hiddenAgentIds.has(agent.id) && !agent.ownerTaskId
    );
    return {
      allSubagents: all,
      subagentAgents: visible.slice(0, maxSubagents),
      excessSubagents: visible.slice(maxSubagents),
    };
  }, [cards, hiddenAgentIds, t]);

  // Per-subagent telemetry resolver consumed by MessageList. Maps a message
  // segment's `subagentId` (a toolCallId) through `resolveSubagentIdToAgentId`
  // and reads tool count + token usage off the matching card. Falls back to
  // the history entry on a fresh load — cards are created lazily on click,
  // so without this fallback the inline row would stay hidden after refresh
  // until the user clicks into the subagent. Keeping the resolution in this
  // closure means MessageList never touches the cards or the toolCallId map
  // directly.
  const resolveSubagentTelemetry = useCallback((subagentId: string) => {
    const card = cards[`subagent-${resolveSubagentIdToAgentId(subagentId)}`];
    const sd = card?.subagentData as SubagentDataLike | undefined;
    const history = getSubagentHistory?.(subagentId) as SubagentHistoryLike | undefined;
    return resolveSubagentTelemetryPure(sd, history);
  }, [cards, resolveSubagentIdToAgentId, getSubagentHistory]);

  // Same closure shape for workflow-run progress: the inline WorkflowRunCard
  // subscribes at the leaf and reads the live card's reduced state, falling
  // back to the history projection after a refresh.
  const resolveWorkflowRun = useCallback((subagentId: string) => {
    const card = cards[`subagent-${resolveSubagentIdToAgentId(subagentId)}`];
    const history = getSubagentHistory?.(subagentId) as { workflowRun?: WorkflowRunState } | null;
    return resolveWorkflowRunPure(card?.subagentData?.workflowRun, history?.workflowRun);
  }, [cards, resolveSubagentIdToAgentId, getSubagentHistory]);

  // Auto-hide excess agents (beyond 11 subagents)
  const excessIds = useMemo(() => excessSubagents.map(a => a.id).join(','), [excessSubagents]);
  useEffect(() => {
    if (excessSubagents.length > 0) {
      setHiddenAgentIds((prev) => {
        const newSet = new Set(prev);
        excessSubagents.forEach(agent => {
          newSet.add(agent.id);
        });
        return newSet;
      });
    }
  }, [excessSubagents.length, excessIds]); // eslint-disable-line react-hooks/exhaustive-deps

  // Combine: main agent first, then visible subagents (limited to 11)
  const agents = useMemo((): AgentInfo[] => [MAIN_AGENT, ...subagentAgents], [subagentAgents]);

  // Nav-tree row projection. `agents` takes a fresh identity on every cards
  // update (i.e. every streamed subagent chunk), but the tree renders only the
  // row fields — keep the previous array identity whenever none of them
  // changed so the sidebar bridge publish and the panels stay quiet while a
  // subagent streams.
  const sidebarAgentRows = useStableArray(
    useMemo(() => agents.map(toSidebarAgentRow), [agents]),
    sidebarAgentRowsEqual,
  );

  // Find the active agent object for subagent view. Falls back to the
  // unfiltered card list so sidebar-hidden agents (workflow children,
  // auto-hidden excess) can still be opened in the detail view.
  const activeAgent: AgentInfo | null = activeAgentId !== 'main'
    ? agents.find(a => a.id === activeAgentId)
      || allSubagents.find(a => a.id === activeAgentId)
      || null
    : null;

  // Callback: user sent an instruction to the active subagent via the status bar.
  // Immediately insert a pending user message (breathing animation) into the card.
  const handleSubagentInstruction = useCallback((content: string) => {
    if (!activeAgent) return;
    const agentId = activeAgent.id;
    const cardId = `subagent-${agentId}`;
    const card = cards[cardId];
    const existingMessages = card?.subagentData?.messages || [];

    const pendingMessage = {
      id: `pending-instruction-${Date.now()}`,
      role: 'user',
      content,
      contentSegments: [{ type: 'text', content, order: 0 }],
      reasoningProcesses: {},
      toolCallProcesses: {},
      isPending: true,
    };

    updateSubagentCard(agentId, {
      messages: [...existingMessages, pendingMessage],
    });
  }, [activeAgent, cards, updateSubagentCard]);

  // Refresh subagent card with latest data from history or inline status.
  // Ensures status/currentTool are accurate regardless of stale streaming data.
  // agentId: stable agent_id (already resolved from toolCallId if needed)
  // overrides: optional { description, type, status } from inline card click
  const refreshSubagentCard = useCallback((agentId: string, overrides: Partial<SubagentInfo> = {}) => {
    if (!updateSubagentCard || !agentId) return;

    const history = getSubagentHistory ? getSubagentHistory(agentId) : null;
    // Preserve existing card description/type. Priority:
    // 1. History description (most authoritative — from replay)
    // 2. Existing card description (set during spawn — must not be overwritten
    //    by follow-up/resume inline cards whose description is the instruction)
    // 3. Override description (from inline card click — only used when card has
    //    no description yet, e.g., first open of a newly spawned task)
    const cardId = `subagent-${agentId}`;
    const existingDescription = cards[cardId]?.subagentData?.description;
    const existingPrompt = cards[cardId]?.subagentData?.prompt;
    const existingType = cards[cardId]?.subagentData?.type;
    const existingStatus = cards[cardId]?.subagentData?.status;
    const existingOwner = cards[cardId]?.subagentData?.ownerTaskId;
    const finalDescription = history?.description || existingDescription || overrides.description || '';
    const finalPrompt = history?.prompt || existingPrompt || overrides.prompt || '';
    const finalType = history?.type || existingType || overrides.type || 'general-purpose';
    // Workflow ownership: history is authoritative, but the drill-in click
    // (overrides) covers replayed threads whose child lanes have no history
    // entry — the owner drives back-navigation and sidebar hiding.
    const finalOwner = history?.ownerTaskId || existingOwner || overrides.ownerTaskId;
    // A card that already settled terminal is authoritative: never downgrade it to
    // a stale non-terminal history value (a history entry can still read 'running'
    // when the ledger hasn't refreshed locally yet). A genuine resume — a separate
    // code path — is the only thing that reactivates a settled task.
    const finalStatus = isTerminalStatus(existingStatus)
      ? existingStatus!
      : (history?.status || overrides.status || 'completed');
    const finalError = history?.error || overrides.error;

    // Check if card is currently live (active with an open stream). A card
    // whose own status already settled is not live however its flag reads.
    const existingCard = cards[cardId]?.subagentData;
    const isLive =
      existingCard?.isActive === true && !history && !isTerminalStatus(existingStatus);

    const updateData: SubagentUpdateData = {
      agentId,
      taskId: agentId,
      description: finalDescription,
      prompt: finalPrompt,
      type: finalType,
      isHistory: !!history,
      // Liveness is a fact here, never a write-permission flag: a settled task
      // whose card claims isActive makes updateSubagentCard reject the NEXT
      // write from this very function — the lazily hydrated transcript, which
      // carries isHistory — as "stale history over a live card". Clearing stale
      // fields on a settled card rides the terminal-write exemption instead.
      isActive: !history && (isLive || !isTerminalStatus(finalStatus)),
    };
    if (finalOwner) updateData.ownerTaskId = finalOwner;
    if (isLive) {
      // Card is actively streaming — preserve its current status and currentTool.
      // Overwriting these causes a brief "completed" flash in the SubagentStatusBar.
    } else {
      updateData.status = finalStatus;
      updateData.currentTool = '';
      if (finalError) updateData.error = finalError;
    }
    if (history) {
      updateData.messages = (history.messages || []) as SubagentMessage[];
      // Also seed tokenUsage from history. Without this, clicking a replayed
      // subagent card creates the live card with tokenUsage=ZERO_USAGE, and
      // the telemetry resolver's "card path" wins on return (messages.length > 0)
      // and reports zero tokens — even though history still has the real total.
      updateData.tokenUsage = (history.tokenUsage as SubagentTokenUsage) ?? ZERO_USAGE;
    }

    updateSubagentCard(agentId, updateData);
  }, [updateSubagentCard, getSubagentHistory, cards]);

  // Durable hydration for a clicked task whose local card is ambiguous:
  // non-terminal (a settled task whose live stream drained while the tab was
  // backgrounded — the "Initializing" spinner that never resolves), or errored
  // without a reason (a live transport-loss whose failure text only lives in
  // the ledger). Resolves the ledger's real terminal state and lands it on the
  // card. The existing messages ride along so the inactive-card guard treats
  // this as a content update — authoritative terminal truth must not be dropped
  // just because the card already settled non-terminally.
  const hydrateTaskStatusIfStale = useCallback(async (agentId: string) => {
    if (agentId === 'main' || !threadId || threadId === '__default__') return;
    const shortId = taskIdFromAgentId(agentId) ?? agentId;
    if (!shortId) return;
    const history = getSubagentHistory ? getSubagentHistory(agentId) : null;
    const card = cards[`subagent-${agentId}`]?.subagentData;
    const knownStatus = deriveSubagentStatus({
      status: (history?.status ?? card?.status) as string | undefined,
      messages: (history?.messages ?? card?.messages) as unknown[] | undefined,
    });
    const hasReason = !!(history?.error || card?.error);
    const isTerminal = knownStatus === 'completed' || knownStatus === 'cancelled' || knownStatus === 'error';
    // Already have the full terminal picture (incl. a reason for errors) — no fetch.
    if (isTerminal && (knownStatus !== 'error' || hasReason)) return;
    try {
      const res = await getSubagentTaskStatus(threadId, shortId);
      // Normalize at the boundary: the ledger endpoint speaks client vocabulary
      // today, but raw run statuses ('failed'/'interrupted') must land as
      // 'error' here rather than silently skipping the hydration.
      const s = normalizeWireStatus(res?.status);
      if (!isTerminalStatus(s)) return;
      const existing = cards[`subagent-${agentId}`]?.subagentData;
      updateSubagentCard(agentId, {
        agentId,
        taskId: agentId,
        status: s,
        ...(res.error ? { error: res.error } : {}),
        // Ride along the existing content so the inactive-card guard admits
        // this authoritative terminal update.
        messages: (existing?.messages as SubagentMessage[]) || [],
        isActive: false,
      });
    } catch {
      // Best-effort: a failed hydration leaves the card as-is (a full thread
      // reload still corrects it via replay stamping).
    }
  }, [threadId, getSubagentHistory, cards, updateSubagentCard]);

  // Lazy transcript hydration for tasks replay never projects as lanes
  // (workflow children have no Task-tool launch artifact in the main
  // transcript): fetch the checkpoint transcript on demand, then re-refresh
  // the card so the landed history entry populates it.
  const hydrateTranscriptThenRefresh = useCallback(
    (agentId: string, overrides: Partial<SubagentInfo> = {}) => {
      if (!hydrateTaskTranscript) return;
      const existing = getSubagentHistory?.(agentId) as SubagentHistoryLike | null;
      if (existing?.messages?.length) return;
      void hydrateTaskTranscript(agentId, {
        description: overrides.description,
        type: overrides.type,
        status: overrides.status,
      }).then((landed) => {
        if (landed) refreshSubagentCard(agentId, overrides);
      });
    },
    [hydrateTaskTranscript, getSubagentHistory, refreshSubagentCard],
  );

  // Handle sidebar agent selection — refresh card data, then switch tab.
  // useStableHandler (not useCallback): this crosses the sidebar bridge, and
  // hydrateTaskStatusIfStale's deps include `cards` — a useCallback here would
  // take a fresh identity on every streamed card update and re-publish the
  // bridge slice, re-rendering the whole AppSidebar per chunk.
  const handleSelectAgent = useStableHandler((agentId: string) => {
    if (agentId !== 'main') {
      refreshSubagentCard(agentId);
      void hydrateTaskStatusIfStale(agentId);
    }
    switchAgent(agentId);
  });

  // Open subagent task (navigate to subagent tab) - shared between MessageList and DetailPanel
  const handleOpenSubagentTask = useCallback((subagentInfo: SubagentInfo) => {
    const { subagentId, description, prompt, type, status, ownerTaskId } = subagentInfo;
    // Resolve subagentId (may be toolCallId from segment) to stable agent_id for card operations
    const agentId = resolveSubagentIdToAgentId
      ? resolveSubagentIdToAgentId(subagentId)
      : subagentId;

    if (!updateSubagentCard) {
      console.error('[ChatView] updateSubagentCard is not defined!');
      return;
    }

    refreshSubagentCard(agentId, { description, prompt, type, status, ownerTaskId });
    void hydrateTaskStatusIfStale(agentId);
    hydrateTranscriptThenRefresh(agentId, { description, prompt, type, status, ownerTaskId });

    // Drill-down from the owning workflow's tab pushes history so browser
    // back returns to the workflow view, not the main chat.
    const isDrillDown = !!ownerTaskId && ownerTaskId === activeAgentIdRef.current;
    switchAgent(agentId, { push: isDrillDown });
  }, [resolveSubagentIdToAgentId, updateSubagentCard, refreshSubagentCard, hydrateTaskStatusIfStale, hydrateTranscriptThenRefresh, switchAgent, activeAgentIdRef]);

  // Handle removing an agent from sidebar (just hide from display, don't
  // affect state). Stable for the same bridge reason as handleSelectAgent.
  const handleRemoveAgent = useStableHandler((agentId: string) => {
    // Add to hidden set
    setHiddenAgentIds((prev) => {
      const newSet = new Set(prev);
      newSet.add(agentId);
      return newSet;
    });

    // If the removed agent was active, fallback to main (preserving main's scroll position)
    if (activeAgentIdRef.current === agentId) {
      switchAgent('main');
    }
  });

  // Sync activeAgentId with URL-derived initialTaskId (browser back/forward)
  useEffect(() => {
    const urlAgentId = initialTaskId ? `task:${initialTaskId}` : 'main';
    if (urlAgentId !== activeAgentIdRef.current) {
      saveScrollPosition();
      if (scrollPositionsRef.current[urlAgentId] != null) {
        skipSubagentAutoScrollRef.current = true;
      }
      setActiveAgentId(urlAgentId);
    }
  }, [initialTaskId, saveScrollPosition, activeAgentIdRef, scrollPositionsRef, skipSubagentAutoScrollRef, setActiveAgentId]);

  // Refresh subagent card data on deep link / browser forward (guarded to run once per taskId)
  const lastRefreshedTaskRef = useRef<string | undefined>(undefined);
  useEffect(() => {
    if (!initialTaskId || isLoadingHistory) {
      lastRefreshedTaskRef.current = undefined;
      return;
    }
    if (lastRefreshedTaskRef.current === initialTaskId) return;
    lastRefreshedTaskRef.current = initialTaskId;
    refreshSubagentCard(`task:${initialTaskId}`);
    // Deep link straight to a workflow child: its lane was never projected,
    // so the transcript must hydrate on demand like the drill-in path — and
    // so must the status. With no lane and no click-through override, the
    // card's status falls back to 'completed', which is a guess the ledger
    // can settle: without this a child that failed reads as finished work.
    void hydrateTaskStatusIfStale(`task:${initialTaskId}`);
    hydrateTranscriptThenRefresh(`task:${initialTaskId}`);
  }, [initialTaskId, isLoadingHistory, refreshSubagentCard, hydrateTaskStatusIfStale, hydrateTranscriptThenRefresh]);

  return {
    sidebarAgentRows,
    activeAgent,
    switchAgent,
    handleSelectAgent,
    handleOpenSubagentTask,
    handleRemoveAgent,
    handleSubagentInstruction,
    resolveSubagentTelemetry,
    resolveWorkflowRun,
  };
}
