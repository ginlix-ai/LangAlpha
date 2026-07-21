import { useState } from 'react';
import { type SubagentTokenUsage, ZERO_USAGE } from '../utils/tokenUsage';
import { isTerminalStatus } from '../utils/subagentStatus';

// --- Card-level types ---

interface TodoItem {
  status: 'pending' | 'in_progress' | 'completed' | 'stale';
  [key: string]: unknown;
}

interface TodoData {
  todos: TodoItem[];
  total: number;
  completed: number;
  in_progress: number;
  pending: number;
  [key: string]: unknown;
}

interface SubagentMessage {
  role: string;
  isStreaming?: boolean;
  toolCallProcesses?: Record<string, { isInProgress?: boolean; isComplete?: boolean; [key: string]: unknown }>;
  reasoningProcesses?: Record<string, { isReasoning?: boolean; reasoningComplete?: boolean; [key: string]: unknown }>;
  [key: string]: unknown;
}

interface SubagentData {
  agentId?: string;
  taskId?: string;
  description?: string;
  prompt?: string;
  type?: string;
  tokenUsage?: SubagentTokenUsage;
  currentTool?: string;
  status?: string;
  /** Ledger failure reason for an errored task (shown in the detail header). */
  error?: string;
  messages?: SubagentMessage[];
  isActive?: boolean;
  isHistory?: boolean;
  isReconnect?: boolean;
  title?: string;
  [key: string]: unknown;
}

interface Card {
  title?: string;
  todoData?: TodoData;
  subagentData?: SubagentData;
  [key: string]: unknown;
}

type CardsMap = Record<string, Card>;

export interface UseCardStateResult {
  cards: CardsMap;
  updateTodoListCard: (todoData: TodoData) => void;
  updateSubagentCard: (agentId: string, subagentDataUpdate: SubagentData) => void;
  finalizePendingTodos: () => void;
  clearSubagentCards: () => void;
}

export function useCardState(initialCards: CardsMap = {}): UseCardStateResult {
  const [cards, setCards] = useState<CardsMap>(initialCards);

  const updateTodoListCard = (todoData: TodoData) => {
    const cardId = 'todo-list-card';

    setCards((prev) => {
      if (prev[cardId]) {
        return {
          ...prev,
          [cardId]: {
            ...prev[cardId],
            todoData: todoData,
          },
        };
      } else {
        return {
          ...prev,
          [cardId]: {
            title: 'Todo List',
            todoData: todoData,
          },
        };
      }
    });
  };

  const updateSubagentCard = (agentId: string, subagentDataUpdate: SubagentData) => {
    const cardId = `subagent-${agentId}`;

    // An explicit terminal status write (a per-task chan_close) is authoritative:
    // it must always land, even on a card the guards below would otherwise treat
    // as inactive or absent — otherwise a closure is silently dropped and the card
    // is left free to be re-activated by a stale-liveness signal.
    const isTerminalWrite = isTerminalStatus(subagentDataUpdate.status);

    setCards((prev) => {
      if (prev[cardId]) {
        const existingCard = prev[cardId];
        const existingSubagentData = existingCard.subagentData || {};
        const isCurrentlyInactive = existingSubagentData.isActive === false;
        const isBeingReactivated = subagentDataUpdate.isActive === true;

        // Guard: don't overwrite an active card (receiving live updates) with stale
        // history data. This prevents clicking a resumed inline card from replacing
        // the live streaming messages with an old pre-resume snapshot.
        if (!isCurrentlyInactive && subagentDataUpdate.isHistory) {
          // Card is active (live streaming) — reject the stale history push.
          return prev;
        }

        // If card is inactive and not being reactivated, skip pure status updates.
        // However, allow content updates (messages, tokenUsage) through — trailing
        // message_chunk / tool_call_result events arrive after the per-task stream
        // closes due to the tail loop's polling interval, and token_usage events
        // for the subagent's last LLM call are emitted on the MAIN stream which
        // typically finishes draining slightly after the per-task stream marks the
        // card inactive — dropping them here would zero out the displayed total.
        const hasContentUpdate =
          subagentDataUpdate.messages !== undefined ||
          subagentDataUpdate.tokenUsage !== undefined;
        if (isCurrentlyInactive && !isBeingReactivated && !hasContentUpdate && !isTerminalWrite) {
          // Card is inactive and not being reactivated — drop the pure status update.
          // A terminal write is exempt: a closure correcting a stale non-terminal
          // state (e.g. an errored task left reading 'active') must not be dropped.
          return prev;
        }
        // Compute resolved values before building the card
        let finalMessages: SubagentMessage[] = (() => {
          if (subagentDataUpdate.messages === undefined) {
            return existingSubagentData.messages || [];
          }
          const existing = existingSubagentData.messages || [];
          if (existing.length > 0 && subagentDataUpdate.messages!.length < existing.length) {
            return existing;
          }
          return subagentDataUpdate.messages!;
        })();

        const finalStatus: string = (() => {
          const newStatus = subagentDataUpdate.status;
          const existingStatus = existingSubagentData.status;

          if (newStatus !== undefined) {
            return newStatus;
          }

          const preservedStatus = existingStatus || 'active';
          return preservedStatus;
        })();

        const finalIsActive = subagentDataUpdate.isHistory
          ? false
          : (subagentDataUpdate.isActive !== undefined
            ? subagentDataUpdate.isActive
            : existingSubagentData.isActive !== undefined
              ? existingSubagentData.isActive
              : true);

        // Auto-finalize messages whenever the card settles — ANY terminal
        // state (completed/cancelled/error): a stopped or failed task's last
        // message must not keep its streaming spinner forever.
        if (isTerminalStatus(finalStatus) && finalMessages.length > 0) {
          finalMessages = finalMessages.map(msg => {
            if (msg.role !== 'assistant') return msg;
            const m: SubagentMessage = { ...msg, isStreaming: false };
            if (m.toolCallProcesses) {
              const procs = { ...m.toolCallProcesses };
              for (const [id, proc] of Object.entries(procs)) {
                if (proc.isInProgress) procs[id] = { ...proc, isInProgress: false, isComplete: true };
              }
              m.toolCallProcesses = procs;
            }
            if (m.reasoningProcesses) {
              const rps = { ...m.reasoningProcesses };
              for (const [id, rp] of Object.entries(rps)) {
                if (rp.isReasoning) rps[id] = { ...rp, isReasoning: false, reasoningComplete: true };
              }
              m.reasoningProcesses = rps;
            }
            return m;
          });
        }

        return {
          ...prev,
          [cardId]: {
            ...existingCard,
            subagentData: {
              ...existingSubagentData,
              ...subagentDataUpdate,
              messages: finalMessages,
              currentTool: subagentDataUpdate.currentTool !== undefined
                ? subagentDataUpdate.currentTool
                : existingSubagentData.currentTool || '',
              status: finalStatus,
              isActive: finalIsActive,
            },
          },
        };
      } else {
        // Don't create new cards for completed/inactive tasks from live streaming
        const isCompletedFromLiveStream = subagentDataUpdate.isActive === false && subagentDataUpdate.isHistory !== true && subagentDataUpdate.isReconnect !== true;

        if (isCompletedFromLiveStream && !isTerminalWrite) {
          // Inactive live update for an absent card is normally dropped (e.g. a
          // trailing message with no card to attach to). But an explicit terminal
          // write must land: a chan_close that arrives before any card exists still
          // creates the card in its settled state, so the outcome is never lost.
          return prev;
        }

        return {
          ...prev,
          [cardId]: {
            title: subagentDataUpdate.title || 'Subagent',
            subagentData: {
              agentId: agentId,
              taskId: agentId,
              description: '',
              prompt: '',
              type: 'general-purpose',
              tokenUsage: ZERO_USAGE,
              currentTool: '',
              // Spawned-but-no-signal-yet. deriveSubagentStatus promotes this to
              // 'active' the moment any content streams; an explicit live status
              // in the update below (resume/active_tasks) overrides it outright.
              status: 'initializing',
              messages: [],
              ...subagentDataUpdate,
              isActive: subagentDataUpdate.isHistory ? false : (subagentDataUpdate.isActive !== undefined ? subagentDataUpdate.isActive : true),
            },
          },
        };
      }
    });
  };

  const finalizePendingTodos = () => {
    setCards((prev) => {
      const card = prev['todo-list-card'];
      if (!Array.isArray(card?.todoData?.todos)) return prev;

      const hasIncomplete = card.todoData.todos.some(
        (t) => t.status !== 'completed' && t.status !== 'stale'
      );
      if (!hasIncomplete) return prev;

      const finalizedTodos = card.todoData.todos.map((t) =>
        t.status === 'completed' || t.status === 'stale'
          ? t
          : { ...t, status: 'stale' as const }
      );

      return {
        ...prev,
        'todo-list-card': {
          ...card,
          todoData: {
            ...card.todoData,
            todos: finalizedTodos,
            in_progress: 0,
            pending: 0,
          },
        },
      };
    });
  };

  const clearSubagentCards = () => {
    setCards((prev) => {
      const cleaned: CardsMap = {};
      Object.entries(prev).forEach(([key, value]) => {
        if (!key.startsWith('subagent-')) {
          cleaned[key] = value;
        }
      });
      return cleaned;
    });
  };

  return {
    cards,
    updateTodoListCard,
    updateSubagentCard,
    finalizePendingTodos,
    clearSubagentCards,
  };
}
