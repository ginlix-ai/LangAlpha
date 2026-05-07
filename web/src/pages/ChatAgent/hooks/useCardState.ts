import { useState } from 'react';
import { type SubagentTokenUsage, ZERO_USAGE } from '../utils/tokenUsage';

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
  inactivateAllSubagents: () => void;
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
          if (import.meta.env.DEV) {
            console.log('[updateSubagentCard] Skipping history overwrite on active card:', {
              agentId,
              cardId,
              reason: 'Card is active (live streaming) — history push rejected',
            });
          }
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
        if (isCurrentlyInactive && !isBeingReactivated && !hasContentUpdate) {
          if (import.meta.env.DEV) {
            console.log('[updateSubagentCard] Skipping update to inactive card:', {
              agentId,
              cardId,
              reason: 'Card is inactive and not being reactivated (no content update)',
            });
          }
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
            if (import.meta.env.DEV) {
              console.log('[updateSubagentCard] Status update:', {
                agentId,
                newStatus,
                previousStatus: existingStatus,
                willUpdate: newStatus !== existingStatus,
              });
            }
            return newStatus;
          }

          const preservedStatus = existingStatus || 'active';
          if (import.meta.env.DEV && existingStatus === 'completed') {
            console.log('[updateSubagentCard] Preserving completed status:', {
              agentId,
              preservedStatus,
            });
          }
          return preservedStatus;
        })();

        const finalIsActive = subagentDataUpdate.isHistory
          ? false
          : (subagentDataUpdate.isActive !== undefined
            ? subagentDataUpdate.isActive
            : existingSubagentData.isActive !== undefined
              ? existingSubagentData.isActive
              : true);

        // Auto-finalize messages whenever the card is in completed state.
        if (finalStatus === 'completed' && finalMessages.length > 0) {
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

        if (isCompletedFromLiveStream) {
          if (import.meta.env.DEV) {
            console.log('[updateSubagentCard] Skipping creation of new card for completed task from live streaming:', {
              agentId,
              cardId,
              reason: 'Completed tasks from live streaming should only update existing cards, not create new ones',
              isActive: subagentDataUpdate.isActive,
              isHistory: subagentDataUpdate.isHistory,
            });
          }
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
              status: 'active',
              messages: [],
              ...subagentDataUpdate,
              isActive: subagentDataUpdate.isHistory ? false : (subagentDataUpdate.isActive !== undefined ? subagentDataUpdate.isActive : true),
            },
          },
        };
      }
    });
  };

  const inactivateAllSubagents = () => {
    setCards((prev) => {
      const updated = { ...prev };
      let hasChanges = false;

      Object.keys(updated).forEach((cardId) => {
        if (cardId.startsWith('subagent-') && updated[cardId]?.subagentData) {
          const card = updated[cardId];
          if (card.subagentData!.isActive !== false) {
            const msgs = card.subagentData!.messages;
            let finalizedMsgs = msgs;
            if (msgs && msgs.length > 0) {
              finalizedMsgs = msgs.map(msg => {
                if (msg.role !== 'assistant') return msg;
                const m: SubagentMessage = { ...msg, isStreaming: false };
                if (m.toolCallProcesses) {
                  const procs = { ...m.toolCallProcesses };
                  for (const [id, proc] of Object.entries(procs)) {
                    if (proc.isInProgress) {
                      procs[id] = { ...proc, isInProgress: false, isComplete: true };
                    }
                  }
                  m.toolCallProcesses = procs;
                }
                if (m.reasoningProcesses) {
                  const rps = { ...m.reasoningProcesses };
                  for (const [id, rp] of Object.entries(rps)) {
                    if (rp.isReasoning) {
                      rps[id] = { ...rp, isReasoning: false, reasoningComplete: true };
                    }
                  }
                  m.reasoningProcesses = rps;
                }
                return m;
              });
            }

            updated[cardId] = {
              ...card,
              subagentData: {
                ...card.subagentData,
                isActive: false,
                status: 'completed',
                currentTool: '',
                messages: finalizedMsgs,
              },
            };
            hasChanges = true;
            if (import.meta.env.DEV) {
              console.log('[inactivateAllSubagents] Marking subagent as inactive:', {
                taskId: card.subagentData!.taskId,
                cardId,
                previousStatus: card.subagentData!.status,
              });
            }
          }
        }
      });

      return hasChanges ? updated : prev;
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
    inactivateAllSubagents,
    finalizePendingTodos,
    clearSubagentCards,
  };
}
