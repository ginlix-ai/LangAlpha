import { useState } from 'react';

/**
 * useFloatingCards Hook
 *
 * Manages state for subagent cards and todo list data.
 * The floating card UI (draggable, minimizable cards) has been removed —
 * subagents render via AgentSidebar + MessageList, and todos via TodoDrawer.
 * This hook still stores card data keyed by cardId for those consumers.
 *
 * @param {Object} initialCards - Initial cards configuration
 * @returns {Object} Cards state and handlers
 */
export function useFloatingCards(initialCards = {}) {
  const [floatingCards, setFloatingCards] = useState(initialCards);

  /**
   * Update or create todo list card data.
   * Called when todo list is detected/updated during live streaming.
   * @param {Object} todoData - Todo list data { todos, total, completed, in_progress, pending }
   */
  const updateTodoListCard = (todoData) => {
    const cardId = 'todo-list-card';

    setFloatingCards((prev) => {
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

  /**
   * Update or create subagent card data.
   * Called when subagent status is detected/updated during live streaming.
   * @param {string} agentId - Stable agent identity (format: "type:uuid", e.g., "research:550e8400-...")
   * @param {Object} subagentDataUpdate - Partial subagent data to merge
   */
  const updateSubagentCard = (agentId, subagentDataUpdate) => {
    const cardId = `subagent-${agentId}`;

    setFloatingCards((prev) => {
      if (prev[cardId]) {
        const existingCard = prev[cardId];
        const existingSubagentData = existingCard.subagentData || {};
        const isCurrentlyInactive = existingSubagentData.isActive === false;
        const isBeingReactivated = subagentDataUpdate.isActive === true;

        // If card is inactive and not being reactivated, skip the update entirely
        if (isCurrentlyInactive && !isBeingReactivated) {
          if (process.env.NODE_ENV === 'development') {
            console.log('[updateSubagentCard] Skipping update to inactive card:', {
              agentId,
              cardId,
              reason: 'Card is inactive and not being reactivated',
            });
          }
          return prev;
        }
        return {
          ...prev,
          [cardId]: {
            ...existingCard,
            subagentData: {
              ...existingSubagentData,
              ...subagentDataUpdate,
              messages: subagentDataUpdate.messages !== undefined
                ? subagentDataUpdate.messages
                : existingSubagentData.messages || [],
              currentTool: subagentDataUpdate.currentTool !== undefined
                ? subagentDataUpdate.currentTool
                : existingSubagentData.currentTool || '',
              status: (() => {
                const newStatus = subagentDataUpdate.status;
                const existingStatus = existingSubagentData.status;

                if (newStatus !== undefined) {
                  if (process.env.NODE_ENV === 'development') {
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
                if (process.env.NODE_ENV === 'development' && existingStatus === 'completed') {
                  console.log('[updateSubagentCard] Preserving completed status:', {
                    agentId,
                    preservedStatus,
                  });
                }
                return preservedStatus;
              })(),
              isActive: subagentDataUpdate.isHistory
                ? false
                : (subagentDataUpdate.isActive !== undefined
                  ? subagentDataUpdate.isActive
                  : existingSubagentData.isActive !== undefined
                    ? existingSubagentData.isActive
                    : true)
            },
          },
        };
      } else {
        // Don't create new cards for completed/inactive tasks from live streaming
        const isCompletedFromLiveStream = subagentDataUpdate.isActive === false && subagentDataUpdate.isHistory !== true;

        if (isCompletedFromLiveStream) {
          if (process.env.NODE_ENV === 'development') {
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
              type: 'general-purpose',
              toolCalls: 0,
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

  /**
   * Inactivate all subagent cards.
   * Called at the end of streaming to mark all subagents as inactive.
   */
  const inactivateAllSubagents = () => {
    setFloatingCards((prev) => {
      const updated = { ...prev };
      let hasChanges = false;

      Object.keys(updated).forEach((cardId) => {
        if (cardId.startsWith('subagent-') && updated[cardId]?.subagentData) {
          const card = updated[cardId];
          if (card.subagentData.isActive !== false) {
            updated[cardId] = {
              ...card,
              subagentData: {
                ...card.subagentData,
                isActive: false,
                status: 'completed',
                currentTool: '',
              },
            };
            hasChanges = true;
            if (process.env.NODE_ENV === 'development') {
              console.log('[inactivateAllSubagents] Marking subagent as inactive:', {
                taskId: card.subagentData.taskId,
                cardId,
                previousStatus: card.subagentData.status,
              });
            }
          }
        }
      });

      return hasChanges ? updated : prev;
    });
  };

  /**
   * Complete all pending todos in the todo list card.
   * Called at the end of streaming to mark remaining in_progress/pending items as completed.
   */
  const completePendingTodos = () => {
    setFloatingCards((prev) => {
      const card = prev['todo-list-card'];
      if (!card?.todoData?.todos) return prev;

      const hasIncomplete = card.todoData.todos.some((t) => t.status !== 'completed');
      if (!hasIncomplete) return prev;

      const completedTodos = card.todoData.todos.map((t) => ({
        ...t,
        status: 'completed',
      }));

      return {
        ...prev,
        'todo-list-card': {
          ...card,
          todoData: {
            ...card.todoData,
            todos: completedTodos,
            completed: card.todoData.total || completedTodos.length,
            in_progress: 0,
            pending: 0,
          },
        },
      };
    });
  };

  /**
   * Minimize all inactive subagent cards (no-op now, kept for API compatibility).
   * Previously used to minimize floating cards; now subagents are shown via sidebar.
   */
  const minimizeInactiveSubagents = () => {
    // No-op: floating card minimize is no longer needed
  };

  return {
    floatingCards,
    updateTodoListCard,
    updateSubagentCard,
    inactivateAllSubagents,
    minimizeInactiveSubagents,
    completePendingTodos,
  };
}
