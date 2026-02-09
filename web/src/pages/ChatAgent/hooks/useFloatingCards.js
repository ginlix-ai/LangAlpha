import { useState } from 'react';

/**
 * useFloatingCards Hook
 * 
 * Manages state and handlers for floating cards in the ChatView.
 * Encapsulates all floating card logic including:
 * - Card state management (position, z-index, minimize state)
 * - Z-index management for bring-to-front functionality
 * - Minimization order tracking
 * - Card interaction handlers
 * 
 * @param {Object} initialCards - Initial cards configuration
 * @returns {Object} Floating cards state and handlers
 */
export function useFloatingCards(initialCards = {}) {
  // Floating cards state management
  // Structure: { [cardId]: { title: string, isMinimized: boolean, position: { x, y }, zIndex: number, minimizeOrder: number, hasUnreadUpdate: boolean, content: ReactNode } }
  // zIndex: Higher values appear on top. Starts at 50, increments when card is interacted with.
  // minimizeOrder: Order in which cards were minimized (lower number = minimized earlier)
  // hasUnreadUpdate: Whether the card has been updated while minimized (for visual indicator)
  const [floatingCards, setFloatingCards] = useState(initialCards);

  // Track the highest z-index to assign to newly interacted cards
  const [maxZIndex, setMaxZIndex] = useState(() => {
    // Find the highest z-index from initial cards
    const initialZIndices = Object.values(initialCards).map(card => card.zIndex || 50);
    return initialZIndices.length > 0 ? Math.max(51, ...initialZIndices) : 51;
  });

  // Track the next minimize order number
  const [nextMinimizeOrder, setNextMinimizeOrder] = useState(0);

  /**
   * Handle floating card minimize
   * Sets the card to minimized state and assigns a minimize order
   */
  const handleCardMinimize = (cardId) => {
    setFloatingCards((prev) => ({
      ...prev,
      [cardId]: {
        ...prev[cardId],
        isMinimized: true,
        minimizeOrder: nextMinimizeOrder, // Set the order when minimized
      },
    }));
    setNextMinimizeOrder((prev) => prev + 1);
  };

  /**
   * Handle floating card maximize (from icon click)
   * Restores the card from minimized state and brings it to front
   */
  const handleCardMaximize = (cardId) => {
    setFloatingCards((prev) => ({
      ...prev,
      [cardId]: {
        ...prev[cardId],
        isMinimized: false,
        minimizeOrder: null, // Clear minimize order when restored
        zIndex: maxZIndex + 1, // Bring to front when restored
        hasUnreadUpdate: false, // Clear unread update indicator when card is opened
      },
    }));
    setMaxZIndex((prev) => prev + 1);
  };

  /**
   * Handle floating card toggle (minimize/maximize from bookmark icon)
   * Toggles the card's minimized state
   */
  const handleCardToggle = (cardId) => {
    setFloatingCards((prev) => {
      const card = prev[cardId];
      if (!card) return prev;
      
      if (card.isMinimized) {
        // Currently minimized - maximize it
        const newZIndex = maxZIndex + 1;
        setMaxZIndex(newZIndex);
        return {
          ...prev,
          [cardId]: {
            ...card,
            isMinimized: false,
            minimizeOrder: null,
            zIndex: newZIndex,
            hasUnreadUpdate: false, // Clear unread update indicator when card is opened
          },
        };
      } else {
        // Currently maximized - minimize it
        const currentMinimizeOrder = nextMinimizeOrder;
        setNextMinimizeOrder((prev) => prev + 1);
        return {
          ...prev,
          [cardId]: {
            ...card,
            isMinimized: true,
            minimizeOrder: currentMinimizeOrder,
          },
        };
      }
    });
  };

  /**
   * Handle floating card position change
   * Updates the card's position when dragged
   */
  const handleCardPositionChange = (cardId, newPosition) => {
    setFloatingCards((prev) => ({
      ...prev,
      [cardId]: {
        ...prev[cardId],
        position: newPosition,
      },
    }));
  };

  /**
   * Handle bringing card to front when interacted with
   * Increments z-index to bring the card on top of others
   * Also clears unread update indicator when user interacts with the card
   */
  const handleBringToFront = (cardId) => {
    setFloatingCards((prev) => {
      const newZIndex = maxZIndex + 1;
      setMaxZIndex(newZIndex);
      return {
        ...prev,
        [cardId]: {
          ...prev[cardId],
          zIndex: newZIndex,
          hasUnreadUpdate: false, // Clear unread update indicator when user interacts with card
        },
      };
    });
  };

  /**
   * Get minimized cards sorted by minimize order
   * Returns array of [cardId, card] tuples sorted by minimizeOrder
   */
  const getMinimizedCards = () => {
    return Object.entries(floatingCards)
      .filter(([_, card]) => card.isMinimized)
      .sort(([_, cardA], [__, cardB]) => {
        // Sort by minimizeOrder (lower number = minimized earlier, appears first)
        const orderA = cardA.minimizeOrder ?? Infinity;
        const orderB = cardB.minimizeOrder ?? Infinity;
        return orderA - orderB;
      });
  };

  /**
   * Get all floating cards (for displaying bookmark icons)
   * Returns array of [cardId, card] tuples
   * Minimized cards are sorted by minimizeOrder, non-minimized cards come after
   */
  const getAllCards = () => {
    const minimized = Object.entries(floatingCards)
      .filter(([_, card]) => card.isMinimized)
      .sort(([_, cardA], [__, cardB]) => {
        const orderA = cardA.minimizeOrder ?? Infinity;
        const orderB = cardB.minimizeOrder ?? Infinity;
        return orderA - orderB;
      });
    
    const notMinimized = Object.entries(floatingCards)
      .filter(([_, card]) => !card.isMinimized);
    
    return [...minimized, ...notMinimized];
  };

  /**
   * Update or create todo list floating card
   * Called when todo list is detected/updated during live streaming
   * @param {Object} todoData - Todo list data { todos, total, completed, in_progress, pending }
   * @param {boolean} isNewConversation - Whether this is a new conversation (should overwrite existing card)
   */
  const updateTodoListCard = (todoData, isNewConversation = false) => {
    const cardId = 'todo-list-card';
    
    setFloatingCards((prev) => {
      // Calculate default position - same as subagent cards
      // Card width is ~520px (same as subagent cards), so center it horizontally
      const getDefaultPosition = () => {
        // Use window.innerWidth if available, otherwise default to a reasonable value
        const windowWidth = typeof window !== 'undefined' ? window.innerWidth : 1920;
        // Center horizontally: windowWidth / 1.25 - 260 (same calculation as subagent cards)
        const centeredX = (windowWidth / 1.25) - 260;
        return {
          x: Math.max(80, centeredX), // Ensure some margin from the left edge
          y: 80, // Same vertical position as first subagent card
        };
      };

      // If card exists, ALWAYS merge the update with existing data (same pattern as subagent cards)
      // This ensures the card persists even after streaming ends
      // IMPORTANT: Preserve existing position object reference and all state to prevent reconstruction
      // During live streaming updates, we should NEVER remove/recreate the card, only update its data
      if (prev[cardId]) {
        const existingCard = prev[cardId];
        return {
          ...prev,
          [cardId]: {
            ...existingCard, // Preserve all existing state (isMinimized, zIndex, minimizeOrder, position, etc.)
            // Update content with new todo data
            todoData: todoData,
            // Keep existing position object reference (don't create new object)
            // This prevents FloatingCard from resetting position when todoData updates
            position: existingCard.position, // Preserve exact same object reference
            // Keep existing zIndex, minimize state, minimizeOrder - all preserved via ...existingCard
            // Set hasUnreadUpdate to true only if card is minimized (user can't see updates)
            // If card is not minimized, user can see updates directly, so no need for green indicator
            // No auto-open: isMinimized state is preserved, so card stays minimized if it was minimized
            hasUnreadUpdate: existingCard.isMinimized ? true : false,
          },
        };
      } else {
        // Create new todo list card (first time todo list is detected)
        // Use default position (same as subagent cards)
        // Note: isNewConversation is only used to determine if we should create a new card,
        // but if card already exists, we always preserve its state (handled above)
        const newZIndex = maxZIndex + 1;
        setMaxZIndex(newZIndex);
        return {
          ...prev,
          [cardId]: {
            title: 'Todo List',
            isMinimized: false,
            position: getDefaultPosition(),
            zIndex: newZIndex,
            minimizeOrder: null,
            hasUnreadUpdate: false,
            todoData: todoData,
          },
        };
      }
    });
  };

  /**
   * Update or create subagent floating card
   * Called when subagent status is detected/updated during live streaming
   * @param {string} agentId - Stable agent identity (format: "type:uuid", e.g., "research:550e8400-...")
   * @param {Object} subagentDataUpdate - Partial subagent data to merge { agentId, displayId, taskId, description, type, toolCalls, currentTool, status, messages }
   */
  const updateSubagentCard = (agentId, subagentDataUpdate) => {
    const cardId = `subagent-${agentId}`;
    
    setFloatingCards((prev) => {
      // Calculate default position closer to the center of the window
      // Card width is ~520px, so center it horizontally with a small left margin.
      const getDefaultPosition = () => {
        // Use window.innerWidth if available, otherwise default to a reasonable value
        const windowWidth = typeof window !== 'undefined' ? window.innerWidth : 1920;
        // Center horizontally: windowWidth / 2 - cardWidth / 2 (~260px)
        // Offset vertically based on number of existing ACTIVE subagent cards only
        // This ensures new cards stack properly on top of active cards, ignoring inactive ones
        const activeSubagentCards = Object.keys(prev).filter(id => {
          if (!id.startsWith('subagent-')) return false;
          const card = prev[id];
          // Only count active cards (isActive !== false)
          return card?.subagentData?.isActive !== false;
        });
        const verticalOffset = activeSubagentCards.length * 120; // 120px spacing between cards (reduced from 350px)
        const centeredX = (windowWidth / 1.25) - 260;
        return {
          x: Math.max(80, centeredX), // Ensure some margin from the left edge
          y: 80 + verticalOffset,
        };
      };

      // If card exists, check if it's inactive
      if (prev[cardId]) {
        const existingCard = prev[cardId];
        const existingSubagentData = existingCard.subagentData || {};
        const isCurrentlyInactive = existingSubagentData.isActive === false;
        const isBeingReactivated = subagentDataUpdate.isActive === true;
        
        // If card is inactive and not being reactivated, skip the update entirely
        // This prevents inactive cards from receiving updates when new subagents with the same task ID are created
        if (isCurrentlyInactive && !isBeingReactivated) {
          if (process.env.NODE_ENV === 'development') {
            console.log('[updateSubagentCard] Skipping update to inactive card:', {
              agentId,
              cardId,
              reason: 'Card is inactive and not being reactivated',
            });
          }
          return prev; // Return previous state unchanged
        }
        return {
          ...prev,
          [cardId]: {
            ...existingCard,
            // Merge subagent data (preserve existing messages, currentTool, and status, update other fields)
            // IMPORTANT: Order matters - we spread existingSubagentData first, then subagentDataUpdate,
            // then override specific fields to ensure proper preservation logic
            subagentData: {
              ...existingSubagentData,
              ...subagentDataUpdate,
              // If messages are provided, use them; otherwise keep existing
              messages: subagentDataUpdate.messages !== undefined
                ? subagentDataUpdate.messages
                : existingSubagentData.messages || [],
              // If currentTool is explicitly provided (including empty string), use it
              // Empty string means "clear currentTool", undefined means "preserve existing"
              // This allows explicit clearing when tool calls fail or complete
              currentTool: subagentDataUpdate.currentTool !== undefined
                ? subagentDataUpdate.currentTool // Use provided value (even if empty string)
                : existingSubagentData.currentTool || '', // Preserve only if not explicitly provided
              // Status handling: if explicitly provided, use it; otherwise preserve existing
              // This ensures 'completed' status from handleSubagentStatus is not overwritten
              // IMPORTANT: Once a task is 'completed', it should stay 'completed' unless explicitly changed
              status: (() => {
                const newStatus = subagentDataUpdate.status;
                const existingStatus = existingSubagentData.status;
                
                if (newStatus !== undefined) {
                  // Explicit status update (e.g., 'completed' from subagent_status)
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
                
                // Preserve existing status - if it's 'completed', keep it 'completed'
                // Only default to 'active' if status was never set
                const preservedStatus = existingStatus || 'active';
                if (process.env.NODE_ENV === 'development' && existingStatus === 'completed') {
                  console.log('[updateSubagentCard] Preserving completed status:', {
                    agentId,
                    preservedStatus,
                  });
                }
                return preservedStatus;
              })(),
              // isActive handling: if explicitly provided, use it; otherwise preserve existing
              // Default to true for new cards, but preserve existing value for updates
              // IMPORTANT: If loading from history, always mark as inactive to prevent duplicate card creation
              isActive: subagentDataUpdate.isHistory 
                ? false // Force inactive if loading from history
                : (subagentDataUpdate.isActive !== undefined
                  ? subagentDataUpdate.isActive
                  : existingSubagentData.isActive !== undefined
                    ? existingSubagentData.isActive
                    : true) // Default to active if not set
            },
            // Keep existing position object reference (don't create new object)
            // This prevents FloatingCard from resetting position when subagentData updates
            position: existingCard.position, // Preserve exact same object reference
            // Keep existing zIndex, minimize state
            // Set hasUnreadUpdate to true only if card is minimized AND active (user can't see updates)
            // Inactive cards should never show unread updates, even if minimized
            // If card is not minimized, user can see updates directly, so no need for green indicator
            hasUnreadUpdate: (() => {
              // Determine if card will be active after this update
              const willBeActive = subagentDataUpdate.isActive !== undefined
                ? subagentDataUpdate.isActive
                : existingSubagentData.isActive !== false; // Default to true if not set
              // Only show unread update if card is minimized AND will be active
              return existingCard.isMinimized && willBeActive;
            })(),
          },
        };
      } else {
        // Create new subagent card (first time subagent is detected)
        // IMPORTANT: Don't create new cards for completed/inactive tasks from live streaming
        // Completed tasks from live streaming should only update existing cards (e.g., from history)
        // New cards should only be created for:
        // 1. Active tasks in live streaming (isActive !== false and not from history)
        // 2. Cards explicitly opened from history by user (isHistory: true)
        const isCompletedFromLiveStream = subagentDataUpdate.isActive === false && subagentDataUpdate.isHistory !== true;
        
        if (isCompletedFromLiveStream) {
          // Don't create a new card for completed tasks from live streaming
          // These should only update existing cards (e.g., cards opened from history)
          // If no card exists, it means this task is from history and hasn't been opened yet
          if (process.env.NODE_ENV === 'development') {
            console.log('[updateSubagentCard] Skipping creation of new card for completed task from live streaming:', {
              agentId,
              cardId,
              reason: 'Completed tasks from live streaming should only update existing cards, not create new ones',
              isActive: subagentDataUpdate.isActive,
              isHistory: subagentDataUpdate.isHistory,
            });
          }
          return prev; // Return previous state unchanged (don't create card)
        }
        
        // Use default position on the right side
        const newZIndex = maxZIndex + 1;
        setMaxZIndex(newZIndex);
        return {
          ...prev,
          [cardId]: {
            // Use a human-friendly title for subagent cards.
            // If a custom title is ever provided, prefer it; otherwise default to "Subagent".
            title: subagentDataUpdate.title || 'Subagent',
            isMinimized: false,
            position: getDefaultPosition(),
            zIndex: newZIndex,
            minimizeOrder: null,
            hasUnreadUpdate: false,
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
              // Force isActive to false if loading from history, regardless of what was provided
              // This prevents duplicate floating card creation when user calls subagent in the future
              isActive: subagentDataUpdate.isHistory ? false : (subagentDataUpdate.isActive !== undefined ? subagentDataUpdate.isActive : true),
            },
          },
        };
      }
    });
  };

  /**
   * Inactivate all subagent cards
   * Called at the end of streaming to mark all subagents as inactive
   * This prevents task ID collisions when new subagents are created with the same IDs
   * Also forcefully sets status to 'completed' and clears currentTool when subagent becomes inactive
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
                status: 'completed', // Forcefully set status to 'completed' when subagent becomes inactive
                currentTool: '', // Clear currentTool when subagent becomes inactive
              },
            };
            hasChanges = true;
            if (process.env.NODE_ENV === 'development') {
              console.log('[inactivateAllSubagents] Marking subagent as inactive, setting status to completed, and clearing currentTool:', {
                taskId: card.subagentData.taskId,
                cardId,
                previousStatus: card.subagentData.status,
                previousCurrentTool: card.subagentData.currentTool,
              });
            }
          }
        }
      });

      return hasChanges ? updated : prev;
    });
  };

  /**
   * Complete all pending todos in the floating todo list card
   * Called at the end of streaming to mark remaining in_progress/pending items as completed
   * No-op if no todo card exists or all items are already completed
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
   * Minimize all inactive subagent cards
   * Called at the end of streaming to minimize inactive cards
   * This keeps the UI clean by hiding cards from previous conversations
   */
  const minimizeInactiveSubagents = () => {
    setFloatingCards((prev) => {
      const updated = { ...prev };
      let hasChanges = false;
      let currentMinimizeOrder = 0;

      // Find the highest minimize order to continue from
      Object.values(updated).forEach((card) => {
        if (card.minimizeOrder !== null && card.minimizeOrder > currentMinimizeOrder) {
          currentMinimizeOrder = card.minimizeOrder;
        }
      });

      Object.keys(updated).forEach((cardId) => {
        if (cardId.startsWith('subagent-') && updated[cardId]?.subagentData) {
          const card = updated[cardId];
          // Only minimize if card is inactive and not already minimized
          if (card.subagentData.isActive === false && !card.isMinimized) {
            currentMinimizeOrder++;
            updated[cardId] = {
              ...card,
              isMinimized: true,
              minimizeOrder: currentMinimizeOrder,
            };
            hasChanges = true;
            if (process.env.NODE_ENV === 'development') {
              console.log('[minimizeInactiveSubagents] Minimizing inactive subagent:', {
                taskId: card.subagentData.taskId,
                cardId,
                minimizeOrder: currentMinimizeOrder,
              });
            }
          }
        }
      });

      return hasChanges ? updated : prev;
    });
  };

  return {
    // State
    floatingCards,
    
    // Handlers
    handleCardMinimize,
    handleCardMaximize,
    handleCardToggle,
    handleCardPositionChange,
    handleBringToFront,
    
    // Helpers
    getMinimizedCards,
    getAllCards,
    inactivateAllSubagents,
    minimizeInactiveSubagents,
    completePendingTodos,
    updateTodoListCard,
    updateSubagentCard,
  };
}
