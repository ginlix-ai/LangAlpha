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
      // Calculate default position on the right side of the window
      // Card width is 320px, so position it with some margin from the right edge
      const getDefaultPosition = () => {
        // Use window.innerWidth if available, otherwise default to a reasonable value
        const windowWidth = typeof window !== 'undefined' ? window.innerWidth : 1920;
        // Position on the right side: window width - card width (320px) - margin (20px)
        return {
          x: Math.max(100, windowWidth - 340), // Ensure at least 100px from left edge
          y: 100,
        };
      };

      // If new conversation (new thread), remove existing todo card first, then create new one
      if (isNewConversation && prev[cardId]) {
        const { [cardId]: removed, ...rest } = prev;
        // Create new card for new conversation with default right-side position
        const newZIndex = maxZIndex + 1;
        setMaxZIndex(newZIndex);
        return {
          ...rest,
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

      // If card exists, always update it (never remove it)
      // This ensures the card persists even after streaming ends
      // IMPORTANT: Preserve existing position object reference to prevent position reset
      if (prev[cardId]) {
        const existingCard = prev[cardId];
        return {
          ...prev,
          [cardId]: {
            ...existingCard,
            // Update content with new todo data
            todoData: todoData,
            // Keep existing position object reference (don't create new object)
            // This prevents FloatingCard from resetting position when todoData updates
            position: existingCard.position, // Preserve exact same object reference
            // Keep existing zIndex, minimize state
            // Set hasUnreadUpdate to true only if card is minimized (user can't see updates)
            // If card is not minimized, user can see updates directly, so no need for green indicator
            hasUnreadUpdate: existingCard.isMinimized ? true : false,
          },
        };
      } else {
        // Create new todo list card (first time todo list is detected)
        // Use default position on the right side
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
   * @param {string} taskId - Task ID (e.g., "Task-1")
   * @param {Object} subagentDataUpdate - Partial subagent data to merge { taskId, description, type, toolCalls, currentTool, status, messages }
   */
  const updateSubagentCard = (taskId, subagentDataUpdate) => {
    const cardId = `subagent-${taskId}`;
    
    setFloatingCards((prev) => {
      // Calculate default position closer to the center of the window
      // Card width is ~520px, so center it horizontally with a small left margin.
      const getDefaultPosition = () => {
        // Use window.innerWidth if available, otherwise default to a reasonable value
        const windowWidth = typeof window !== 'undefined' ? window.innerWidth : 1920;
        // Center horizontally: windowWidth / 2 - cardWidth / 2 (~260px)
        // Offset vertically based on number of existing subagent cards
        const existingSubagentCards = Object.keys(prev).filter(id => id.startsWith('subagent-'));
        const verticalOffset = existingSubagentCards.length * 350; // 350px spacing between cards
        const centeredX = (windowWidth / 1.25) - 260;
        return {
          x: Math.max(80, centeredX), // Ensure some margin from the left edge
          y: 80 + verticalOffset,
        };
      };

      // If card exists, merge the update with existing data
      // This ensures the card persists even after streaming ends
      // IMPORTANT: Preserve existing position object reference to prevent position reset
      if (prev[cardId]) {
        const existingCard = prev[cardId];
        const existingSubagentData = existingCard.subagentData || {};
        return {
          ...prev,
          [cardId]: {
            ...existingCard,
            // Merge subagent data (preserve existing messages, update other fields)
            subagentData: {
              ...existingSubagentData,
              ...subagentDataUpdate,
              // If messages are provided, use them; otherwise keep existing
              messages: subagentDataUpdate.messages !== undefined 
                ? subagentDataUpdate.messages 
                : existingSubagentData.messages || [],
            },
            // Keep existing position object reference (don't create new object)
            // This prevents FloatingCard from resetting position when subagentData updates
            position: existingCard.position, // Preserve exact same object reference
            // Keep existing zIndex, minimize state
            // Set hasUnreadUpdate to true only if card is minimized (user can't see updates)
            // If card is not minimized, user can see updates directly, so no need for green indicator
            hasUnreadUpdate: existingCard.isMinimized ? true : false,
          },
        };
      } else {
        // Create new subagent card (first time subagent is detected)
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
              taskId: taskId,
              description: '',
              type: 'general-purpose',
              toolCalls: 0,
              currentTool: '',
              status: 'active',
              messages: [],
              ...subagentDataUpdate,
            },
          },
        };
      }
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
    updateTodoListCard,
    updateSubagentCard,
  };
}
