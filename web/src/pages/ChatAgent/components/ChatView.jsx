import React, { useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import { ScrollArea } from '../../../components/ui/scroll-area';
import ChatInput from './ChatInput';
import MessageList from './MessageList';
import FloatingCard from './FloatingCard';
import FloatingCardIcon from './FloatingCardIcon';
import TodoListCardContent from './TodoListCardContent';
import SubagentCardContent from './SubagentCardContent';
import { useChatMessages } from '../hooks/useChatMessages';
import { useFloatingCards } from '../hooks/useFloatingCards';

/**
 * ChatView Component
 * 
 * Displays the chat interface for a specific workspace and thread.
 * Handles:
 * - Message display and streaming
 * - Auto-scrolling
 * - Navigation back to thread gallery
 * - Auto-sending initial message from navigation state
 * 
 * @param {string} workspaceId - The workspace ID to chat in
 * @param {string} threadId - The thread ID to chat in
 * @param {Function} onBack - Callback to navigate back to thread gallery
 */
function ChatView({ workspaceId, threadId, onBack }) {
  const scrollAreaRef = useRef(null);
  const location = useLocation();
  const navigate = useNavigate();
  const initialMessageSentRef = useRef(false);

  // Floating cards management - extracted to custom hook for better encapsulation
  // Must be called before useChatMessages since updateTodoListCard and updateSubagentCard are passed to it
  const {
    floatingCards,
    handleCardMinimize,
    handleCardMaximize,
    handleCardToggle,
    handleCardPositionChange,
    handleBringToFront,
    getMinimizedCards,
    getAllCards,
    updateTodoListCard,
    updateSubagentCard,
    inactivateAllSubagents,
    minimizeInactiveSubagents,
  } = useFloatingCards();

  // Chat messages management - receives updateTodoListCard and updateSubagentCard from floating cards hook
  const { 
    messages, 
    isLoading, 
    isLoadingHistory, 
    messageError, 
    handleSendMessage, 
    threadId: currentThreadId,
    getSubagentHistory,
  } = useChatMessages(workspaceId, threadId, updateTodoListCard, updateSubagentCard, inactivateAllSubagents, minimizeInactiveSubagents);

  // Update URL when thread ID changes (e.g., when __default__ becomes actual thread ID)
  // This triggers a re-render with the new threadId, which will then load history
  useEffect(() => {
    if (currentThreadId && currentThreadId !== '__default__' && currentThreadId !== threadId && workspaceId) {
      console.log('[ChatView] Thread ID changed from', threadId, 'to', currentThreadId, '- updating URL');
      // Update URL to reflect the actual thread ID
      // This will cause ChatAgent to re-render with new threadId prop, triggering history load
      navigate(`/chat/${workspaceId}/${currentThreadId}`, { replace: true });
    }
  }, [currentThreadId, threadId, workspaceId, navigate]);

  // Auto-send initial message from navigation state (e.g., from Dashboard)
  useEffect(() => {
    // Only proceed if we have the required IDs
    if (!workspaceId || !threadId) {
      return;
    }

    // Handle onboarding flow
    if (location.state?.isOnboarding && !initialMessageSentRef.current && !isLoading && !isLoadingHistory) {
      initialMessageSentRef.current = true;
      // Clear navigation state to prevent re-sending on re-renders
      navigate(location.pathname, { replace: true, state: {} });
      // Small delay to ensure component is fully mounted
      setTimeout(() => {
        const onboardingMessage = "Hi! I am new here and would like to set up my profile.";
        const additionalContext = [
          {
            type: "skills",
            name: "user-profile",
            instruction: "Help the user with first time onboarding. Reference the skills/user-profile/onboarding.md for details. You should use load_skill tool to load the user-profile skill before calling any of the tools."
          }
        ];
        handleSendMessage(onboardingMessage, false, additionalContext);
      }, 100);
      return;
    }
    
    // Handle regular message flow
    if (location.state?.initialMessage && !initialMessageSentRef.current) {
      // For new threads (__default__), send immediately without waiting for history
      // For existing threads, wait for history to finish loading
      if (threadId === '__default__') {
        // New thread - send immediately
        initialMessageSentRef.current = true;
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          const { initialMessage, planMode } = location.state;
          handleSendMessage(initialMessage, planMode || false);
        }, 100);
      } else if (!isLoadingHistory && !isLoading) {
        // Existing thread - wait for history to load, then send
        // This ensures we don't send duplicate messages
        initialMessageSentRef.current = true;
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          const { initialMessage, planMode } = location.state;
          handleSendMessage(initialMessage, planMode || false);
        }, 100);
      }
    }
  }, [location.state, workspaceId, threadId, isLoading, isLoadingHistory, handleSendMessage, navigate, location.pathname]);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    if (scrollAreaRef.current) {
      // ScrollArea component has a nested structure with overflow-auto
      const scrollContainer = scrollAreaRef.current.querySelector('.overflow-auto') ||
                             scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]') ||
                             scrollAreaRef.current;
      if (scrollContainer) {
        // Use setTimeout to ensure DOM is updated
        setTimeout(() => {
          scrollContainer.scrollTop = scrollContainer.scrollHeight;
        }, 0);
      }
    }
  }, [messages]);


  // Early return if workspaceId or threadId is missing
  if (!workspaceId || !threadId) {
    return (
      <div className="flex items-center justify-center h-full" style={{ backgroundColor: '#1B1D25' }}>
        <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.65 }}>
          Missing workspace or thread information
        </p>
      </div>
    );
  }

  return (
    <div className="chat-agent-container" style={{ backgroundColor: '#1B1D25' }}>
      {/* Header */}
      <div
        className="flex items-center justify-between px-6 py-4 flex-shrink-0"
        style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}
      >
        <div className="flex items-center gap-4">
          <button
            onClick={onBack}
            className="p-2 rounded-md transition-colors hover:bg-white/10"
            style={{ color: '#FFFFFF' }}
            title="Back to threads"
          >
            <ArrowLeft className="h-5 w-5" />
          </button>
          <h1 className="text-lg font-semibold" style={{ color: '#FFFFFF' }}>
            Chat Agent
          </h1>
          {isLoadingHistory && (
            <span className="text-xs" style={{ color: '#FFFFFF', opacity: 0.5 }}>
              Loading history...
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {/* Floating card icons for all cards - always visible */}
          {getAllCards().map(([cardId, card]) => {
            // Check if this is a subagent card and get its isActive status
            const isSubagentCard = cardId.startsWith('subagent-');
            const isActive = isSubagentCard && card.subagentData
              ? card.subagentData.isActive !== false // Default to true if not set
              : true; // Non-subagent cards are always considered active
            
            return (
              <FloatingCardIcon
                key={cardId}
                id={cardId}
                title={card.title || 'Card'}
                onClick={() => handleCardToggle(cardId)}
                hasUnreadUpdate={card.hasUnreadUpdate || false}
                isActive={isActive}
              />
            );
          })}
          {messageError && (
            <p className="text-xs" style={{ color: '#FF383C' }}>
              {messageError}
            </p>
          )}
        </div>
      </div>

      {/* Messages Area - Fixed height, scrollable */}
      <div 
        className="flex-1 overflow-hidden"
        style={{ 
          minHeight: 0,
          height: 0, // Force flex-1 to work properly
        }}
      >
        <ScrollArea ref={scrollAreaRef} className="h-full w-full">
          <div className="px-6 py-4">
            <MessageList 
              messages={messages} 
              onOpenSubagentTask={(subagentInfo) => {
                console.log('[ChatView] onOpenSubagentTask called with:', subagentInfo);
                const { subagentId, description, type, status } = subagentInfo;

                if (!updateSubagentCard) {
                  console.error('[ChatView] updateSubagentCard is not defined!');
                  return;
                }

                // Check if card already exists and is minimized
                const cardId = `subagent-${subagentId}`;
                const existingCard = floatingCards[cardId];
                const isMinimized = existingCard?.isMinimized || false;

                // If card exists and is minimized, maximize it first
                if (existingCard && isMinimized) {
                  console.log('[ChatView] Card is minimized, maximizing it');
                  handleCardMaximize(cardId);
                }

                // Try to load history for this subagent (if available)
                const history = getSubagentHistory
                  ? getSubagentHistory(subagentId)
                  : null;

                const finalDescription = history?.description || description || '';
                const finalType = history?.type || type || 'general-purpose';
                const finalStatus = history?.status || status || 'unknown';
                const finalMessages = history?.messages || [];

                console.log('[ChatView] Opening subagent card with history:', {
                  subagentId,
                  hasHistory: !!history,
                  messagesCount: finalMessages.length,
                });

                updateSubagentCard(subagentId, {
                  taskId: subagentId,
                  description: finalDescription,
                  type: finalType,
                  status: finalStatus,
                  toolCalls: 0,
                  currentTool: '',
                  messages: finalMessages,
                  isHistory: !!history,
                  isActive: !history, // Mark as inactive if loading from history to prevent duplicate card creation
                });
              }}
            />
          </div>
        </ScrollArea>
      </div>

      {/* Input Area */}
      <div className="flex-shrink-0 p-4" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)' }}>
        <ChatInput onSend={handleSendMessage} disabled={isLoading || isLoadingHistory || !workspaceId} />
      </div>

      {/* Floating Cards */}
      {Object.entries(floatingCards).map(([cardId, card]) => (
        <FloatingCard
          key={cardId}
          id={cardId}
          title={card.title || 'Card'}
          isMinimized={card.isMinimized}
          onMinimize={() => handleCardMinimize(cardId)}
          onMaximize={() => handleCardMaximize(cardId)}
          initialPosition={card.position}
          onPositionChange={handleCardPositionChange}
          zIndex={card.zIndex || 50}
          onBringToFront={handleBringToFront}
        >
          {cardId === 'todo-list-card' && card.todoData ? (
            <TodoListCardContent
              todos={card.todoData.todos}
              total={card.todoData.total}
              completed={card.todoData.completed}
              in_progress={card.todoData.in_progress}
              pending={card.todoData.pending}
            />
          ) : cardId.startsWith('subagent-') && card.subagentData ? (
            <SubagentCardContent
              taskId={card.subagentData.taskId}
              description={card.subagentData.description}
              type={card.subagentData.type}
              toolCalls={card.subagentData.toolCalls}
              currentTool={card.subagentData.currentTool}
              status={card.subagentData.status || 'active'}
              messages={card.subagentData.messages || []}
              isHistory={card.subagentData.isHistory || false}
            />
          ) : (
            <div className="text-sm" style={{ color: '#FFFFFF' }}>
              <p className="mb-2">This is {card.title}.</p>
              <p className="mb-2">You can drag this card by clicking and dragging the header.</p>
              <p className="mb-2">Click anywhere on the card to bring it to the front.</p>
              <p className="mb-2">Click the minimize button to minimize it to an icon in the top bar.</p>
              <p>Click the bookmark icon in the top bar to restore it.</p>
            </div>
          )}
        </FloatingCard>
      ))}
    </div>
  );
}

export default ChatView;
