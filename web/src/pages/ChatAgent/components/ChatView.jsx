import { ArrowLeft, ChevronLeft, ChevronRight } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { cn } from '../../../lib/utils';
import { useChatMessages } from '../hooks/useChatMessages';
import { useFloatingCards } from '../hooks/useFloatingCards';
import AgentPanel from './AgentPanel';
import ChatInput from './ChatInput';
import FloatingCard from './FloatingCard';
import FloatingCardIcon from './FloatingCardIcon';
import MessageList from './MessageList';
import TodoDrawer from './TodoDrawer';
import TodoListCardContent from './TodoListCardContent';

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

  // 新增：分屏相关状态
  const [selectedAgentId, setSelectedAgentId] = useState(null);
  const [isAgentPanelVisible, setIsAgentPanelVisible] = useState(true);

  // 从 floatingCards 中提取 subagent agents
  const subagentAgents = useMemo(() => {
    const agents = Object.entries(floatingCards)
      .filter(([cardId, card]) => {
        // 只提取 subagent 类型的卡片
        return cardId.startsWith('subagent-') && card.subagentData;
      })
      .map(([cardId, card], index) => {
        const subagentData = card.subagentData || {};
        return {
          id: cardId,
          taskId: subagentData.taskId || cardId,
          name: card.title || `Agent ${index + 1}`,
          number: String(index + 1).padStart(2, '0'),
          type: subagentData.type || 'general',
          status: subagentData.status || 'active',
          description: subagentData.description || '',
          toolCalls: subagentData.toolCalls || 0,
          currentTool: subagentData.currentTool || '',
          messages: subagentData.messages || [],
        };
      });

    console.log('[ChatView] Extracted subagent agents:', agents);
    return agents;
  }, [floatingCards]);

  // 自动选中第一个 agent（如果还没选中的话）
  useEffect(() => {
    if (subagentAgents.length > 0 && !selectedAgentId) {
      console.log('[ChatView] Auto-selecting first agent:', subagentAgents[0].id);
      setSelectedAgentId(subagentAgents[0].id);
    }
    // 如果选中的 agent 不存在了，重新选中第一个
    if (selectedAgentId && !subagentAgents.find(a => a.id === selectedAgentId)) {
      console.log('[ChatView] Selected agent no longer exists, selecting first');
      setSelectedAgentId(subagentAgents.length > 0 ? subagentAgents[0].id : null);
    }
  }, [subagentAgents, selectedAgentId]);

  // 当有新 agent 创建时，自动切换到它
  useEffect(() => {
    const latestAgent = subagentAgents[subagentAgents.length - 1];
    if (latestAgent && latestAgent.id !== selectedAgentId) {
      console.log('[ChatView] New agent detected, switching to:', latestAgent.id);
      setSelectedAgentId(latestAgent.id);
      setIsAgentPanelVisible(true); // 自动显示面板
    }
  }, [subagentAgents.length]); // 只在数量变化时触发

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
    <div className="flex h-screen w-full overflow-hidden bg-[#0D0E12]">
      {/* 左侧：聊天面板 */}
      <div
        className={cn(
          "flex flex-col",
          "bg-[#0D0E12] min-w-0 overflow-hidden",
          // 如果右侧面板显示，左侧占 2 份；否则占满
          isAgentPanelVisible ? "flex-[2]" : "flex-1"
        )}
      >
        {/* 顶部栏 */}
        <div className="flex items-center justify-between p-4 min-w-0 flex-shrink-0">
          <div className="flex items-center gap-4 min-w-0 flex-shrink">
            <button
              onClick={onBack}
              className="p-2 rounded-md transition-colors hover:bg-white/10 flex-shrink-0"
              style={{ color: '#FFFFFF' }}
              title="Back to threads"
            >
              <ArrowLeft className="h-5 w-5" />
            </button>
            <h1 className="text-lg font-semibold whitespace-nowrap" style={{ color: '#FFFFFF' }}>
              Chat Agent
            </h1>
            {isLoadingHistory && (
              <span className="text-xs whitespace-nowrap" style={{ color: '#FFFFFF', opacity: 0.5 }}>
                Loading history...
              </span>
            )}
          </div>

          <div className="flex items-center gap-2 flex-shrink-0">
            {/* Floating card icons for non-subagent cards */}
            {getAllCards()
              .filter(([cardId]) => !cardId.startsWith('subagent-'))
              .map(([cardId, card]) => (
                <FloatingCardIcon
                  key={cardId}
                  id={cardId}
                  title={card.title || 'Card'}
                  onClick={() => handleCardToggle(cardId)}
                  hasUnreadUpdate={card.hasUnreadUpdate || false}
                  isActive={true}
                />
              ))}

            {/* 新增：收起/展开按钮 - 常驻 */}
            <button
              onClick={() => setIsAgentPanelVisible(!isAgentPanelVisible)}
              className="p-2 rounded-lg hover:bg-white/10 transition-colors text-white/70 hover:text-white"
              title={isAgentPanelVisible ? "Hide agent panel" : "Show agent panel"}
            >
              {isAgentPanelVisible ? (
                <ChevronRight className="h-4 w-4" />
              ) : (
                <ChevronLeft className="h-4 w-4" />
              )}
            </button>

            {messageError && (
              <p className="text-xs" style={{ color: '#FF383C' }}>
                {messageError}
              </p>
            )}
          </div>
        </div>

        {/* 消息列表区域 */}
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

                  // 自动选中这个 agent 并显示面板
                  const cardId = `subagent-${subagentId}`;
                  setSelectedAgentId(cardId);
                  setIsAgentPanelVisible(true);
                }}
              />
            </div>
          </ScrollArea>
        </div>

        {/* 输入框区域 */}
        <div className="p-4 space-y-3">
          {/* Todo Drawer - 放在 input 上方 */}
          <TodoDrawer todoData={floatingCards['todo-list-card']?.todoData} />

          {/* Chat Input */}
          <ChatInput onSend={handleSendMessage} disabled={isLoading || isLoadingHistory || !workspaceId} />
        </div>
      </div>

      {/* 右侧：Agent 面板（新增） - 可随时调出 */}
      {isAgentPanelVisible && (
        <div className="flex-[3] flex flex-col min-w-0 overflow-hidden">
          <AgentPanel
            agents={subagentAgents}
            selectedAgentId={selectedAgentId}
            onSelectAgent={setSelectedAgentId}
          />
        </div>
      )}

      {/* 保留非 subagent 的 FloatingCard（排除 todo-list，因为它现在用 TodoDrawer 显示） */}
      {Object.entries(floatingCards)
        .filter(([cardId]) => !cardId.startsWith('subagent-') && cardId !== 'todo-list-card')
        .map(([cardId, card]) => (
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
