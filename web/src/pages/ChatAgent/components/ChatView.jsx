import React, { useEffect, useRef, useState, useCallback } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { ArrowLeft, FolderOpen, Bot, StopCircle, ScrollText, AlertTriangle, CheckCircle2, Circle, Loader2 } from 'lucide-react';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { useAuth } from '../../../contexts/AuthContext';
import { updateCurrentUser } from '../../Dashboard/utils/api';
import { softInterruptWorkflow, getWorkspace } from '../utils/api';
import { useChatMessages } from '../hooks/useChatMessages';
import { saveChatSession, getChatSession, clearChatSession } from '../hooks/utils/chatSessionRestore';
import { useFloatingCards } from '../hooks/useFloatingCards';
import { useWorkspaceFiles } from '../hooks/useWorkspaceFiles';
import FilePanel from './FilePanel';
import './FilePanel.css';
import ChatInput from '../../../components/ui/chat-input';
import { attachmentsToImageContexts } from '../utils/fileUpload';
import DetailPanel from './DetailPanel';
import MessageList, { normalizeSubagentText } from './MessageList';
import Markdown from './Markdown';
import AgentSidebar from './AgentSidebar';
import SubagentStatusBar from './SubagentStatusBar';
import TodoDrawer from './TodoDrawer';
import { parseErrorMessage } from '../utils/parseErrorMessage';
import '../../Dashboard/Dashboard.css';

/**
 * SubagentStatusIndicator — inline status line for subagent view.
 */
function SubagentStatusIndicator({ status, currentTool, toolCalls = 0, messagesCount = 0 }) {
  const getIcon = () => {
    if (currentTool) {
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    if (status === 'active' && messagesCount > 0) {
      return <Loader2 className="h-3.5 w-3.5 animate-spin" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    if (status === 'completed') {
      return <CheckCircle2 className="h-3.5 w-3.5" style={{ color: 'rgba(255, 255, 255, 0.4)' }} />;
    }
    return <Circle className="h-3.5 w-3.5" style={{ color: 'rgba(255, 255, 255, 0.3)' }} />;
  };

  const getText = () => {
    if (currentTool) return `Running: ${currentTool}`;
    if (status === 'completed') {
      return toolCalls > 0 ? `Completed (${toolCalls} tool calls)` : 'Completed';
    }
    if (status === 'active') {
      return messagesCount > 0 ? 'Running' : 'Initializing';
    }
    return 'Initializing';
  };

  return (
    <div className="flex items-center gap-1.5 text-xs" style={{ color: 'rgba(255, 255, 255, 0.5)' }}>
      {getIcon()}
      <span>{getText()}</span>
    </div>
  );
}

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
  const subagentScrollAreaRef = useRef(null);
  const location = useLocation();
  const navigate = useNavigate();
  const { refreshUser } = useAuth();
  const initialMessageSentRef = useRef(false);
  // Determine agent mode: flash workspaces use flash mode, otherwise ptc
  const [agentMode, setAgentMode] = useState(location.state?.agentMode || 'ptc');
  const isFlashMode = agentMode === 'flash' || location.state?.workspaceStatus === 'flash';
  const [filePanelTargetFile, setFilePanelTargetFile] = useState(null);
  const [filePanelTargetDir, setFilePanelTargetDir] = useState(null);
  const isDraggingRef = useRef(false);

  // Right panel management - can show 'file', 'detail', or null (closed)
  const [rightPanelType, setRightPanelType] = useState(null);
  const [rightPanelWidth, setRightPanelWidth] = useState(420);
  // Active agent in main view (default: 'main')
  const [activeAgentId, setActiveAgentId] = useState('main');
  // Sidebar visibility
  const [sidebarVisible, setSidebarVisible] = useState(false);
  // Tool call detail panel state
  const [detailToolCall, setDetailToolCall] = useState(null);
  // Plan detail panel state
  const [detailPlanData, setDetailPlanData] = useState(null);
  // Track hidden agents (removed from sidebar, but not from state)
  const [hiddenAgentIds, setHiddenAgentIds] = useState(new Set());
  // Track whether the agent was soft-interrupted
  const [wasInterrupted, setWasInterrupted] = useState(false);
  // Track intentional back navigation (skip session save on unmount)
  const intentionalExitRef = useRef(false);

  // --- Scroll position memory for tab switching ---
  // Stores scrollTop per agentId so switching tabs preserves position
  const scrollPositionsRef = useRef({});
  const activeAgentIdRef = useRef(activeAgentId);
  activeAgentIdRef.current = activeAgentId;
  // Flag to skip subagent auto-scroll when restoring a saved position
  const skipSubagentAutoScrollRef = useRef(false);

  // Helper: get the scrollable container from a ScrollArea ref
  const getScrollContainer = useCallback((ref) => {
    if (!ref?.current) return null;
    return ref.current.querySelector('[data-radix-scroll-area-viewport]') ||
           ref.current.querySelector('.overflow-auto') ||
           ref.current;
  }, []);

  // Save scroll position of the currently active tab
  const saveScrollPosition = useCallback(() => {
    const currentId = activeAgentIdRef.current;
    const ref = currentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
    const container = getScrollContainer(ref);
    if (container) {
      scrollPositionsRef.current[currentId] = container.scrollTop;
    }
  }, [getScrollContainer]);

  // Switch agent tab with scroll position preservation
  const switchAgent = useCallback((newAgentId) => {
    if (newAgentId === activeAgentIdRef.current) return;
    saveScrollPosition();
    // If destination has a saved position, skip auto-scroll so restore wins
    if (scrollPositionsRef.current[newAgentId] != null) {
      skipSubagentAutoScrollRef.current = true;
    }
    setActiveAgentId(newAgentId);
  }, [saveScrollPosition]);

  // Restore scroll position after the new tab mounts
  useEffect(() => {
    const savedPosition = scrollPositionsRef.current[activeAgentId];
    if (savedPosition == null) return;

    // requestAnimationFrame waits for DOM commit + layout
    requestAnimationFrame(() => {
      const ref = activeAgentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
      const container = getScrollContainer(ref);
      if (container) {
        container.scrollTop = savedPosition;
      }
    });
  }, [activeAgentId, getScrollContainer]);

  // Reset sidebar and active agent on thread change
  useEffect(() => {
    setActiveAgentId('main');
    setSidebarVisible(false);
    scrollPositionsRef.current = {}; // Clear saved positions for new thread
  }, [threadId]);

  // Direct URL navigation fallback: detect flash workspace from API
  useEffect(() => {
    if (location.state?.agentMode || !workspaceId) return;
    let cancelled = false;
    getWorkspace(workspaceId).then((ws) => {
      if (!cancelled && ws?.status === 'flash') {
        setAgentMode('flash');
      }
    }).catch(() => {});
    return () => { cancelled = true; };
  }, [workspaceId, location.state?.agentMode]);

  // Floating cards management - extracted to custom hook for better encapsulation
  // Must be called before useChatMessages since updateTodoListCard and updateSubagentCard are passed to it
  const {
    floatingCards,
    updateTodoListCard,
    updateSubagentCard,
    inactivateAllSubagents,
    minimizeInactiveSubagents,
    completePendingTodos,
  } = useFloatingCards();

  // Sync onboarding_completed via PUT when ChatAgent completes onboarding (risk_preference + stocks)
  const handleOnboardingRelatedToolComplete = useCallback(async () => {
    try {
      await updateCurrentUser({ onboarding_completed: true });
      await refreshUser?.();
    } catch (e) {
      console.warn('[ChatView] Failed to sync onboarding_completed:', e);
    }
  }, [refreshUser]);

  // Workspace files - shared between FilePanel and ChatInput
  // Must be declared before useChatMessages so refreshFiles can be passed as onFileArtifact
  // Skip for flash mode — no sandbox
  const {
    files: workspaceFiles,
    loading: filesLoading,
    error: filesError,
    refresh: refreshFiles,
  } = useWorkspaceFiles(isFlashMode ? null : workspaceId);

  // Chat messages management - receives updateTodoListCard and updateSubagentCard from floating cards hook
  const {
    messages,
    isLoading,
    isLoadingHistory,
    isReconnecting,
    messageError,
    handleSendMessage,
    pendingInterrupt,
    pendingRejection,
    handleApproveInterrupt,
    handleRejectInterrupt,
    handleAnswerQuestion,
    handleSkipQuestion,
    threadId: currentThreadId,
    getSubagentHistory,
    resolveSubagentIdToAgentId,
  } = useChatMessages(workspaceId, threadId, updateTodoListCard, updateSubagentCard, inactivateAllSubagents, minimizeInactiveSubagents, completePendingTodos, handleOnboardingRelatedToolComplete, refreshFiles, agentMode);

  // Ref to avoid stale closure in unmount cleanup
  const currentThreadIdRef = useRef(currentThreadId);
  currentThreadIdRef.current = currentThreadId;

  // Save chat session on unmount for cross-tab restoration.
  // If user clicked back, save workspace-level only (no threadId) so tab
  // switching returns to the workspace page, not the conversation.
  useEffect(() => {
    return () => {
      if (intentionalExitRef.current) {
        saveChatSession({ workspaceId });
        return;
      }
      const container = getScrollContainer(scrollAreaRef);
      saveChatSession({
        workspaceId,
        threadId: currentThreadIdRef.current,
        scrollTop: container?.scrollTop || 0,
      });
    };
  }, [workspaceId, getScrollContainer]);

  // Restore scroll position from saved session on mount
  const sessionRestoredRef = useRef(false);
  useEffect(() => {
    if (sessionRestoredRef.current) return;
    const session = getChatSession();
    if (!session || session.workspaceId !== workspaceId) return;
    sessionRestoredRef.current = true;
    clearChatSession();
    const timer = setTimeout(() => {
      requestAnimationFrame(() => {
        const container = getScrollContainer(scrollAreaRef);
        if (container && session.scrollTop) {
          container.scrollTop = session.scrollTop;
        }
      });
    }, 300);
    return () => clearTimeout(timer);
  }, [workspaceId, getScrollContainer]);

  // Soft-interrupt handler: pauses main agent while keeping subagents running
  const handleSoftInterrupt = useCallback(async () => {
    const tid = currentThreadId || threadId;
    if (!tid || tid === '__default__') return;
    try {
      await softInterruptWorkflow(tid);
      setWasInterrupted(true);
    } catch (e) {
      console.warn('[ChatView] Failed to soft-interrupt workflow:', e);
    }
  }, [currentThreadId, threadId]);

  // Wrapper: converts ChatInput's (message, planMode, attachments) into
  // handleSendMessage(message, planMode, additionalContext, attachmentMeta)
  const handleSendWithAttachments = useCallback((message, planMode, attachments = []) => {
    let additionalContext = null;
    let attachmentMeta = null;
    if (attachments && attachments.length > 0) {
      additionalContext = attachmentsToImageContexts(attachments);
      attachmentMeta = attachments.map((a) => ({
        name: a.file.name,
        type: a.type,
        size: a.file.size,
        preview: a.preview || null,
        dataUrl: a.dataUrl,
      }));
    }
    handleSendMessage(message, planMode, additionalContext, attachmentMeta);
  }, [handleSendMessage]);

  // Show sidebar at the start of each backend response (streaming)
  // Auto-refresh workspace files when agent finishes (isLoading transitions true→false)
  const prevLoadingRef = useRef(false);
  useEffect(() => {
    const wasLoading = prevLoadingRef.current;
    prevLoadingRef.current = isLoading;
    if (isLoading && !wasLoading) {
      setSidebarVisible(true);
      setWasInterrupted(false);
    }
    if (!isLoading && wasLoading) {
      refreshFiles();
    }
  }, [isLoading, refreshFiles]);

  // Ensure new active agents are visible (remove from hidden list)
  useEffect(() => {
    Object.entries(floatingCards).forEach(([cardId, card]) => {
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
  }, [floatingCards]);

  // Convert floatingCards to agents array for sidebar
  // Filter out hidden agents and add main agent
  // Limit to 12 agents total (1 main + 11 subagents), hide older ones
  const allSubagentAgents = Object.entries(floatingCards)
    .filter(([cardId]) => cardId.startsWith('subagent-'))
    .map(([cardId, card]) => ({
      id: cardId.replace('subagent-', ''),
      name: card.subagentData?.displayId || 'Agent',
      taskId: card.subagentData?.taskId || card.subagentData?.agentId || '',
      description: card.subagentData?.description || '',
      type: card.subagentData?.type || 'general-purpose',
      status: card.subagentData?.status || 'active',
      toolCalls: card.subagentData?.toolCalls || 0,
      currentTool: card.subagentData?.currentTool || '',
      messages: card.subagentData?.messages || [],
      isActive: card.subagentData?.isActive !== false,
      isMainAgent: false,
    }))
    .reverse(); // Newer first (Object.entries returns insertion order, so reverse = newest first)

  // Filter out hidden agents
  const visibleSubagentAgents = allSubagentAgents.filter(agent => !hiddenAgentIds.has(agent.id));

  // Limit to 11 subagents (main agent makes 12 total)
  const maxSubagents = 11;
  const subagentAgents = visibleSubagentAgents.slice(0, maxSubagents);
  const excessSubagents = visibleSubagentAgents.slice(maxSubagents);

  // Auto-hide excess agents (beyond 11 subagents)
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
  }, [excessSubagents.length, excessSubagents.map(a => a.id).join(',')]);

  // Main agent (always first) - Director
  const mainAgent = {
    id: 'main',
    name: 'master', // Icon label at bottom
    displayName: 'LangAlpha', // Floating card / panel header title
    taskId: '',
    description: '',
    type: 'main',
    status: 'active',
    toolCalls: 0,
    currentTool: '',
    messages: [],
    isActive: true,
    isMainAgent: true,
  };

  // Combine: main agent first, then visible subagents (limited to 11)
  const agents = [mainAgent, ...subagentAgents];

  // Find the active agent object for subagent view
  const activeAgent = activeAgentId !== 'main'
    ? agents.find(a => a.id === activeAgentId) || null
    : null;

  // Show sidebar when new subagent spawns (don't auto-switch activeAgentId)
  const prevSubagentCountRef = useRef(0);
  useEffect(() => {
    if (subagentAgents.length > prevSubagentCountRef.current) {
      setSidebarVisible(true);
    }
    prevSubagentCountRef.current = subagentAgents.length;
  }, [subagentAgents.length]);

  // Handle drag panel width
  const handleDividerMouseDown = useCallback((e) => {
    e.preventDefault();
    isDraggingRef.current = true;
    const startX = e.clientX;
    const startWidth = rightPanelWidth;

    const onMouseMove = (moveEvent) => {
      if (!isDraggingRef.current) return;
      const delta = startX - moveEvent.clientX;
      const newWidth = Math.max(280, Math.min(startWidth + delta, window.innerWidth * 0.6));
      setRightPanelWidth(newWidth);
    };

    const onMouseUp = () => {
      isDraggingRef.current = false;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [rightPanelWidth]);

  // Open a file in the right panel from chat tool calls
  const handleOpenFileFromChat = useCallback((filePath) => {
    setRightPanelType('file');
    setFilePanelTargetDir(null);
    setFilePanelTargetFile(filePath);
  }, []);

  // Open file panel filtered to a specific directory
  const handleOpenDirFromChat = useCallback((dirPath) => {
    setRightPanelType('file');
    setFilePanelTargetFile(null);
    setFilePanelTargetDir(dirPath);
  }, []);

  // Open tool call detail in right panel
  const handleToolCallDetailClick = useCallback((toolCallProcess) => {
    setDetailToolCall(toolCallProcess);
    setDetailPlanData(null);
    setRightPanelType('detail');
  }, []);

  // Open plan detail in right panel
  const handlePlanDetailClick = useCallback((planData) => {
    setDetailPlanData(planData);
    setDetailToolCall(null);
    setRightPanelType('detail');
  }, []);

  // Toggle file panel
  const handleToggleFilePanel = useCallback(() => {
    if (rightPanelType === 'file') {
      setRightPanelType(null);
    } else {
      setRightPanelType('file');
    }
  }, [rightPanelType]);

  // Toggle sidebar visibility
  const handleToggleSidebar = useCallback(() => {
    setSidebarVisible(prev => !prev);
  }, []);

  // Refresh subagent card with latest data from history or inline status.
  // Ensures status/currentTool are accurate regardless of stale streaming data.
  // agentId: stable agent_id (already resolved from toolCallId if needed)
  // overrides: optional { description, type, status } from inline card click
  const refreshSubagentCard = useCallback((agentId, overrides = {}) => {
    if (!updateSubagentCard || !agentId) return;

    const history = getSubagentHistory ? getSubagentHistory(agentId) : null;
    const finalDescription = history?.description || overrides.description || '';
    const finalType = history?.type || overrides.type || 'general-purpose';
    const finalStatus = history?.status || overrides.status || 'completed';

    const updateData = {
      agentId,
      taskId: agentId,
      description: finalDescription,
      type: finalType,
      status: finalStatus,
      toolCalls: 0,
      currentTool: '',
      isHistory: !!history,
      // isActive: true bypasses the inactive-card guard so stale fields get cleared.
      // For history cards this will be immediately overridden to false by the
      // isHistory check inside updateSubagentCard.
      isActive: !history,
    };
    if (history) {
      updateData.messages = history.messages || [];
    }

    updateSubagentCard(agentId, updateData);
  }, [updateSubagentCard, getSubagentHistory]);

  // Handle sidebar agent selection — refresh card data, then switch tab
  const handleSelectAgent = useCallback((agentId) => {
    if (agentId !== 'main') {
      refreshSubagentCard(agentId);
    }
    switchAgent(agentId);
  }, [refreshSubagentCard, switchAgent]);

  // Open subagent task (navigate to subagent tab) - shared between MessageList and DetailPanel
  const handleOpenSubagentTask = useCallback((subagentInfo) => {
    const { subagentId, description, type, status } = subagentInfo;
    // Resolve subagentId (may be toolCallId from segment) to stable agent_id for card operations
    const agentId = resolveSubagentIdToAgentId
      ? resolveSubagentIdToAgentId(subagentId)
      : subagentId;

    if (!updateSubagentCard) {
      console.error('[ChatView] updateSubagentCard is not defined!');
      return;
    }

    refreshSubagentCard(agentId, { description, type, status });

    switchAgent(agentId);
    setSidebarVisible(true);
  }, [resolveSubagentIdToAgentId, updateSubagentCard, refreshSubagentCard, switchAgent]);

  // Handle removing an agent from sidebar (just hide from display, don't affect state)
  const handleRemoveAgent = useCallback((agentId) => {
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
  }, [switchAgent]);

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
        // Capture state values before clearing (navigate may update location ref)
        const { initialMessage, planMode, additionalContext, attachmentMeta } = location.state;
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          handleSendMessage(initialMessage, planMode || false, additionalContext || null, attachmentMeta || null);
        }, 100);
      } else if (!isLoadingHistory && !isLoading) {
        // Existing thread - wait for history to load, then send
        // This ensures we don't send duplicate messages
        initialMessageSentRef.current = true;
        // Capture state values before clearing (navigate may update location ref)
        const { initialMessage, planMode, additionalContext, attachmentMeta } = location.state;
        // Clear navigation state to prevent re-sending on re-renders
        navigate(location.pathname, { replace: true, state: {} });
        // Small delay to ensure component is fully mounted
        setTimeout(() => {
          handleSendMessage(initialMessage, planMode || false, additionalContext || null, attachmentMeta || null);
        }, 100);
      }
    }
  }, [location.state, workspaceId, threadId, isLoading, isLoadingHistory, handleSendMessage, navigate, location.pathname]);

  // Smart auto-scroll: only scroll to bottom when user is already near the bottom
  const isNearBottomRef = useRef(true);
  const isSubagentNearBottomRef = useRef(true);

  // Attach scroll listener to detect user scroll position
  // Re-attaches when activeAgentId changes (ScrollArea remounts on tab switch)
  useEffect(() => {
    const isMain = activeAgentId === 'main';
    const ref = isMain ? scrollAreaRef : subagentScrollAreaRef;
    const nearBottomRef = isMain ? isNearBottomRef : isSubagentNearBottomRef;

    if (!ref.current) return;
    const scrollContainer = ref.current.querySelector('[data-radix-scroll-area-viewport]') ||
                           ref.current.querySelector('.overflow-auto') ||
                           ref.current;
    if (!scrollContainer) return;

    // Reset to near-bottom when switching tabs
    nearBottomRef.current = true;

    const handleScroll = () => {
      const threshold = 120;
      const { scrollTop, scrollHeight, clientHeight } = scrollContainer;
      nearBottomRef.current = scrollHeight - scrollTop - clientHeight < threshold;
    };

    scrollContainer.addEventListener('scroll', handleScroll, { passive: true });
    return () => scrollContainer.removeEventListener('scroll', handleScroll);
  }, [activeAgentId]);

  // Auto-scroll main chat to bottom when messages change, but only if user is near the bottom
  useEffect(() => {
    if (!isNearBottomRef.current) return;
    if (!scrollAreaRef.current) return;

    const scrollContainer = scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]') ||
                           scrollAreaRef.current.querySelector('.overflow-auto') ||
                           scrollAreaRef.current;
    if (scrollContainer) {
      setTimeout(() => {
        scrollContainer.scrollTop = scrollContainer.scrollHeight;
      }, 0);
    }
  }, [messages]);

  // Auto-scroll subagent view when active subagent's messages change
  // Uses the same smart-scroll logic: only scroll if user is near the bottom
  // Skipped when restoring a saved scroll position after tab switch
  useEffect(() => {
    if (skipSubagentAutoScrollRef.current) {
      skipSubagentAutoScrollRef.current = false;
      return;
    }
    if (!isSubagentNearBottomRef.current) return;
    if (!activeAgent || !subagentScrollAreaRef.current) return;
    const scrollContainer = subagentScrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]') ||
                           subagentScrollAreaRef.current.querySelector('.overflow-auto') ||
                           subagentScrollAreaRef.current;
    if (scrollContainer) {
      setTimeout(() => {
        scrollContainer.scrollTop = scrollContainer.scrollHeight;
      }, 0);
    }
  }, [activeAgent?.messages]);


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
    <div
      className="flex h-screen w-full overflow-hidden"
      style={{
        backgroundColor: 'var(--color-bg-page)',
      }}
    >
      {/* Left Side: Topbar + Sidebar + Chat Window */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Top bar */}
        <div className="flex items-center justify-between px-4 py-2 border-b border-white/10 min-w-0 flex-shrink-0">
          <div className="flex items-center gap-4 min-w-0 flex-shrink">
            <button
              onClick={() => { intentionalExitRef.current = true; onBack(); }}
              className="p-2 rounded-md transition-colors hover:bg-white/10 flex-shrink-0"
              style={{ color: '#FFFFFF' }}
              title="Back to threads"
            >
              <ArrowLeft className="h-5 w-5" />
            </button>
            <h1 className="text-base font-semibold whitespace-nowrap dashboard-title-font" style={{ color: '#FFFFFF' }}>
              LangAlpha
            </h1>
            {isLoadingHistory ? (
              <span className="text-xs whitespace-nowrap" style={{ color: '#FFFFFF', opacity: 0.5 }}>
                Loading history...
              </span>
            ) : null}
          </div>

          <div className="flex items-center gap-2">
            {!isFlashMode && (
              <button
                onClick={handleToggleFilePanel}
                className={`p-2 rounded-md transition-colors ${rightPanelType === 'file' ? 'bg-white/15' : 'hover:bg-white/10'}`}
                style={{ color: '#FFFFFF' }}
                title="Workspace Files"
              >
                <FolderOpen className="h-5 w-5" />
              </button>
            )}
            <button
              onClick={handleToggleSidebar}
              className={`p-2 rounded-md transition-colors ${sidebarVisible ? 'bg-white/15' : 'hover:bg-white/10'}`}
              style={{ color: '#FFFFFF' }}
              title="Agents"
            >
              <Bot className="h-5 w-5" />
            </button>
          </div>
        </div>

        {/* Content area: Sidebar + Chat Window */}
        <div className="flex-1 flex overflow-hidden">
          {/* Agent Sidebar */}
          {sidebarVisible && (
            <AgentSidebar
              agents={agents}
              activeAgentId={activeAgentId}
              onSelectAgent={handleSelectAgent}
              onRemoveAgent={handleRemoveAgent}
            />
          )}

          {/* Chat Window */}
          <div className="flex-1 flex flex-col overflow-hidden min-w-0">
            {/* Messages Area - Fixed height, scrollable */}
            <div
              className="flex-1 overflow-hidden"
              style={{
                minHeight: 0,
                height: 0, // Force flex-1 to work properly
              }}
            >
              {activeAgentId === 'main' ? (
                <ScrollArea ref={scrollAreaRef} className="h-full w-full">
                  <div className="px-6 py-4 flex justify-center">
                    <div className="w-full max-w-3xl">
                      <MessageList
                        messages={messages}
                        onOpenFile={handleOpenFileFromChat}
                        onOpenDir={handleOpenDirFromChat}
                        onToolCallDetailClick={handleToolCallDetailClick}
                        onOpenSubagentTask={handleOpenSubagentTask}
                        onApprovePlan={handleApproveInterrupt}
                        onRejectPlan={handleRejectInterrupt}
                        onPlanDetailClick={handlePlanDetailClick}
                        onAnswerQuestion={handleAnswerQuestion}
                        onSkipQuestion={handleSkipQuestion}
                      />
                    </div>
                  </div>
                </ScrollArea>
              ) : activeAgent ? (
                <ScrollArea ref={subagentScrollAreaRef} className="h-full w-full">
                  <div className="px-6 py-4 flex justify-center">
                    <div className="w-full max-w-3xl space-y-2.5">
                      {/* Task description */}
                      {activeAgent.description && (
                        <div style={{ color: 'rgba(255, 255, 255, 0.8)' }}>
                          <Markdown
                            variant="chat"
                            content={normalizeSubagentText(activeAgent.description)}
                            className="text-sm leading-relaxed"
                          />
                        </div>
                      )}
                      {/* Status indicator */}
                      <SubagentStatusIndicator
                        status={activeAgent.status}
                        currentTool={activeAgent.currentTool}
                        toolCalls={activeAgent.toolCalls}
                        messagesCount={activeAgent.messages?.length}
                      />
                      {/* Messages — reuse MessageList */}
                      {activeAgent.messages?.length > 0 && (
                        <div style={{ borderTop: '0.5px solid rgba(255, 255, 255, 0.04)', paddingTop: '8px' }}>
                          <MessageList
                            messages={activeAgent.messages}
                            isSubagentView={true}
                            hideAvatar={true}
                            onOpenFile={handleOpenFileFromChat}
                            onToolCallDetailClick={handleToolCallDetailClick}
                          />
                        </div>
                      )}
                    </div>
                  </div>
                </ScrollArea>
              ) : (
                // Active agent not found (may have been removed) - fallback
                <div className="flex items-center justify-center h-full">
                  <p className="text-sm" style={{ color: 'rgba(255, 255, 255, 0.5)' }}>
                    Agent not found
                  </p>
                </div>
              )}
            </div>

            {/* Input Area */}
            <div className="flex-shrink-0 p-4 flex justify-center">
              <div className="w-full max-w-3xl space-y-3">
                {activeAgentId === 'main' ? (
                  <>
                    <TodoDrawer todoData={floatingCards['todo-list-card']?.todoData} />
                    {pendingRejection && (
                      <div
                        className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
                        style={{ backgroundColor: 'rgba(97, 85, 245, 0.08)', color: 'rgba(255, 255, 255, 0.75)', border: '1px solid rgba(97, 85, 245, 0.2)' }}
                      >
                        <ScrollText className="h-4 w-4 flex-shrink-0" style={{ color: '#6155F5' }} />
                        <span>Type your feedback to revise the plan, then send.</span>
                      </div>
                    )}
                    {wasInterrupted && !isLoading && !pendingInterrupt && !pendingRejection && (
                      <div
                        className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
                        style={{ backgroundColor: 'rgba(220, 38, 38, 0.1)', color: 'rgba(255, 255, 255, 0.75)' }}
                      >
                        <StopCircle className="h-4 w-4 flex-shrink-0" style={{ color: '#dc2626' }} />
                        <span>Agent interrupted. Feel free to provide new instructions.</span>
                      </div>
                    )}
                    {messageError && !isLoading && (() => {
                      const parsed = parseErrorMessage(messageError);
                      return (
                        <div
                          className="flex items-center gap-2 px-3 py-2 rounded-md text-sm"
                          style={{ backgroundColor: 'rgba(220, 38, 38, 0.1)', color: 'rgba(255, 180, 180, 0.9)' }}
                        >
                          <AlertTriangle className="h-4 w-4 flex-shrink-0" style={{ color: 'rgba(255, 120, 120, 0.9)' }} />
                          <span>{parsed.detail ? `${parsed.title}: ${parsed.detail}` : parsed.title}</span>
                        </div>
                      );
                    })()}
                    <ChatInput
                      onSend={handleSendWithAttachments}
                      disabled={isLoading || isLoadingHistory || !workspaceId || !!pendingInterrupt}
                      onStop={handleSoftInterrupt}
                      isLoading={isLoading}
                      files={workspaceFiles}
                    />
                  </>
                ) : activeAgent ? (
                  <SubagentStatusBar agent={activeAgent} />
                ) : null}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Right Side: Split Panel (File or Detail only) */}
      {rightPanelType && (
        <>
          <div
            className="chat-split-divider"
            onMouseDown={handleDividerMouseDown}
          />
          <div className="flex-shrink-0" style={{ width: rightPanelWidth }}>
            {rightPanelType === 'file' ? (
              <FilePanel
                workspaceId={workspaceId}
                onClose={() => setRightPanelType(null)}
                targetFile={filePanelTargetFile}
                onTargetFileHandled={() => setFilePanelTargetFile(null)}
                targetDirectory={filePanelTargetDir}
                onTargetDirHandled={() => setFilePanelTargetDir(null)}
                files={workspaceFiles}
                filesLoading={filesLoading}
                filesError={filesError}
                onRefreshFiles={refreshFiles}
              />
            ) : rightPanelType === 'detail' && (detailToolCall || detailPlanData) ? (
              <DetailPanel
                toolCallProcess={detailToolCall}
                planData={detailPlanData}
                onClose={() => {
                  setRightPanelType(null);
                  setDetailToolCall(null);
                  setDetailPlanData(null);
                }}
                onOpenFile={handleOpenFileFromChat}
                onOpenSubagentTask={handleOpenSubagentTask}
              />
            ) : null}
          </div>
        </>
      )}

    </div>
  );
}

export default ChatView;
