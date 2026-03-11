import React, { useState, useCallback, useMemo } from 'react';
import {
  ChevronRight, ChevronDown, Folder, Zap, Pin, MessageSquareText,
  Crown, Bot, Check, Circle, Loader2, X, ChevronsDown,
} from 'lucide-react';
import { ScrollArea } from '../../../components/ui/scroll-area';
import './NavigationPanel.css';

interface WorkspaceEntry {
  workspace_id: string;
  name?: string;
  status?: string;
  is_pinned?: boolean;
  [key: string]: unknown;
}

interface ThreadEntry {
  thread_id: string;
  title?: string;
  first_query_content?: string;
  [key: string]: unknown;
}

interface ThreadsData {
  threads: ThreadEntry[];
  loading?: boolean;
}

interface AgentMessage {
  role: string;
  isStreaming?: boolean;
  toolCallProcesses?: Record<string, { isInProgress?: boolean }>;
  [key: string]: unknown;
}

interface AgentEntry {
  id: string;
  name: string;
  isMainAgent?: boolean;
  status?: string;
  messages?: AgentMessage[];
  [key: string]: unknown;
}

interface NavigationPanelProps {
  workspaces: WorkspaceEntry[];
  workspaceThreads: Record<string, ThreadsData>;
  currentWorkspaceId?: string | null;
  currentThreadId?: string | null;
  agents?: AgentEntry[];
  activeAgentId?: string | null;
  expandWorkspace: (wsId: string) => void;
  onSelectAgent: (agentId: string) => void;
  onRemoveAgent?: (agentId: string) => void;
  onNavigateThread: (wsId: string, threadId: string) => void;
  hasMore?: boolean;
  onLoadMore?: () => void;
}

/**
 * NavigationPanel -- hover-triggered overlay sidebar showing
 * Workspace -> Thread -> Agent hierarchy.
 *
 * Follows the collapsible tree pattern from FilePanel's DirectoryNode:
 * ChevronRight/Down toggles, indented rows, Lucide icons throughout.
 */
function NavigationPanel({
  workspaces,
  workspaceThreads,
  currentWorkspaceId,
  currentThreadId,
  agents,
  activeAgentId,
  expandWorkspace,
  onSelectAgent,
  onRemoveAgent,
  onNavigateThread,
  hasMore,
  onLoadMore,
}: NavigationPanelProps) {
  // Track expanded workspaces and threads via Sets
  // Current workspace is expanded by default
  const [expandedWorkspaces, setExpandedWorkspaces] = useState<Set<string>>(
    () => new Set(currentWorkspaceId ? [currentWorkspaceId] : [])
  );
  const [expandedThreads, setExpandedThreads] = useState<Set<string>>(
    () => new Set(currentThreadId && currentThreadId !== '__default__' ? [currentThreadId] : [])
  );

  // Keep current workspace and thread expanded when they change
  React.useEffect(() => {
    if (currentWorkspaceId) {
      setExpandedWorkspaces((prev) => {
        if (prev.has(currentWorkspaceId)) return prev;
        const next = new Set(prev);
        next.add(currentWorkspaceId);
        return next;
      });
      expandWorkspace(currentWorkspaceId);
    }
  }, [currentWorkspaceId, expandWorkspace]);

  React.useEffect(() => {
    if (currentThreadId && currentThreadId !== '__default__') {
      setExpandedThreads((prev) => {
        if (prev.has(currentThreadId)) return prev;
        const next = new Set(prev);
        next.add(currentThreadId);
        return next;
      });
    }
  }, [currentThreadId]);

  const toggleWorkspace = useCallback((wsId: string) => {
    setExpandedWorkspaces((prev) => {
      const next = new Set(prev);
      if (next.has(wsId)) {
        next.delete(wsId);
      } else {
        next.add(wsId);
      }
      return next;
    });
    // Lazy-load threads when expanding -- called outside updater to avoid setState-during-render warning.
    // expandWorkspace is a no-op when data is already cached, so calling unconditionally is safe.
    expandWorkspace(wsId);
  }, [expandWorkspace]);

  const toggleThread = useCallback((threadId: string) => {
    setExpandedThreads((prev) => {
      const next = new Set(prev);
      if (next.has(threadId)) {
        next.delete(threadId);
      } else {
        next.add(threadId);
      }
      return next;
    });
  }, []);

  // Derive agent status for display
  const getAgentStatus = useCallback((agent: AgentEntry): string => {
    if (agent.isMainAgent) return 'active';
    const messages = agent.messages || [];
    if (agent.status === 'completed') return 'completed';
    if (messages.length === 0) return 'initializing';
    const lastAssistant = [...messages].reverse().find((m) => m.role === 'assistant');
    const isStreaming = lastAssistant?.isStreaming === true;
    const hasInProgressTool = lastAssistant?.toolCallProcesses
      ? Object.values(lastAssistant.toolCallProcesses).some((p) => p.isInProgress)
      : false;
    if (isStreaming || hasInProgressTool) return 'active';
    if (lastAssistant && lastAssistant.isStreaming === false) return 'completed';
    return agent.status || 'pending';
  }, []);

  // Sorted workspaces: current workspace first, then the rest in hook-provided order
  const sortedWorkspaces = useMemo(() => {
    if (!workspaces.length) return [];
    const current = workspaces.find((ws) => ws.workspace_id === currentWorkspaceId);
    const rest = workspaces.filter((ws) => ws.workspace_id !== currentWorkspaceId);
    return current ? [current, ...rest] : workspaces;
  }, [workspaces, currentWorkspaceId]);

  return (
    <div
      className="nav-panel h-full flex flex-col"
    >
      <ScrollArea className="flex-1">
        <div className="py-2">
          {sortedWorkspaces.map((ws) => {
            const wsId = ws.workspace_id;
            const isExpanded = expandedWorkspaces.has(wsId);
            const isFlash = ws.status === 'flash';
            const isPinned = ws.is_pinned;
            const isCurrent = wsId === currentWorkspaceId;
            const threadsData = workspaceThreads[wsId];
            const threads = threadsData?.threads || [];
            const threadsLoading = threadsData?.loading || false;

            return (
              <div key={wsId}>
                {/* Workspace row */}
                <div
                  className="nav-panel-row"
                  style={{ paddingLeft: 10 }}
                  onClick={() => toggleWorkspace(wsId)}
                >
                  {isExpanded
                    ? <ChevronDown className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                    : <ChevronRight className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                  }
                  {isFlash
                    ? <Zap className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                    : isPinned
                      ? <Pin className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                      : <Folder className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                  }
                  <span
                    className="text-sm font-medium truncate"
                    style={{ color: isCurrent ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
                  >
                    {ws.name || 'Workspace'}
                  </span>
                  {threadsLoading && (
                    <Loader2 className="h-3.5 w-3.5 animate-spin flex-shrink-0 ml-auto" style={{ color: 'var(--color-text-tertiary)' }} />
                  )}
                </div>

                {/* Threads under this workspace */}
                {isExpanded && (
                  <div>
                    {!threadsLoading && threads.length === 0 && (
                      <div
                        className="text-xs px-2 py-1"
                        style={{ paddingLeft: 44, color: 'var(--color-icon-muted)' }}
                      >
                        No conversations yet
                      </div>
                    )}
                    {threads.map((thread) => {
                      const tid = thread.thread_id;
                      const isCurrentThread = tid === currentThreadId;
                      const isThreadExpanded = expandedThreads.has(tid);
                      const subagents = agents?.filter((a) => !a.isMainAgent) || [];
                      const hasSubagents = isCurrentThread && subagents.length > 0;
                      const title = thread.title || thread.first_query_content?.slice(0, 40) || 'Untitled';

                      return (
                        <div key={tid}>
                          {/* Thread row */}
                          <div
                            className={`nav-panel-row ${isCurrentThread ? 'nav-panel-row-active' : ''}`}
                            style={{ paddingLeft: 28 }}
                            onClick={() => {
                              if (isCurrentThread) {
                                // Toggle agents expand for current thread
                                if (hasSubagents) toggleThread(tid);
                              } else {
                                onNavigateThread(wsId, tid);
                              }
                            }}
                          >
                            {/* Chevron for expanding agents -- only on current thread */}
                            {hasSubagents ? (
                              <button
                                onClick={(e) => { e.stopPropagation(); toggleThread(tid); }}
                                className="flex-shrink-0 p-0 bg-transparent border-none cursor-pointer"
                              >
                                {isThreadExpanded
                                  ? <ChevronDown className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                                  : <ChevronRight className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                                }
                              </button>
                            ) : (
                              <span style={{ width: 16, flexShrink: 0 }} />
                            )}
                            <MessageSquareText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                            <span
                              className="text-sm truncate"
                              style={{ color: isCurrentThread ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
                              title={title}
                            >
                              {title}
                            </span>
                          </div>

                          {/* Agent rows -- only when subagents exist, for current thread when expanded */}
                          {hasSubagents && isThreadExpanded && (
                            <div className="nav-panel-agent-group">
                              {agents!.map((agent) => {
                                const isMainAgent = agent.isMainAgent;
                                const isSelected = activeAgentId === agent.id;
                                const status = getAgentStatus(agent);
                                const isActive = status === 'active';
                                const isCompleted = status === 'completed';

                                return (
                                  <div
                                    key={agent.id}
                                    className={`nav-panel-agent-row group ${isActive && !isMainAgent ? 'nav-panel-agent-pulse' : ''}`}
                                    style={{
                                      backgroundColor: isSelected ? 'var(--color-border-muted)' : undefined,
                                    }}
                                    onClick={() => onSelectAgent(agent.id)}
                                  >
                                    {/* Agent icon */}
                                    {isMainAgent
                                      ? <Crown className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                                      : <Bot className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                                    }
                                    {/* Agent name */}
                                    <span
                                      className="text-xs truncate"
                                      style={{ color: isSelected ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
                                    >
                                      {agent.name}
                                    </span>
                                    {/* Status badge */}
                                    {!isMainAgent && (
                                      <span className="flex-shrink-0 ml-auto flex items-center">
                                        {isCompleted ? (
                                          <Check className="h-3 w-3" style={{ color: 'var(--color-text-tertiary)' }} />
                                        ) : isActive ? (
                                          <Loader2 className="h-3 w-3 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
                                        ) : (
                                          <Circle className="h-3 w-3" style={{ color: 'var(--color-icon-muted)' }} />
                                        )}
                                      </span>
                                    )}
                                    {/* Remove button -- non-main agents only, on hover */}
                                    {!isMainAgent && (
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          onRemoveAgent?.(agent.id);
                                        }}
                                        className="flex-shrink-0 opacity-0 group-hover:opacity-100 p-0 bg-transparent border-none cursor-pointer transition-opacity"
                                        title="Remove agent"
                                      >
                                        <X className="h-3 w-3" style={{ color: 'var(--color-text-tertiary)' }} />
                                      </button>
                                    )}
                                  </div>
                                );
                              })}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
          {hasMore && (
            <div
              className="nav-panel-row"
              style={{ paddingLeft: 10, justifyContent: 'center' }}
              onClick={onLoadMore}
            >
              <ChevronsDown className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                Load all
              </span>
            </div>
          )}
        </div>
      </ScrollArea>
    </div>
  );
}

export default NavigationPanel;
