import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ArrowLeft, FolderOpen, Loader2 } from 'lucide-react';
import { ScrollArea } from '@/components/ui/scroll-area';
import MessageList from '../ChatAgent/components/MessageList';
import FilePanel from '../ChatAgent/components/FilePanel';
import { WorkspaceProvider } from '../ChatAgent/contexts/WorkspaceContext';
import { useTheme } from '../../contexts/ThemeContext';
import logoLight from '../../assets/img/logo.svg';
import logoDark from '../../assets/img/logo-dark.svg';
import {
  handleHistoryUserMessage,
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
  handleHistoryToolCalls,
  handleHistoryToolCallResult,
  handleHistoryTodoUpdate,
} from '../ChatAgent/hooks/utils/historyEventHandlers';
import { getSharedThread, replaySharedThread, getSharedFiles, readSharedFile, downloadSharedFileAs } from './api';

function updateMessage(messages, messageId, updater) {
  return messages.map((m) => (m.id === messageId ? updater(m) : m));
}

/**
 * SharedChatView — Public read-only view of a shared conversation.
 * Mirrors ChatView layout exactly, with interactive operations disabled.
 * Accessible at /s/:shareToken without authentication.
 */
export default function SharedChatView() {
  const { shareToken } = useParams();
  const { theme } = useTheme();
  const logo = theme === 'dark' ? logoDark : logoLight;

  const [metadata, setMetadata] = useState(null);
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [replayDone, setReplayDone] = useState(false);

  // File panel (right side, matching ChatView's rightPanelType === 'file')
  const [showFilePanel, setShowFilePanel] = useState(false);
  const [files, setFiles] = useState([]);
  const [filesLoading, setFilesLoading] = useState(false);
  const [filePanelTargetFile, setFilePanelTargetFile] = useState(null);
  const [rightPanelWidth, setRightPanelWidth] = useState(750);
  const isDraggingRef = useRef(false);

  const scrollAreaRef = useRef(null);

  // Fetch metadata
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const meta = await getSharedThread(shareToken);
        if (!cancelled) setMetadata(meta);
      } catch (e) {
        if (!cancelled) setError(e.message);
      }
    })();
    return () => { cancelled = true; };
  }, [shareToken]);

  // Replay conversation once metadata is loaded
  useEffect(() => {
    if (!metadata) return;
    let cancelled = false;

    const assistantMessagesByPair = new Map();
    const pairStateByPair = new Map();
    let currentActivePairIndex = null;
    let currentActivePairState = null;

    // Persistent refs that survive across event callbacks (matching useChatMessages shape)
    const sharedRefs = {
      recentlySentTracker: { isRecentlySent: () => false },
      currentMessageRef: { current: null },
      newMessagesStartIndexRef: { current: 0 },
      historyMessagesRef: { current: new Set() },
    };

    (async () => {
      try {
        await replaySharedThread(shareToken, (event) => {
          if (cancelled) return;
          const eventType = event.event;
          const contentType = event.content_type;
          const hasRole = event.role !== undefined;
          const hasPairIndex = event.turn_index !== undefined;

          if (hasPairIndex) {
            currentActivePairIndex = event.turn_index;
            currentActivePairState = pairStateByPair.get(event.turn_index);
          }

          if (eventType === 'replay_done') {
            setReplayDone(true);
            setLoading(false);
            return;
          }

          // Skip subagent events in shared view
          if (event.agent && typeof event.agent === 'string' && event.agent.startsWith('task:')) {
            return;
          }

          // user_message
          if (eventType === 'user_message' && hasPairIndex) {
            handleHistoryUserMessage({
              event,
              pairIndex: event.turn_index,
              assistantMessagesByPair,
              pairStateByPair,
              refs: sharedRefs,
              messages: [],
              setMessages,
            });
            return;
          }

          // message_chunk (text, reasoning)
          if (eventType === 'message_chunk' && hasRole && event.role === 'assistant' && hasPairIndex) {
            const pairIndex = event.turn_index;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            if (contentType === 'reasoning_signal') {
              handleHistoryReasoningSignal({
                assistantMessageId: currentAssistantMessageId,
                signalContent: event.content || '',
                pairIndex,
                pairState,
                setMessages,
              });
              return;
            }

            if (contentType === 'reasoning' && event.content) {
              handleHistoryReasoningContent({
                assistantMessageId: currentAssistantMessageId,
                content: event.content,
                pairState,
                setMessages,
              });
              return;
            }

            if (contentType === 'text' && event.content) {
              handleHistoryTextContent({
                assistantMessageId: currentAssistantMessageId,
                content: event.content,
                finishReason: event.finish_reason,
                pairState,
                setMessages,
              });
              return;
            }

            if (event.finish_reason) {
              setMessages((prev) =>
                updateMessage(prev, currentAssistantMessageId, (msg) => ({
                  ...msg,
                  isStreaming: false,
                }))
              );
              return;
            }
          }

          // tool_call_chunks — skip
          if (eventType === 'tool_call_chunks') return;

          // artifact (todo_update)
          if (eventType === 'artifact' && event.artifact_type === 'todo_update' && hasPairIndex) {
            const pairIndex = event.turn_index;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (currentAssistantMessageId && pairState) {
              handleHistoryTodoUpdate({
                assistantMessageId: currentAssistantMessageId,
                artifactType: event.artifact_type,
                artifactId: event.artifact_id,
                payload: event.payload || {},
                pairState,
                setMessages,
              });
            }
            return;
          }

          // tool_calls
          if (eventType === 'tool_calls' && hasPairIndex) {
            const pairIndex = event.turn_index;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            handleHistoryToolCalls({
              assistantMessageId: currentAssistantMessageId,
              toolCalls: event.tool_calls,
              pairState,
              setMessages,
            });
            return;
          }

          // tool_call_result
          if (eventType === 'tool_call_result' && hasPairIndex) {
            const pairIndex = event.turn_index;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            handleHistoryToolCallResult({
              assistantMessageId: currentAssistantMessageId,
              toolCallId: event.tool_call_id,
              result: {
                content: event.content,
                content_type: event.content_type,
                tool_call_id: event.tool_call_id,
                artifact: event.artifact,
              },
              pairState,
              setMessages,
            });
            return;
          }

          // interrupt — show plan approval as already-resolved
          if (eventType === 'interrupt' && hasPairIndex) {
            const pairIndex = event.turn_index;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            const interrupts = event.interrupts || [];
            interrupts.forEach((interrupt) => {
              if (interrupt.type === 'plan_approval' || interrupt.action_request?.action === 'SubmitPlan') {
                const planData = interrupt.action_request?.args || interrupt.data || {};
                const planId = `plan-${pairIndex}-${interrupt.id || 'default'}`;
                pairState.contentOrderCounter = (pairState.contentOrderCounter || 0) + 1;
                setMessages((prev) =>
                  updateMessage(prev, currentAssistantMessageId, (msg) => ({
                    ...msg,
                    contentSegments: [
                      ...(msg.contentSegments || []),
                      { type: 'plan_approval', planApprovalId: planId, order: pairState.contentOrderCounter },
                    ],
                    planApprovals: {
                      ...(msg.planApprovals || {}),
                      [planId]: {
                        ...planData,
                        interruptId: interrupt.id,
                        status: 'approved',
                      },
                    },
                  }))
                );
              }
            });
            return;
          }
        });

        // Mark all messages as done streaming
        setMessages((prev) => prev.map((m) => ({ ...m, isStreaming: false })));
        setLoading(false);
        setReplayDone(true);
      } catch (e) {
        if (!cancelled) {
          setError(e.message);
          setLoading(false);
        }
      }
    })();

    return () => { cancelled = true; };
  }, [metadata, shareToken]);

  // Auto-scroll on new messages
  useEffect(() => {
    if (scrollAreaRef.current) {
      const el = scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]');
      if (el) el.scrollTop = el.scrollHeight;
    }
  }, [messages]);

  // Permissions
  const permissions = metadata?.permissions || {};
  const canBrowseFiles = permissions.allow_files === true;
  const canDownload = permissions.allow_download === true;

  // File panel handlers
  const handleToggleFilePanel = useCallback(async () => {
    if (!canBrowseFiles) return;
    const next = !showFilePanel;
    setShowFilePanel(next);
    if (next && files.length === 0) {
      setFilesLoading(true);
      try {
        const result = await getSharedFiles(shareToken);
        setFiles(result.files || []);
      } catch { /* ignore */ }
      setFilesLoading(false);
    }
  }, [canBrowseFiles, showFilePanel, files.length, shareToken]);

  // Build API adapter for FilePanel — wraps public endpoints
  const fileApiAdapter = useMemo(() => ({
    readFile: (path) => readSharedFile(shareToken, path),
    downloadFile: (path) => downloadSharedFileAs(shareToken, path, 'blob'),
    downloadFileAsArrayBuffer: (path) => downloadSharedFileAs(shareToken, path, 'arraybuffer'),
    triggerDownload: (path) => downloadSharedFileAs(shareToken, path, 'download'),
  }), [shareToken]);

  // Image downloader for WorkspaceProvider — enables inline image rendering in markdown
  const imageDownloader = useCallback(
    (path) => downloadSharedFileAs(shareToken, path, 'blob'),
    [shareToken],
  );

  // Open file from chat (tool call artifacts, file mention cards)
  const handleOpenFile = useCallback(async (filePath) => {
    if (!canBrowseFiles) return;
    setShowFilePanel(true);
    setFilePanelTargetFile(filePath);
    // Ensure files are loaded
    if (files.length === 0) {
      setFilesLoading(true);
      try {
        const result = await getSharedFiles(shareToken);
        setFiles(result.files || []);
      } catch { /* ignore */ }
      setFilesLoading(false);
    }
  }, [canBrowseFiles, files.length, shareToken]);

  // Drag-to-resize file panel (matches ChatView)
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

  // Error state
  if (error) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen gap-4" style={{ backgroundColor: 'var(--color-bg-page)' }}>
        <img src={logo} alt="LangAlpha" className="h-8 opacity-60" />
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
          {error.includes('404') ? 'This shared conversation is no longer available.' : error}
        </p>
        <Link to="/" className="text-sm underline" style={{ color: 'var(--color-accent-primary)' }}>
          Go to LangAlpha
        </Link>
      </div>
    );
  }

  // Loading state
  if (!metadata) {
    return (
      <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
        <Loader2 className="h-6 w-6 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
      </div>
    );
  }

  return (
    <div
      className="flex h-screen w-full overflow-hidden"
      style={{ backgroundColor: 'var(--color-bg-page)' }}
    >
      {/* Left Side: Topbar + Chat Window — identical structure to ChatView */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Top bar — matches ChatView's top bar exactly */}
        <div className="flex items-center justify-between px-4 py-2 border-b min-w-0 flex-shrink-0" style={{ borderColor: 'var(--color-border-muted)' }}>
          <div className="flex items-center gap-4 min-w-0 flex-shrink">
            <Link
              to="/"
              className="p-2 rounded-md transition-colors flex-shrink-0"
              style={{ color: 'var(--color-text-primary)' }}
              title="Back to LangAlpha"
              onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = ''; }}
            >
              <ArrowLeft className="h-5 w-5" />
            </Link>
            <h1 className="text-base font-semibold whitespace-nowrap title-font truncate" style={{ color: 'var(--color-text-primary)' }}>
              {metadata.workspace_name || metadata.title || 'Shared Conversation'}
            </h1>
            {loading && (
              <span className="text-xs whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }}>
                Loading history...
              </span>
            )}
          </div>

          <div className="flex items-center gap-2">
            {canBrowseFiles && (
              <button
                onClick={handleToggleFilePanel}
                className="p-2 rounded-md transition-colors"
                style={{ color: 'var(--color-text-primary)', backgroundColor: showFilePanel ? 'var(--color-border-muted)' : undefined }}
                title="Workspace Files"
                onMouseEnter={(e) => { if (!showFilePanel) e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
                onMouseLeave={(e) => { if (!showFilePanel) e.currentTarget.style.backgroundColor = ''; }}
              >
                <FolderOpen className="h-5 w-5" />
              </button>
            )}
          </div>
        </div>

        {/* Content area: Chat Window */}
        <div className="flex-1 flex overflow-hidden">
          {/* Chat Window */}
          <div className="flex-1 flex flex-col overflow-hidden min-w-0">
            {/* Messages Area - Fixed height, scrollable */}
            <div
              className="flex-1 overflow-hidden"
              style={{ minHeight: 0, height: 0 }}
            >
              <ScrollArea ref={scrollAreaRef} className="h-full w-full">
                <div className="px-6 py-4 flex justify-center">
                  <div className="w-full max-w-3xl">
                    {loading && messages.length === 0 ? (
                      <div className="flex items-center justify-center py-20">
                        <Loader2 className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
                        <span className="ml-2 text-sm" style={{ color: 'var(--color-text-tertiary)' }}>Loading conversation...</span>
                      </div>
                    ) : (
                      <WorkspaceProvider downloadFile={imageDownloader}>
                      <MessageList
                        messages={messages}
                        readOnly={true}
                        allowFiles={canBrowseFiles}
                        onOpenSubagentTask={() => {}}
                        onOpenFile={handleOpenFile}
                      />
                      </WorkspaceProvider>
                    )}
                  </div>
                </div>
              </ScrollArea>
            </div>

            {/* Input Area — matches ChatView's input area styling */}
            <div className="flex-shrink-0 p-4 flex justify-center">
              <div className="w-full max-w-3xl space-y-3">
                <div
                  className="flex flex-col items-stretch rounded-2xl border"
                  style={{
                    borderColor: 'var(--color-border-muted)',
                    backgroundColor: 'var(--color-bg-card)',
                  }}
                >
                  <div className="flex flex-col px-3 pt-3 pb-2 gap-2">
                    <div
                      className="text-sm px-1 py-2"
                      style={{ color: 'var(--color-text-tertiary)' }}
                    >
                      This is a read-only shared conversation
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-xs px-1" style={{ color: 'var(--color-text-tertiary)' }}>
                        Powered by{' '}
                        <Link to="/" className="hover:underline" style={{ color: 'var(--color-accent-primary)' }}>
                          LangAlpha
                        </Link>
                      </span>
                      <button
                        disabled
                        className="flex items-center justify-center w-8 h-8 rounded-lg"
                        style={{
                          backgroundColor: 'var(--color-accent-disabled)',
                          color: 'var(--color-text-tertiary)',
                          cursor: 'not-allowed',
                        }}
                      >
                        <ArrowLeft className="h-4 w-4 rotate-180" />
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Right Side: File Panel — reuses real FilePanel in readOnly mode */}
      {showFilePanel && canBrowseFiles && (
        <>
          <div className="chat-split-divider" onMouseDown={handleDividerMouseDown} />
          <div className="flex-shrink-0" style={{ width: rightPanelWidth }}>
            <FilePanel
              readOnly
              apiAdapter={fileApiAdapter}
              onClose={() => setShowFilePanel(false)}
              files={files}
              filesLoading={filesLoading}
              targetFile={filePanelTargetFile}
              onTargetFileHandled={() => setFilePanelTargetFile(null)}
            />
          </div>
        </>
      )}
    </div>
  );
}
