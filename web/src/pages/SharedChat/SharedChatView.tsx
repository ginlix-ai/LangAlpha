import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
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
import {
  getSharedThread,
  replaySharedThread,
  getSharedFiles,
  readSharedFile,
  downloadSharedFileAs,
  fetchSharedServeObjectUrl,
  fetchSharedServeArrayBuffer,
} from './api';
import type { SharedThreadMetadata, SSEEvent } from './api';
import { buildSharedServeUrl } from '../ChatAgent/components/viewers/html/wsfilesUrl';

// Message record type compatible with historyEventHandlers
type MessageRecord = Record<string, unknown>;

/** SetMessages type matching historyEventHandlers' signature */
type SetMessages = (updater: (prev: MessageRecord[]) => MessageRecord[]) => void;

/** PairState type matching historyEventHandlers' PairState */
interface PairState {
  contentOrderCounter: number;
  reasoningId: string | null;
  toolCallId: string | null;
}

function updateMessage(messages: MessageRecord[], messageId: string, updater: (m: MessageRecord) => MessageRecord): MessageRecord[] {
  return messages.map((m) => (m.id === messageId ? updater(m) : m));
}

/**
 * SharedChatView — Public read-only view of a shared conversation.
 * Mirrors ChatView layout exactly, with interactive operations disabled.
 * Accessible at /s/:shareToken without authentication.
 */
export default function SharedChatView() {
  const { shareToken } = useParams<{ shareToken: string }>();
  const { theme } = useTheme();
  const logo = theme === 'dark' ? logoDark : logoLight;

  const [metadata, setMetadata] = useState<SharedThreadMetadata | null>(null);
  const [messages, setMessages] = useState<MessageRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // File panel (right side, matching ChatView's rightPanelType === 'file')
  const [showFilePanel, setShowFilePanel] = useState(false);
  const [files, setFiles] = useState<string[]>([]);
  const [filesLoading, setFilesLoading] = useState(false);
  const [filePanelTargetFile, setFilePanelTargetFile] = useState<string | null>(null);
  const [rightPanelWidth, setRightPanelWidth] = useState(750);
  const isDraggingRef = useRef(false);

  const scrollAreaRef = useRef<HTMLDivElement>(null);

  // Fetch metadata
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const meta = await getSharedThread(shareToken!);
        if (!cancelled) setMetadata(meta);
      } catch (e: unknown) {
        if (!cancelled) setError((e as Error).message);
      }
    })();
    return () => { cancelled = true; };
  }, [shareToken]);

  // Replay conversation once metadata is loaded
  useEffect(() => {
    if (!metadata) return;
    let cancelled = false;

    const assistantMessagesByPair = new Map<number, string>();
    const pairStateByPair = new Map<number, PairState>();
    let _currentActivePairIndex: number | null = null;
    let _currentActivePairState: PairState | undefined = undefined;

    // Persistent refs that survive across event callbacks (matching useChatMessages shape)
    const sharedRefs = {
      recentlySentTracker: { isRecentlySent: (_content: string) => false },
      currentMessageRef: { current: null as string | null },
      newMessagesStartIndexRef: { current: 0 },
      historyMessagesRef: { current: new Set<string>() },
    };

    // Cast setMessages to the narrower type expected by historyEventHandlers
    const setMessagesCompat: SetMessages = setMessages;

    (async () => {
      try {
        await replaySharedThread(shareToken!, (event: SSEEvent) => {
          if (cancelled) return;
          const eventType = event.event as string | undefined;
          const contentType = event.content_type as string | undefined;
          const hasRole = event.role !== undefined;
          const hasPairIndex = event.turn_index !== undefined;

          if (hasPairIndex) {
            _currentActivePairIndex = event.turn_index as number;
            _currentActivePairState = pairStateByPair.get(event.turn_index as number);
          }

          if (eventType === 'replay_done') {
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
              pairIndex: event.turn_index as number,
              assistantMessagesByPair,
              pairStateByPair,
              refs: sharedRefs,
              messages: [],
              setMessages: setMessagesCompat,
            });
            return;
          }

          // message_chunk (text, reasoning)
          if (eventType === 'message_chunk' && hasRole && event.role === 'assistant' && hasPairIndex) {
            const pairIndex = event.turn_index as number;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            if (contentType === 'reasoning_signal') {
              handleHistoryReasoningSignal({
                assistantMessageId: currentAssistantMessageId,
                signalContent: (event.content as string) || '',
                pairIndex,
                pairState,
                setMessages: setMessagesCompat,
              });
              return;
            }

            if (contentType === 'reasoning' && event.content) {
              handleHistoryReasoningContent({
                assistantMessageId: currentAssistantMessageId,
                content: event.content as string,
                pairState,
                setMessages: setMessagesCompat,
              });
              return;
            }

            if (contentType === 'text' && event.content) {
              handleHistoryTextContent({
                assistantMessageId: currentAssistantMessageId,
                content: event.content as string,
                finishReason: event.finish_reason as string | undefined,
                pairState,
                setMessages: setMessagesCompat,
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
            const pairIndex = event.turn_index as number;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (currentAssistantMessageId && pairState) {
              handleHistoryTodoUpdate({
                assistantMessageId: currentAssistantMessageId,
                artifactType: event.artifact_type as string,
                artifactId: event.artifact_id as string,
                payload: (event.payload as Record<string, unknown>) || {},
                pairState,
                setMessages: setMessagesCompat,
              });
            }
            return;
          }

          // tool_calls
          if (eventType === 'tool_calls' && hasPairIndex) {
            const pairIndex = event.turn_index as number;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            handleHistoryToolCalls({
              assistantMessageId: currentAssistantMessageId,
              toolCalls: event.tool_calls as Array<Record<string, unknown>>,
              pairState,
              setMessages: setMessagesCompat,
            });
            return;
          }

          // tool_call_result
          if (eventType === 'tool_call_result' && hasPairIndex) {
            const pairIndex = event.turn_index as number;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            handleHistoryToolCallResult({
              assistantMessageId: currentAssistantMessageId,
              toolCallId: event.tool_call_id as string,
              result: {
                content: event.content,
                content_type: event.content_type as string,
                tool_call_id: event.tool_call_id as string,
                artifact: event.artifact,
              },
              pairState,
              setMessages: setMessagesCompat,
            });
            return;
          }

          // provenance — DELIBERATELY NOT replayed in public shared chats.
          // Provenance records carry result snippets, checksums, and identifiers
          // (incl. private file/memo/memory paths) of data the owner's agent
          // accessed. Replaying them here would leak that into a public share.
          // Sources are owner-only by design; do not add a `provenance` branch
          // without a redaction story for the public surface.

          // interrupt — show plan approval as already-resolved
          if (eventType === 'interrupt' && hasPairIndex) {
            const pairIndex = event.turn_index as number;
            const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
            const pairState = pairStateByPair.get(pairIndex);
            if (!currentAssistantMessageId || !pairState) return;

            const interrupts = (event.interrupts as Array<Record<string, unknown>>) || [];
            interrupts.forEach((interrupt) => {
              const actionRequest = interrupt.action_request as Record<string, unknown> | undefined;
              if (interrupt.type === 'plan_approval' || actionRequest?.action === 'SubmitPlan') {
                const planData = (actionRequest?.args as Record<string, unknown>) || (interrupt.data as Record<string, unknown>) || {};
                const planId = `plan-${pairIndex}-${interrupt.id || 'default'}`;
                pairState.contentOrderCounter = (pairState.contentOrderCounter || 0) + 1;
                setMessages((prev) =>
                  updateMessage(prev, currentAssistantMessageId, (msg) => ({
                    ...msg,
                    contentSegments: [
                      ...((msg.contentSegments as Array<Record<string, unknown>>) || []),
                      { type: 'plan_approval', planApprovalId: planId, order: pairState.contentOrderCounter },
                    ],
                    planApprovals: {
                      ...((msg.planApprovals as Record<string, unknown>) || {}),
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
      } catch (e: unknown) {
        if (!cancelled) {
          setError((e as Error).message);
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
  const permissions = (metadata?.permissions || {}) as Record<string, unknown>;
  const canBrowseFiles = permissions.allow_files === true;
  const _canDownload = permissions.allow_download === true;

  // File panel handlers
  const handleToggleFilePanel = useCallback(async () => {
    if (!canBrowseFiles) return;
    const next = !showFilePanel;
    setShowFilePanel(next);
    if (next && files.length === 0) {
      setFilesLoading(true);
      try {
        const result = await getSharedFiles(shareToken!);
        setFiles(result.files || []);
      } catch { /* ignore */ }
      setFilesLoading(false);
    }
  }, [canBrowseFiles, showFilePanel, files.length, shareToken]);

  // Build API adapter for FilePanel — wraps public endpoints. buildServedUrl
  // points the HTML preview iframe at the public serve URL (no workspace UUID).
  const fileApiAdapter = useMemo(() => ({
    readFile: (path: string) => readSharedFile(shareToken!, path),
    // HTML files read their full source here; the public read endpoint caps at
    // its own line limit, and the preview renders via the served URL regardless.
    readFileFull: (path: string) => readSharedFile(shareToken!, path),
    // Byte-access previews go through the serve endpoint (allow_files), matching
    // the rendered report. Only the explicit save affordance uses the download
    // endpoint (allow_download) — see fetchSharedServeObjectUrl docs.
    downloadFile: (path: string) => fetchSharedServeObjectUrl(shareToken!, path),
    downloadFileAsArrayBuffer: (path: string) => fetchSharedServeArrayBuffer(shareToken!, path),
    triggerDownload: (path: string) => downloadSharedFileAs(shareToken!, path, 'download'),
    buildServedUrl: (path: string, opts?: { injectTheme?: boolean }) =>
      buildSharedServeUrl(shareToken!, path, opts),
  }), [shareToken]);

  // Inline markdown images render via the serve endpoint (allow_files) so they
  // load on a copy-link share, which grants allow_files but not allow_download.
  const imageDownloader = useCallback(
    (path: string) => fetchSharedServeObjectUrl(shareToken!, path),
    [shareToken],
  );

  // Open file from chat (tool call artifacts, file mention cards)
  const handleOpenFile = useCallback(async (filePath: string) => {
    if (!canBrowseFiles) return;
    setShowFilePanel(true);
    setFilePanelTargetFile(filePath);
    // Ensure files are loaded
    if (files.length === 0) {
      setFilesLoading(true);
      try {
        const result = await getSharedFiles(shareToken!);
        setFiles(result.files || []);
      } catch { /* ignore */ }
      setFilesLoading(false);
    }
  }, [canBrowseFiles, files.length, shareToken]);

  // Deep link: `?file=<path>` opens that report directly once metadata + file
  // permission are known. One-shot — the share-link target from §1.3b.
  const fileDeepLinkConsumedRef = useRef(false);
  useEffect(() => {
    if (fileDeepLinkConsumedRef.current || !metadata || !canBrowseFiles) return;
    const fileParam = new URLSearchParams(window.location.search).get('file');
    if (!fileParam) return;
    fileDeepLinkConsumedRef.current = true;
    handleOpenFile(fileParam);
  }, [metadata, canBrowseFiles, handleOpenFile]);

  // Drag-to-resize file panel (matches ChatView)
  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDraggingRef.current = true;
    const startX = e.clientX;
    const startWidth = rightPanelWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
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
                      <WorkspaceProvider workspaceId={null} downloadFile={imageDownloader}>
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
              workspaceId=""
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
