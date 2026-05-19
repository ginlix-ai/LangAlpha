import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { RefreshCw } from 'lucide-react';
import { queryKeys } from '@/lib/queryKeys';
import { ErrorBanner } from '@/components/ui/error-banner';
import LogoLoading from '@/components/ui/logo-loading';
import ChatInput from '@/components/ui/chat-input';
import { useNarrowContainer } from '@/hooks/useNarrowContainer';
import MessageList from '../../ChatAgent/components/MessageList';
import { SubagentTelemetryContext } from '../../ChatAgent/components/SubagentTelemetryContext';
import { WorkspaceProvider } from '../../ChatAgent/contexts/WorkspaceContext';
import { useChatMessages } from '../../ChatAgent/hooks/useChatMessages';
import { getFlashWorkspace } from '../../ChatAgent/utils/api';
import { attachmentsToContexts } from '../../ChatAgent/utils/fileUpload';
import {
  resolveSubagentTelemetry as resolveSubagentTelemetryPure,
  type SubagentHistoryLike,
} from '../../ChatAgent/utils/resolveSubagentTelemetry';
import type { ToolCallProcessRecord, SubagentInfo } from '../../ChatAgent/components/ToolCallDetailView';
import type { PreviewData } from '../../ChatAgent/hooks/utils/types';
import MarketChatHistoryButton from './MarketChatHistoryButton';
import MarketDetailDialog, { type DialogPayload } from './MarketDetailDialog';
import { getMarketThreadId, setMarketThreadId, clearMarketThreadId } from '../utils/threadPersistence';
import './MarketPanel.css';

interface Workspace {
  workspace_id: string;
  name?: string;
  status?: string;
  [key: string]: unknown;
}

interface AttachmentItem {
  dataUrl: string;
  file: { name: string; size: number };
  type: string;
  preview?: string | null;
}

interface MarketChatPanelProps {
  symbol: string;
  mode: 'fast' | 'ptc';
  onModeChange: (mode: 'fast' | 'ptc') => void;
  workspaces: Workspace[];
  selectedWorkspaceId: string | null;
  onWorkspaceChange: (id: string) => void;
  chartImage: string | null;
  chartImageDesc: string | null;
  onCaptureChart: () => Promise<void> | void;
  onClearChartImage: () => void;
  prefillMessage: string;
  onClearPrefill: () => void;
  quickQueries: string[];
  onQuickQuery: (q: string) => void;
  onShuffleQueries: () => void;
  onNavigateSubagent?: (threadId: string, taskId: string) => void;
  placeholder?: string;
}

/**
 * Desktop chat panel for MarketView. Drives the message stream via
 * ChatAgent's `useChatMessages` so the rendering (tool calls, reasoning,
 * artifacts, widgets) stays in lockstep with the main chat page.
 */
export default function MarketChatPanel(props: MarketChatPanelProps): React.ReactElement {
  const {
    symbol,
    mode,
    workspaces,
    selectedWorkspaceId,
  } = props;
  const { t } = useTranslation();

  // Flash workspace: lazily fetched once, cached forever.
  const { data: flashWs } = useQuery({
    queryKey: queryKeys.workspaces.flash(),
    queryFn: getFlashWorkspace,
    staleTime: Infinity,
  });

  // Active workspace per mode. Flash mode uses the shared flash workspace.
  const activeWorkspaceId = mode === 'fast'
    ? (flashWs as { workspace_id?: string } | undefined)?.workspace_id ?? null
    : selectedWorkspaceId;

  // Initial thread resolution. URL `?thread=` wins, then localStorage keyed by
  // (workspace, symbol), then a new chat. This state determines which thread
  // mounts on the keyed `<ChatBody>` — changing it forces a remount so the SSE
  // engine can re-initialise with a different thread.
  const [searchParams, setSearchParams] = useSearchParams();
  const [activeThreadInit, setActiveThreadInit] = useState<string>(() => {
    const fromUrl = searchParams.get('thread');
    if (fromUrl) return fromUrl;
    return getMarketThreadId(activeWorkspaceId, symbol) ?? '__default__';
  });

  // Re-resolve thread when symbol changes — restore the last-seen thread for
  // the new symbol in the current workspace (or start a fresh chat if none).
  const lastSymbolRef = useRef(symbol);
  useEffect(() => {
    if (lastSymbolRef.current === symbol) return;
    lastSymbolRef.current = symbol;
    setActiveThreadInit(getMarketThreadId(activeWorkspaceId, symbol) ?? '__default__');
  }, [symbol, activeWorkspaceId]);

  // Reset to a fresh chat when scope (mode / workspace) changes. The user
  // explicitly chose a different scope — surface a clean slate. localStorage
  // is keyed by (workspace, symbol), so the previous scope's pointer stays
  // intact and is reachable via the history dropdown.
  const prevWorkspaceRef = useRef<string | null>(activeWorkspaceId);
  useEffect(() => {
    const prev = prevWorkspaceRef.current;
    if (prev === activeWorkspaceId) return;
    prevWorkspaceRef.current = activeWorkspaceId;
    if (prev === null || activeWorkspaceId === null) return;
    setActiveThreadInit(`__default__#${Date.now()}`);
    setSearchParams((p) => {
      const next = new URLSearchParams(p);
      if (next.has('thread')) next.delete('thread');
      return next;
    }, { replace: true });
  }, [activeWorkspaceId, setSearchParams]);

  const handleSelectThread = useCallback((threadId: string) => {
    setMarketThreadId(activeWorkspaceId, symbol, threadId);
    setActiveThreadInit(threadId);
  }, [activeWorkspaceId, symbol]);

  const handleStartNewChat = useCallback(() => {
    clearMarketThreadId(activeWorkspaceId, symbol);
    // Force a remount even if we were already on __default__ — append a
    // monotonic suffix that's stripped before passing to useChatMessages.
    setActiveThreadInit(`__default__#${Date.now()}`);
    // Clear `?thread=` from the URL too — otherwise a refresh on the
    // empty new-chat panel would restore the old thread from the URL,
    // beating the freshly-cleared localStorage entry.
    setSearchParams((p) => {
      const next = new URLSearchParams(p);
      if (next.has('thread')) next.delete('thread');
      return next;
    }, { replace: true });
  }, [activeWorkspaceId, symbol, setSearchParams]);

  if (mode === 'ptc' && !activeWorkspaceId) {
    return (
      <div className="market-panel" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1, padding: 16 }}>
        <span style={{ color: 'var(--color-text-tertiary)', fontSize: 14 }}>
          {t('marketView.chatPanel.noWorkspacePrompt')}
        </span>
      </div>
    );
  }

  if (!activeWorkspaceId) {
    return (
      <div className="market-panel" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1, padding: 16 }}>
        <LogoLoading size={36} color="var(--color-accent-overlay)" />
      </div>
    );
  }

  return (
    <WorkspaceProvider workspaceId={activeWorkspaceId} downloadFile={null}>
      <ChatBody
        key={`${activeWorkspaceId}:${activeThreadInit}`}
        {...props}
        activeWorkspaceId={activeWorkspaceId}
        initialThreadId={activeThreadInit.split('#')[0]}
        ptcWorkspaces={workspaces}
        onSelectThread={handleSelectThread}
        onStartNewChat={handleStartNewChat}
      />
    </WorkspaceProvider>
  );
}

interface ChatBodyProps extends MarketChatPanelProps {
  activeWorkspaceId: string;
  initialThreadId: string;
  ptcWorkspaces: Workspace[];
  onSelectThread: (threadId: string) => void;
  onStartNewChat: () => void;
}

function ChatBody(props: ChatBodyProps): React.ReactElement {
  const {
    symbol,
    mode,
    onModeChange,
    ptcWorkspaces,
    selectedWorkspaceId,
    onWorkspaceChange,
    chartImage,
    chartImageDesc,
    onCaptureChart,
    onClearChartImage,
    prefillMessage,
    onClearPrefill,
    quickQueries,
    onQuickQuery,
    onShuffleQueries,
    onNavigateSubagent,
    placeholder,
    activeWorkspaceId,
    initialThreadId,
    onSelectThread,
    onStartNewChat,
  } = props;

  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [, setSearchParams] = useSearchParams();
  const [dialogPayload, setDialogPayload] = useState<DialogPayload | null>(null);

  const messagesContainerRef = useRef<HTMLDivElement | null>(null);
  const isNarrowChat = useNarrowContainer(messagesContainerRef, 640);

  // PTC zero-state — disable PTC option if user has no non-flash workspaces.
  const ptcDisabledReason = ptcWorkspaces.length === 0
    ? t('marketView.chatPanel.ptcDisabledReason')
    : null;

  const agentMode = mode === 'fast' ? 'flash' : 'ptc';

  const handlePreview = useCallback((preview: PreviewData) => {
    setDialogPayload({ type: 'preview', preview });
  }, []);

  // Origin tag — symbol uppercased so AAPL/aapl collapse. Validated server-side
  // against ^[a-z_]+(:[A-Z][A-Z0-9.]*)?$ — only A-Z, digits, and `.` allowed in
  // the suffix, so strip anything else from the user-typed symbol.
  const platformValue = useMemo(() => {
    const cleaned = (symbol || '').trim().toUpperCase().replace(/[^A-Z0-9.]/g, '');
    return cleaned ? `market_view:${cleaned}` : 'market_view';
  }, [symbol]);

  const chat = useChatMessages(
    activeWorkspaceId,
    initialThreadId,
    null,                       // updateTodoListCard
    null,                       // updateSubagentCard
    null,                       // inactivateAllSubagents
    null,                       // finalizePendingTodos
    null,                       // onOnboardingRelatedToolComplete
    null,                       // onFileArtifact
    handlePreview,              // onPreviewUrl
    agentMode,
    null,                       // clearSubagentCards
    null,                       // onWorkspaceCreated
    platformValue,
  );

  const {
    messages,
    isLoading,
    isLoadingHistory,
    messageError,
    threadId,
    threadModels,
    handleSendMessage,
    getSubagentHistory,
  } = chat;

  // Subagent telemetry resolver — feeds ActivityBlock's live token counts.
  // MarketView has no floating cards layer, so we resolve through history only.
  const resolveSubagentTelemetry = useCallback((subagentId: string) => {
    const history = getSubagentHistory?.(subagentId) as SubagentHistoryLike | undefined;
    return resolveSubagentTelemetryPure(undefined, history);
  }, [getSubagentHistory]);

  // Persist thread id per (workspace, symbol) + sync to URL once the workflow
  // assigns a real id.
  useEffect(() => {
    if (!threadId || threadId === '__default__') return;
    setMarketThreadId(activeWorkspaceId, symbol, threadId);
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (next.get('thread') !== threadId) {
        next.set('thread', threadId);
      }
      return next;
    }, { replace: true });
    queryClient.invalidateQueries({ queryKey: queryKeys.threads.byWorkspace(activeWorkspaceId) });
  }, [threadId, symbol, activeWorkspaceId, setSearchParams, queryClient]);

  // Send: shape attachments + chart screenshot like ChatAgent does.
  const handleSend = useCallback(
    (
      message: string,
      planMode: boolean,
      attachments: AttachmentItem[] = [],
      _slashCommands: unknown[] = [],
      modelOptions: { model?: string | null; reasoningEffort?: string | null; fastMode?: boolean | null } = {},
    ) => {
      const contexts: Record<string, unknown>[] = [];
      const metaItems: Record<string, unknown>[] = [];

      if (chartImage) {
        contexts.push({ type: 'image', data: chartImage, description: chartImageDesc || undefined });
        metaItems.push({
          name: chartImageDesc || 'Chart',
          type: 'image',
          size: 0,
          preview: chartImage,
          dataUrl: chartImage,
        });
      }
      if (attachments && attachments.length > 0) {
        contexts.push(...(attachmentsToContexts(attachments as never[]) as unknown as Record<string, unknown>[]));
        attachments.forEach((a) => {
          metaItems.push({
            name: a.file.name,
            type: a.type,
            size: a.file.size,
            preview: a.preview ?? null,
            dataUrl: a.dataUrl,
          });
        });
      }

      const additionalContext = contexts.length > 0 ? contexts : null;
      const attachmentMeta = metaItems.length > 0 ? metaItems : null;

      handleSendMessage(message, planMode, additionalContext, attachmentMeta, modelOptions);
      onClearChartImage();
    },
    [chartImage, chartImageDesc, handleSendMessage, onClearChartImage],
  );

  // Track whether the user is currently parked near the bottom of the
  // message list. Auto-scroll only fires while this is true, so a user
  // who has scrolled up to read earlier content during an active stream
  // isn't yanked back down on every SSE chunk.
  const isNearBottomRef = useRef(true);
  useEffect(() => {
    const el = messagesContainerRef.current;
    if (!el) return;
    const onScroll = () => {
      const threshold = 120;
      isNearBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < threshold;
    };
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, []);

  // Auto-scroll to bottom on new messages / streaming — only when the
  // user is already near the bottom (see isNearBottomRef above).
  useEffect(() => {
    if (!isNearBottomRef.current) return;
    const el = messagesContainerRef.current;
    if (!el || messages.length === 0) return;
    const id = setTimeout(() => {
      el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
    }, 80);
    return () => clearTimeout(id);
  }, [messages]);

  // Subagent navigation — chips deep-link to ChatAgent for full subagent view.
  const handleOpenSubagentTask = useCallback((info: SubagentInfo) => {
    if (!info.subagentId || !threadId || threadId === '__default__') return;
    onNavigateSubagent?.(threadId, info.subagentId);
  }, [threadId, onNavigateSubagent]);

  const handleToolCallDetailClick = useCallback((proc: Record<string, unknown>) => {
    setDialogPayload({ type: 'toolcall', toolCallProcess: proc as ToolCallProcessRecord });
  }, []);

  const initialModel = useMemo(() => threadModels[threadModels.length - 1] ?? null, [threadModels]);

  const showQuickQueries = messages.length === 0 && !isLoading && !isLoadingHistory;

  // Session title: first user message text (truncated) or fallback to "New chat".
  const newChatLabel = t('marketView.chatHistory.newChat');
  const activeTitle = useMemo(() => {
    if (!threadId || threadId === '__default__') return newChatLabel;
    const firstUser = (messages as unknown as Array<Record<string, unknown>>).find(
      (m) => (m.role as string) === 'user',
    );
    const raw = (firstUser?.content as string) || '';
    const trimmed = raw.trim();
    if (!trimmed) return newChatLabel;
    return trimmed.length > 40 ? `${trimmed.slice(0, 40)}…` : trimmed;
  }, [messages, threadId, newChatLabel]);

  return (
    <div className="market-panel">
      {/* Header — session title (left) doubles as history dropdown trigger */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'flex-start',
          padding: '6px 12px',
          borderBottom: '1px solid var(--color-border-muted)',
          flexShrink: 0,
        }}
      >
        <MarketChatHistoryButton
          workspaceId={activeWorkspaceId}
          activeThreadId={threadId === '__default__' ? null : threadId}
          activeTitle={activeTitle}
          onSelectThread={onSelectThread}
          onStartNewChat={onStartNewChat}
        />
      </div>

      {/* Messages */}
      <div
        ref={messagesContainerRef}
        style={{ flex: 1, minHeight: 0, overflowY: 'auto', overflowX: 'hidden' }}
      >
        {messages.length === 0 && !isLoading && !isLoadingHistory ? (
          <div className="market-chat-empty-state" style={{ height: '100%' }}>
            <LogoLoading size={60} color="var(--color-accent-overlay)" />
            <p className="market-chat-empty-text" style={{ marginTop: 16 }}>
              {t('marketView.chatPanel.startConversation')}
            </p>
            {messageError && (
              <div style={{ margin: '16px 24px 0', maxWidth: '100%', width: '100%' }}>
                <ErrorBanner error={messageError} />
              </div>
            )}
          </div>
        ) : (
          <div style={{ padding: '16px 24px', maxWidth: '100%' }}>
            <SubagentTelemetryContext.Provider value={resolveSubagentTelemetry}>
              <MessageList
                messages={messages as never[]}
                isLoading={isLoading}
                isLoadingHistory={isLoadingHistory}
                hideAvatar={isNarrowChat}
                onOpenSubagentTask={handleOpenSubagentTask}
                onToolCallDetailClick={handleToolCallDetailClick}
                onWidgetSendPrompt={(text: string) => handleSendMessage(text, false, null, null, {})}
              />
            </SubagentTelemetryContext.Provider>
            {messageError && (
              <div style={{ margin: '8px 0' }}>
                <ErrorBanner error={messageError} />
              </div>
            )}
          </div>
        )}
      </div>

      {/* Quick queries (empty state) */}
      {showQuickQueries && quickQueries.length > 0 && (
        <div className="market-quick-queries">
          {quickQueries.map((q, i) => (
            <button key={i} className="market-quick-query-card" onClick={() => onQuickQuery(q)}>
              {q}
            </button>
          ))}
          <button
            className="market-quick-query-shuffle"
            onClick={onShuffleQueries}
            title="Show different suggestions"
          >
            <RefreshCw size={13} />
          </button>
        </div>
      )}

      {/* Input */}
      <ChatInput
        onSend={handleSend as never}
        isLoading={isLoading}
        mode={mode}
        onModeChange={onModeChange}
        ptcDisabledReason={ptcDisabledReason}
        workspaces={ptcWorkspaces as never}
        selectedWorkspaceId={selectedWorkspaceId}
        onWorkspaceChange={onWorkspaceChange}
        onCaptureChart={onCaptureChart}
        chartImage={chartImage}
        onRemoveChartImage={onClearChartImage}
        prefillMessage={prefillMessage}
        onClearPrefill={onClearPrefill}
        placeholder={placeholder ?? t('marketView.chatPanel.defaultPlaceholder')}
        initialModel={initialModel}
        threadModels={threadModels}
      />

      <MarketDetailDialog payload={dialogPayload} onClose={() => setDialogPayload(null)} />
    </div>
  );
}
