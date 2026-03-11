import React, { Suspense, useCallback, useState, useEffect } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../lib/queryKeys';
import { getWorkspaceThreads, getThread } from './utils/api';
import ChatView from './components/ChatView';
import './ChatAgent.css';

const WorkspaceGallery = React.lazy(() => import('./components/WorkspaceGallery'));
const ThreadGallery = React.lazy(() => import('./components/ThreadGallery'));

interface LocationState {
  workspaceId?: string;
  workspaceName?: string;
  workspaceStatus?: string | null;
  agentMode?: string;
  initialMessage?: string;
  [key: string]: unknown;
}

interface ThreadErrorResponse {
  response?: { status?: number };
}

/**
 * ChatAgent Component
 *
 * Main component for the chat module that handles routing:
 * - /chat -> Shows workspace gallery
 * - /chat/:workspaceId -> Shows thread gallery for specific workspace
 * - /chat/t/:threadId -> Shows chat interface for specific thread
 *
 * Uses React Router to determine which view to display.
 */
function ChatAgent(): React.ReactElement | null {
  const { workspaceId: urlWorkspaceId, threadId } = useParams<{ workspaceId?: string; threadId?: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const { t } = useTranslation();
  const state = location.state as LocationState | null;

  // Resolve workspaceId: URL param (thread gallery) > location state (navigated from app) > API lookup
  const [resolvedWorkspaceId, setResolvedWorkspaceId] = useState<string | null>(
    urlWorkspaceId || state?.workspaceId || null
  );
  const needsThreadLookup = !!threadId && threadId !== '__default__' && !urlWorkspaceId && !state?.workspaceId;

  const { data: resolvedThread, error: threadError } = useQuery({
    queryKey: queryKeys.threads.detail(threadId!),
    queryFn: () => getThread(threadId!),
    enabled: needsThreadLookup,
    retry: false,
  });

  const accessDenied = (threadError as ThreadErrorResponse | null)?.response?.status === 403;

  // Set resolvedWorkspaceId from thread lookup result
  useEffect(() => {
    if ((resolvedThread as Record<string, unknown> | undefined)?.workspace_id) {
      setResolvedWorkspaceId((resolvedThread as Record<string, unknown>).workspace_id as string);
    }
  }, [resolvedThread]);

  // Redirect on non-403 thread lookup errors
  useEffect(() => {
    if (threadError && !accessDenied) {
      navigate('/chat', { replace: true });
    }
  }, [threadError, accessDenied, navigate]);

  // __default__ with lost state — redirect
  useEffect(() => {
    if (threadId === '__default__' && !resolvedWorkspaceId) {
      navigate('/chat', { replace: true });
    }
  }, [threadId, resolvedWorkspaceId, navigate]);

  // Sync resolvedWorkspaceId when URL params or location state change
  // Use synchronous update to avoid stale workspace on first render after navigation
  const incomingWsId = urlWorkspaceId || state?.workspaceId || null;
  if (incomingWsId && incomingWsId !== resolvedWorkspaceId) {
    setResolvedWorkspaceId(incomingWsId);
  }

  const workspaceId = incomingWsId || resolvedWorkspaceId;
  const queryClient = useQueryClient();

  /**
   * Handles workspace selection from gallery
   * Passes workspace name via route state to avoid refetching all workspaces
   */
  const handleWorkspaceSelect = useCallback((selectedWorkspaceId: string, workspaceName?: string, workspaceStatus?: string) => {
    navigate(`/chat/${selectedWorkspaceId}`, {
      state: {
        workspaceName: workspaceName || 'Workspace',
        workspaceStatus: workspaceStatus || null,
      },
    });
  }, [navigate]);

  const handleBackToWorkspaceGallery = useCallback(() => {
    navigate('/chat');
  }, [navigate]);

  const handleBackToThreadGallery = useCallback(() => {
    if (workspaceId) {
      // Preserve workspace name and status when navigating back from chat
      const cached = queryClient.getQueryData(queryKeys.workspaces.detail(workspaceId)) as Record<string, unknown> | undefined;
      navigate(`/chat/${workspaceId}`, {
        state: {
          workspaceName: cached?.name || state?.workspaceName,
          workspaceStatus: state?.workspaceStatus || null,
        },
      });
    } else {
      navigate('/chat');
    }
  }, [navigate, workspaceId, state, queryClient]);

  const handleThreadSelect = useCallback((selectedWorkspaceId: string, selectedThreadId: string, agentMode?: string | null) => {
    navigate(`/chat/t/${selectedThreadId}`, {
      state: {
        workspaceId: selectedWorkspaceId,
        ...(agentMode ? { agentMode } : {}),
        workspaceStatus: state?.workspaceStatus || null,
      },
    });
  }, [navigate, state]);

  /**
   * Prefetch thread data on workspace card hover
   */
  const prefetchThreads = useCallback((wsId: string) => {
    queryClient.prefetchQuery({
      queryKey: queryKeys.threads.byWorkspace(wsId),
      queryFn: () => getWorkspaceThreads(wsId),
      staleTime: 30_000,
    });
  }, [queryClient]);

  // Determine view key for AnimatePresence transitions
  // threadId = chat view, urlWorkspaceId (no threadId) = thread gallery, neither = workspace gallery
  const viewKey = threadId
    ? `chat-${workspaceId || 'resolving'}`
    : urlWorkspaceId
      ? `threads-${urlWorkspaceId}`
      : 'gallery';

  let content: React.ReactNode;
  if (threadId && accessDenied) {
    content = (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: 12, color: 'var(--text-secondary, #888)', padding: 24 }}>
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.5 }}>
          <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
          <path d="M7 11V7a5 5 0 0 1 10 0v4" />
        </svg>
        <div style={{ fontSize: 16, fontWeight: 500, color: 'var(--text-primary, #ccc)' }}>{t('chat.accessDeniedTitle')}</div>
        <div style={{ fontSize: 14 }}>{t('chat.accessDeniedDesc')}</div>
        <button
          onClick={() => navigate('/chat', { replace: true })}
          style={{ marginTop: 8, padding: '8px 20px', borderRadius: 8, border: '1px solid var(--border-color, #333)', background: 'transparent', color: 'var(--text-primary, #ccc)', cursor: 'pointer', fontSize: 14 }}
        >
          {t('chat.goToChats')}
        </button>
      </div>
    );
  } else if (threadId) {
    if (!workspaceId) {
      // Still resolving workspace from API — show nothing (loading state)
      content = null;
    } else {
      const cached = queryClient.getQueryData(queryKeys.workspaces.detail(workspaceId)) as Record<string, unknown> | undefined;
      const cachedWorkspaceName = (cached?.name as string) || state?.workspaceName || '';
      content = <ChatView key={`${workspaceId}-${threadId}`} workspaceId={workspaceId} threadId={threadId} onBack={handleBackToThreadGallery} workspaceName={cachedWorkspaceName} />;
    }
  } else if (urlWorkspaceId) {
    content = (
      <Suspense fallback={null}>
        <ThreadGallery
          workspaceId={urlWorkspaceId}
          onBack={handleBackToWorkspaceGallery}
          onThreadSelect={handleThreadSelect}
        />
      </Suspense>
    );
  } else {
    content = (
      <Suspense fallback={null}>
        <WorkspaceGallery
          onWorkspaceSelect={handleWorkspaceSelect}
          prefetchThreads={prefetchThreads}
        />
      </Suspense>
    );
  }

  return (
    <AnimatePresence mode="wait">
      <motion.div
        key={viewKey}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.15, ease: [0.22, 1, 0.36, 1] }}
        style={{ height: '100%' }}
      >
        {content}
      </motion.div>
    </AnimatePresence>
  );
}

export default ChatAgent;
