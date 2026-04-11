import React, { Suspense, useCallback, useState, useEffect, useRef } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { motion, AnimatePresence } from 'framer-motion';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useIsMobile } from '@/hooks/useIsMobile';
import { queryKeys } from '../../lib/queryKeys';
import { getWorkspaceThreads, getThread } from './utils/api';
import { getChatSession } from './hooks/utils/chatSessionRestore';
import { useChatViewCache } from './hooks/useChatViewCache';
import ChatView from './components/ChatView';
import './ChatAgent.css';

// View depth for direction-aware transitions: gallery(0) → threads(1) → chat(2)
function getViewDepth(threadId?: string, workspaceId?: string): number {
  if (threadId) return 2;
  if (workspaceId) return 1;
  return 0;
}

const desktopFadeVariants = {
  enter: { opacity: 0 },
  center: { opacity: 1 },
  exit: { opacity: 0 },
};

const WorkspaceGallery = React.lazy(() => import('./components/WorkspaceGallery'));
const ThreadGallery = React.lazy(() => import('./components/ThreadGallery'));

interface LocationState {
  workspaceId?: string;
  workspaceName?: string;
  workspaceStatus?: string | null;
  agentMode?: string;
  initialMessage?: string;
  fromThreadId?: string;
  fromWorkspaceId?: string;
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
  const { workspaceId: urlWorkspaceId, threadId, taskId } = useParams<{ workspaceId?: string; threadId?: string; taskId?: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const state = location.state as LocationState | null;

  // Detect browser-initiated navigation (iOS swipe-back, Android back button).
  // When popstate triggers navigation, iOS Safari already shows its own page
  // transition animation. Setting direction=0 tells our variants to skip animation
  // so we don't get a double-transition flicker.
  const popstateNavRef = useRef(false);
  useEffect(() => {
    const onPopState = () => { popstateNavRef.current = true; };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, []);

  // Track navigation direction synchronously (must be computed during render
  // so AnimatePresence popLayout mode gets the correct custom prop immediately)
  const prevDepthRef = useRef(getViewDepth(threadId, urlWorkspaceId));
  const navDirectionRef = useRef(1);
  const currentDepth = getViewDepth(threadId, urlWorkspaceId);
  if (currentDepth !== prevDepthRef.current) {
    navDirectionRef.current = currentDepth > prevDepthRef.current ? 1 : -1;
    prevDepthRef.current = currentDepth;
  }
  // On mobile, use direction=0 for popstate navigations to skip our animations
  const isPopstateNav = isMobile && popstateNavRef.current;
  if (popstateNavRef.current) popstateNavRef.current = false;
  const navDirection = isPopstateNav ? 0 : navDirectionRef.current;

  // Session restore: when landing at /chat (gallery) with a saved session,
  // navigate to the deep route. This creates the natural history stack:
  // [previous page] → /chat → /chat/:workspaceId or /chat/t/:threadId
  // so Safari's back gesture goes to WorkspaceGallery, not the previous page.
  // Read session synchronously (before WorkspaceGallery mounts and clears it).
  const pendingSessionRef = useRef<ReturnType<typeof getChatSession>>(undefined as any);
  if (pendingSessionRef.current === undefined) {
    pendingSessionRef.current = (!urlWorkspaceId && !threadId) ? getChatSession() : null;
  }
  useEffect(() => {
    const session = pendingSessionRef.current;
    pendingSessionRef.current = null;
    if (!session) return;
    if (session.threadId) {
      navigate(`/chat/t/${session.threadId}`, {
        state: { workspaceId: session.workspaceId },
      });
    } else {
      navigate(`/chat/${session.workspaceId}`);
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

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

  // LRU cache for ChatView instances — keeps up to 5 alive simultaneously
  const cache = useChatViewCache();

  const queryClient = useQueryClient();

  // Track in-progress __default__ → real threadId resolutions.
  // Bridges the gap between async cache.updateKey() and immediate navigate().
  const resolvingRef = useRef(new Map<string, { workspaceId: string; newThreadId: string }>());

  // Ensure cache entry exists before first paint so chatViews is never empty
  // when threadId is set (same setState-during-render pattern as setResolvedWorkspaceId above).
  if (threadId && workspaceId && !cache.entries.some(e => e.workspaceId === workspaceId && e.threadId === threadId)) {
    // Don't create a duplicate entry if an existing entry is resolving to this threadId
    const isPendingResolution = Array.from(resolvingRef.current.values()).some(
      v => v.workspaceId === workspaceId && v.newThreadId === threadId
    );
    if (!isPendingResolution) {
      const cached = queryClient.getQueryData(queryKeys.workspaces.detail(workspaceId)) as Record<string, unknown> | undefined;
      const wsName = (cached?.name as string) || state?.workspaceName || '';
      cache.touch({ workspaceId, threadId, workspaceName: wsName, initialTaskId: taskId });
    }
  }

  // Promote to MRU and update metadata (workspace name, taskId) on subsequent renders
  useEffect(() => {
    if (!threadId || !workspaceId) return;
    // Don't touch if this threadId is pending resolution from an existing entry
    const isPendingResolution = Array.from(resolvingRef.current.values()).some(
      v => v.workspaceId === workspaceId && v.newThreadId === threadId
    );
    if (isPendingResolution) return;
    const cached = queryClient.getQueryData(queryKeys.workspaces.detail(workspaceId)) as Record<string, unknown> | undefined;
    const wsName = (cached?.name as string) || state?.workspaceName || '';
    cache.touch({ workspaceId, threadId, workspaceName: wsName, initialTaskId: taskId });
  }, [threadId, workspaceId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Clean resolvingRef once updateKey's setEntries commits
  useEffect(() => {
    if (resolvingRef.current.size === 0) return;
    const resolved: string[] = [];
    for (const [oldKey, { workspaceId: wsId, newThreadId }] of resolvingRef.current) {
      if (cache.entries.some(e => e.workspaceId === wsId && e.threadId === newThreadId)) {
        resolved.push(oldKey);
      }
    }
    for (const key of resolved) {
      resolvingRef.current.delete(key);
    }
  }, [cache.entries]);

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

  // Determine view key for AnimatePresence transitions (gallery views only)
  const viewKey = urlWorkspaceId
    ? `threads-${urlWorkspaceId}`
    : 'gallery';

  // Gallery content (workspace gallery or thread gallery)
  let galleryContent: React.ReactNode = null;
  if (!threadId) {
    if (urlWorkspaceId) {
      galleryContent = (
        <Suspense fallback={null}>
          <ThreadGallery
            workspaceId={urlWorkspaceId}
            onBack={handleBackToWorkspaceGallery}
            onThreadSelect={handleThreadSelect}
          />
        </Suspense>
      );
    } else {
      galleryContent = (
        <Suspense fallback={<div style={{ height: '100%', background: 'var(--color-bg-page, #0a0a0a)' }} />}>
          <WorkspaceGallery
            onWorkspaceSelect={handleWorkspaceSelect}
            prefetchThreads={prefetchThreads}
          />
        </Suspense>
      );
    }
  }

  // Access denied overlay (shown on top of everything)
  const accessDeniedContent = threadId && accessDenied ? (
    <div style={{ position: 'absolute', inset: 0, zIndex: 10, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: 12, color: 'var(--text-secondary, #888)', padding: 24, backgroundColor: 'var(--color-bg-page, #0a0a0a)' }}>
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
  ) : null;

  // Cached ChatView instances — always rendered, visibility toggled via display
  const chatViews = cache.entries.map(entry => {
    const pendingResolution = resolvingRef.current.get(`${entry.workspaceId}-${entry.threadId}`);
    const isEntryActive = entry.workspaceId === workspaceId
      && (entry.threadId === threadId || (!!pendingResolution && pendingResolution.newThreadId === threadId))
      && !!threadId && !accessDenied;
    return (
      <div
        key={entry.instanceId}
        style={{
          display: isEntryActive ? 'flex' : 'none',
          flexDirection: 'column' as const,
          height: '100%',
        }}
      >
        <ChatView
          workspaceId={entry.workspaceId}
          threadId={entry.threadId}
          initialTaskId={isEntryActive ? taskId : entry.initialTaskId}
          onBack={handleBackToThreadGallery}
          workspaceName={entry.workspaceName}
          isActive={isEntryActive}
          onThreadResolved={(oldTid, newTid) => {
            resolvingRef.current.set(
              `${entry.workspaceId}-${oldTid}`,
              { workspaceId: entry.workspaceId, newThreadId: newTid },
            );
            cache.updateKey(
              `${entry.workspaceId}-${oldTid}`,
              `${entry.workspaceId}-${newTid}`,
              { threadId: newTid },
            );
          }}
        />
      </div>
    );
  });

  // On mobile, skip AnimatePresence — iOS/Android provide their own page transitions.
  if (isMobile) {
    return (
      <div style={{ height: '100%', position: 'relative' }}>
        {!threadId && galleryContent}
        {chatViews}
        {accessDeniedContent}
      </div>
    );
  }

  return (
    <div style={{ height: '100%', position: 'relative' }}>
      {/* Gallery views — animated transitions (R6: z-index:1 so exit fades above chat) */}
      <div style={{ position: threadId ? 'absolute' : 'relative', height: threadId ? 0 : '100%', width: '100%', zIndex: 1, overflow: 'hidden' }}>
        <AnimatePresence mode="wait" custom={navDirection}>
          {!threadId && (
            <motion.div
              key={viewKey}
              custom={navDirection}
              variants={desktopFadeVariants}
              initial="enter"
              animate="center"
              exit="exit"
              transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
              style={{ height: '100%' }}
            >
              {galleryContent}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
      {/* Cached ChatViews — visibility toggled, never unmounted on thread switch */}
      {chatViews}
      {accessDeniedContent}
    </div>
  );
}

export default ChatAgent;
