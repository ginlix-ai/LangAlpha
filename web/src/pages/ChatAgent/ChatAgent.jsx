import React, { useRef, useCallback } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { getAuthUserId } from '@/api/client';
import { getWorkspaceThreads, DEFAULT_USER_ID } from './utils/api';
import WorkspaceGallery from './components/WorkspaceGallery';
import ThreadGallery from './components/ThreadGallery';
import ChatView from './components/ChatView';
import './ChatAgent.css';

// Module-level caches — survive ChatAgent unmount/remount from tab switching
const _workspaceCache = {};  // { [workspaceId]: { threads, workspaceName, files, fetchedAt } }
const _workspaceListCache = { workspaces: null, fetchedAt: 0 };

/**
 * ChatAgent Component
 *
 * Main component for the chat module that handles routing:
 * - /chat -> Shows workspace gallery
 * - /chat/:workspaceId -> Shows thread gallery for specific workspace
 * - /chat/:workspaceId/:threadId -> Shows chat interface for specific workspace and thread
 *
 * Uses React Router to determine which view to display.
 */
function ChatAgent() {
  const { workspaceId, threadId } = useParams();
  const navigate = useNavigate();
  const location = useLocation();

  // Refs point to module-level caches so children get a stable reference
  const workspaceCacheRef = useRef(_workspaceCache);
  const workspaceListCacheRef = useRef(_workspaceListCache);

  /**
   * Handles workspace selection from gallery
   * Passes workspace name via route state to avoid refetching all workspaces
   */
  const handleWorkspaceSelect = useCallback((selectedWorkspaceId, workspaceName) => {
    navigate(`/chat/${selectedWorkspaceId}`, {
      state: { workspaceName: workspaceName || 'Workspace' },
    });
  }, [navigate]);

  const handleBackToWorkspaceGallery = useCallback(() => {
    navigate('/chat');
  }, [navigate]);

  const handleBackToThreadGallery = useCallback(() => {
    if (workspaceId) {
      // Preserve workspace name when navigating back from chat
      const cached = workspaceCacheRef.current[workspaceId];
      navigate(`/chat/${workspaceId}`, {
        state: { workspaceName: cached?.workspaceName || location.state?.workspaceName },
      });
    } else {
      navigate('/chat');
    }
  }, [navigate, workspaceId, location.state]);

  const handleThreadSelect = useCallback((selectedWorkspaceId, selectedThreadId) => {
    navigate(`/chat/${selectedWorkspaceId}/${selectedThreadId}`);
  }, [navigate]);

  /**
   * Prefetch thread data on workspace card hover (Fix 6)
   */
  const prefetchThreads = useCallback(async (wsId) => {
    if (workspaceCacheRef.current[wsId]) return;
    try {
      const userId = getAuthUserId() || DEFAULT_USER_ID;
      const data = await getWorkspaceThreads(wsId, userId);
      workspaceCacheRef.current[wsId] = {
        threads: data.threads || [],
        fetchedAt: Date.now(),
      };
    } catch {
      // Prefetch failure is non-critical
    }
  }, []);

  // If both workspaceId and threadId are provided, show chat view
  if (workspaceId && threadId) {
    return <ChatView workspaceId={workspaceId} threadId={threadId} onBack={handleBackToThreadGallery} />;
  }

  // If only workspaceId is provided, show thread gallery
  if (workspaceId) {
    return (
      <ThreadGallery
        workspaceId={workspaceId}
        onBack={handleBackToWorkspaceGallery}
        onThreadSelect={handleThreadSelect}
        cache={workspaceCacheRef}
      />
    );
  }

  // Otherwise, show workspace gallery
  return (
    <WorkspaceGallery
      onWorkspaceSelect={handleWorkspaceSelect}
      cache={workspaceListCacheRef}
      prefetchThreads={prefetchThreads}
    />
  );
}

export default ChatAgent;
