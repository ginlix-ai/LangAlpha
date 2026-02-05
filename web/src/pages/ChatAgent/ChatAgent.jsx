import React from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import WorkspaceGallery from './components/WorkspaceGallery';
import ThreadGallery from './components/ThreadGallery';
import ChatView from './components/ChatView';
import './ChatAgent.css';

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

  /**
   * Handles workspace selection from gallery
   * Navigates to the thread gallery for the selected workspace
   * @param {string} selectedWorkspaceId - The selected workspace ID
   */
  const handleWorkspaceSelect = (selectedWorkspaceId) => {
    navigate(`/chat/${selectedWorkspaceId}`);
  };

  /**
   * Handles navigation back to workspace gallery
   */
  const handleBackToWorkspaceGallery = () => {
    navigate('/chat');
  };

  /**
   * Handles navigation back to thread gallery
   */
  const handleBackToThreadGallery = () => {
    if (workspaceId) {
      navigate(`/chat/${workspaceId}`);
    } else {
      navigate('/chat');
    }
  };

  /**
   * Handles thread selection from thread gallery
   * Navigates to the chat view for the selected workspace and thread
   * @param {string} selectedWorkspaceId - The selected workspace ID
   * @param {string} selectedThreadId - The selected thread ID
   */
  const handleThreadSelect = (selectedWorkspaceId, selectedThreadId) => {
    navigate(`/chat/${selectedWorkspaceId}/${selectedThreadId}`);
  };

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
      />
    );
  }

  // Otherwise, show workspace gallery
  return <WorkspaceGallery onWorkspaceSelect={handleWorkspaceSelect} />;
}

export default ChatAgent;
