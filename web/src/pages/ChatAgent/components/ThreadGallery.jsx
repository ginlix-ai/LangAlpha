import React, { useState, useEffect, useRef } from 'react';
import { ArrowLeft, Loader2, Plus, Globe, Zap, ChevronDown, Send } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Input } from '../../../components/ui/input';
import ThreadCard from './ThreadCard';
import { getWorkspaceThreads, getWorkspaces } from '../utils/api';
import { DEFAULT_USER_ID } from '../utils/api';
import { useThreadGalleryInput } from '../hooks/useThreadGalleryInput';

/**
 * ThreadGallery Component
 * 
 * Displays a gallery of threads for a specific workspace.
 * Features:
 * - Lists all threads for the workspace
 * - Shows workspace name in header
 * - Back button to return to workspace gallery
 * - Empty state when no threads exist
 * 
 * @param {string} workspaceId - The workspace ID to show threads for
 * @param {Function} onBack - Callback to navigate back to workspace gallery
 * @param {Function} onThreadSelect - Callback when a thread is selected (receives workspaceId and threadId)
 */
function ThreadGallery({ workspaceId, onBack, onThreadSelect }) {
  const [threads, setThreads] = useState([]);
  const [workspaceName, setWorkspaceName] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const navigate = useNavigate();
  const loadingRef = useRef(false);

  // Chat input hook for creating new threads
  const {
    message,
    setMessage,
    planMode,
    setPlanMode,
    isLoading: isInputLoading,
    handleSend,
    handleKeyPress,
  } = useThreadGalleryInput(workspaceId);

  // Load workspace name and threads on mount
  useEffect(() => {
    if (!workspaceId) return;
    
    // Guard: Prevent duplicate calls
    if (loadingRef.current) {
      return;
    }
    
    loadingRef.current = true;
    loadData().finally(() => {
      loadingRef.current = false;
    });
  }, [workspaceId]);

  /**
   * Fetches workspace name and threads from the API
   */
  const loadData = async () => {
    try {
      setIsLoading(true);
      setError(null);
      
      // Load workspace name and threads in parallel
      const [workspacesData, threadsData] = await Promise.all([
        getWorkspaces(DEFAULT_USER_ID).catch(() => ({ workspaces: [] })),
        getWorkspaceThreads(workspaceId, DEFAULT_USER_ID),
      ]);
      
      // Find workspace name
      const workspace = workspacesData.workspaces?.find(
        (ws) => ws.workspace_id === workspaceId
      );
      setWorkspaceName(workspace?.name || 'Workspace');
      
      // Set threads
      setThreads(threadsData.threads || []);
    } catch (err) {
      console.error('Error loading threads:', err);
      setError('Failed to load threads. Please refresh the page.');
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Handles thread selection
   * @param {Object} thread - The selected thread
   */
  const handleThreadClick = (thread) => {
    if (onThreadSelect) {
      onThreadSelect(workspaceId, thread.thread_id);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 animate-spin" style={{ color: '#6155F5' }} />
          <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.65 }}>
            Loading threads...
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4 max-w-md text-center px-4">
          <p className="text-sm" style={{ color: '#FF383C' }}>
            {error}
          </p>
          <button
            onClick={loadData}
            className="px-4 py-2 rounded-md text-sm font-medium transition-colors"
            style={{
              backgroundColor: '#6155F5',
              color: '#FFFFFF',
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col" style={{ backgroundColor: '#1B1D25' }}>
      {/* Header */}
      <div
        className="flex items-center justify-between px-6 py-4 flex-shrink-0"
        style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}
      >
        <div className="flex items-center gap-4">
          <button
            onClick={onBack}
            className="p-2 rounded-md transition-colors hover:bg-white/10"
            style={{ color: '#FFFFFF' }}
            title="Back to workspaces"
          >
            <ArrowLeft className="h-5 w-5" />
          </button>
          <h1 className="text-lg font-semibold" style={{ color: '#FFFFFF' }}>
            {workspaceName}
          </h1>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto p-6">
        {threads.length === 0 ? (
          // Empty state
          <div className="flex flex-col items-center justify-center h-full py-12">
            <p className="text-base font-medium mb-2" style={{ color: '#FFFFFF' }}>
              No threads yet
            </p>
            <p className="text-sm mb-6 text-center max-w-md" style={{ color: '#FFFFFF', opacity: 0.65 }}>
              Start a conversation to create your first thread
            </p>
          </div>
        ) : (
          // Thread grid
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {threads.map((thread) => (
              <ThreadCard
                key={thread.thread_id}
                thread={thread}
                onClick={() => handleThreadClick(thread)}
              />
            ))}
          </div>
        )}
      </div>

      {/* Chat Input Bar */}
      <div
        className="flex-shrink-0 p-4"
        style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)' }}
      >
        <div
          className="flex items-center gap-2 p-3 rounded-lg"
          style={{
            backgroundColor: 'rgba(10, 10, 10, 0.65)',
            border: '1.5px solid hsl(var(--primary))',
          }}
        >
          <button
            className="w-9 h-9 flex items-center justify-center rounded-md transition-colors hover:bg-white/5"
            style={{ color: '#BBBBBB' }}
          >
            <Plus className="h-4 w-4" />
          </button>
          <Input
            placeholder="What would you like to know?"
            className="flex-1 h-9 rounded-md text-sm focus-visible:ring-0 focus-visible:ring-offset-0 focus:outline-none"
            style={{
              backgroundColor: 'transparent',
              border: 'none',
              color: '#BBBBBB',
              fontSize: '14px',
            }}
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyPress={handleKeyPress}
            disabled={isInputLoading || !workspaceId}
          />
          <div className="flex items-center gap-1">
            <button
              className="flex items-center gap-1.5 px-2 py-1.5 rounded-full transition-colors hover:bg-white/5"
              style={{ color: '#BBBBBB' }}
            >
              <Globe className="h-4 w-4" />
              <span className="text-sm font-medium">Agent</span>
            </button>
            <button
              className={`flex items-center gap-1.5 px-2 py-1.5 rounded-full transition-colors ${
                planMode ? 'bg-white/100' : 'hover:bg-white/5'
              }`}
              style={{ color: '#BBBBBB' }}
              onClick={() => setPlanMode(!planMode)}
            >
              <Zap className="h-4 w-4" />
              <span className="text-sm font-medium">Plan Mode</span>
            </button>
            <button
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-md transition-colors hover:bg-white/5"
              style={{ color: '#BBBBBB' }}
            >
              <span className="text-sm font-medium">Tool</span>
              <ChevronDown className="h-4 w-4" />
            </button>
            <button
              className="w-8 h-9 rounded-md flex items-center justify-center transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              style={{
                backgroundColor: (isInputLoading || !message.trim()) ? 'rgba(97, 85, 245, 0.5)' : '#6155F5',
                color: '#FFFFFF',
              }}
              onClick={handleSend}
              disabled={isInputLoading || !message.trim() || !workspaceId}
            >
              <Send className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default ThreadGallery;
