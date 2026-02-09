import React, { useState, useEffect, useRef } from 'react';
import { Plus, Loader2, Search, ArrowDownUp, MoreHorizontal } from 'lucide-react';
import { useNavigate, useParams } from 'react-router-dom';
import CreateWorkspaceModal from './CreateWorkspaceModal';
import DeleteConfirmModal from './DeleteConfirmModal';
import { getAuthUserId } from '@/api/client';
import { getWorkspaces, createWorkspace, deleteWorkspace, DEFAULT_USER_ID } from '../utils/api';
import { DEFAULT_WORKSPACE_NAME } from '../../Dashboard/utils/workspace';
import { removeStoredThreadId } from '../hooks/useChatMessages';
import '../../Dashboard/Dashboard.css';

/**
 * WorkspaceGallery Component
 * 
 * Displays a gallery of workspaces as cards.
 * Features:
 * - Lists all workspaces for the user
 * - "Create Workspace" button that opens a modal
 * - Empty state when no workspaces exist
 * - Handles workspace creation
 * 
 * @param {Function} onWorkspaceSelect - Callback when a workspace is selected (receives workspaceId)
 */
function WorkspaceGallery({ onWorkspaceSelect }) {
  const [workspaces, setWorkspaces] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [deleteModal, setDeleteModal] = useState({ isOpen: false, workspace: null });
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('activity'); // 'activity' or 'name'
  const navigate = useNavigate();
  const { workspaceId: currentWorkspaceId } = useParams();
  const loadingRef = useRef(false);

  // Load workspaces on mount
  useEffect(() => {
    // Guard: Prevent duplicate calls
    if (loadingRef.current) {
      return;
    }
    
    loadingRef.current = true;
    loadWorkspaces().finally(() => {
      loadingRef.current = false;
    });
  }, []);

  /**
   * Fetches all workspaces from the API
   * Filters out '__flash__' workspaces (created by TradingCenter)
   */
  const loadWorkspaces = async () => {
    try {
      setIsLoading(true);
      setError(null);
      const userId = getAuthUserId() || DEFAULT_USER_ID;
      const data = await getWorkspaces(userId);
      // Filter out '__flash__' workspaces
      const filteredWorkspaces = (data.workspaces || []).filter(
        (ws) => ws.name !== '__flash__'
      );
      setWorkspaces(filteredWorkspaces);
    } catch (err) {
      console.error('Error loading workspaces:', err);
      setError('Failed to load workspaces. Please refresh the page.');
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Handles workspace creation
   * @param {Object} workspaceData - Object with name and description
   */
  const handleCreateWorkspace = async (workspaceData) => {
    try {
      const userId = getAuthUserId() || DEFAULT_USER_ID;
      const newWorkspace = await createWorkspace(
        workspaceData.name,
        workspaceData.description,
        {},
        userId
      );
      // Add new workspace to the list
      setWorkspaces((prev) => [newWorkspace, ...prev]);
      // Return workspace so modal can use workspace_id for file uploads
      return newWorkspace;
    } catch (err) {
      console.error('Error creating workspace:', err);
      throw err; // Let modal handle the error display
    }
  };

  /**
   * Handles delete icon click - opens confirmation modal
   * Prevents deletion of default workspace (LangAlpha)
   * @param {Object} workspace - The workspace to delete
   */
  const handleDeleteClick = (workspace) => {
    // Prevent deletion of default workspace
    if (workspace.name === DEFAULT_WORKSPACE_NAME) {
      return;
    }
    setDeleteModal({ isOpen: true, workspace });
    setDeleteError(null);
  };

  /**
   * Handles confirmed workspace deletion
   * Prevents deletion of default workspace (LangAlpha)
   */
  const handleConfirmDelete = async () => {
    if (!deleteModal.workspace) return;

    const workspaceToDelete = deleteModal.workspace;
    
    // Prevent deletion of default workspace
    if (workspaceToDelete.name === DEFAULT_WORKSPACE_NAME) {
      setDeleteError(`Cannot delete the default "${DEFAULT_WORKSPACE_NAME}" workspace.`);
      return;
    }
    
    const workspaceId = workspaceToDelete.workspace_id;

    if (!workspaceId) {
      console.error('No workspace ID found in workspace object:', workspaceToDelete);
      setDeleteError('Invalid workspace. Please try again.');
      return;
    }

    setIsDeleting(true);
    setDeleteError(null);

    try {
      await deleteWorkspace(workspaceId);

      // Clean up localStorage: remove thread ID for deleted workspace
      removeStoredThreadId(workspaceId);

      // Remove workspace from list
      setWorkspaces((prev) =>
        prev.filter((ws) => ws.workspace_id !== workspaceId)
      );

      // If the deleted workspace is currently active, navigate back to gallery
      if (currentWorkspaceId === workspaceId) {
        navigate('/chat');
      }

      // Close modal
      setDeleteModal({ isOpen: false, workspace: null });
    } catch (err) {
      console.error('Error deleting workspace:', err);
      const errorMessage = err.message || 'Failed to delete workspace. Please try again.';
      setDeleteError(errorMessage);
      // Keep modal open so user can see the error
    } finally {
      setIsDeleting(false);
    }
  };

  /**
   * Handles canceling deletion
   */
  const handleCancelDelete = () => {
    setDeleteModal({ isOpen: false, workspace: null });
    setDeleteError(null);
  };

  /**
   * Filter and sort workspaces
   */
  const filteredAndSortedWorkspaces = workspaces
    .filter((workspace) =>
      workspace.name.toLowerCase().includes(searchQuery.toLowerCase())
    )
    .sort((a, b) => {
      if (sortBy === 'activity') {
        // Sort by updated_at (most recent first)
        return new Date(b.updated_at || 0) - new Date(a.updated_at || 0);
      } else {
        // Sort by name (alphabetical)
        return a.name.localeCompare(b.name);
      }
    });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 animate-spin" style={{ color: '#6155F5' }} />
          <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.65 }}>
            Loading workspaces...
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
            onClick={loadWorkspaces}
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
    <div
      className="h-full flex flex-col overflow-hidden"
      style={{
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: 'radial-gradient(circle at center, var(--color-dot-grid) 0.75px, transparent 0.75px)',
        backgroundSize: '18px 18px',
        backgroundPosition: '0 0'
      }}
    >
      {/* Header */}
      <header className="flex w-full h-16 md:h-24 md:items-end mx-auto max-w-4xl flex-shrink-0 px-4 md:px-8">
        <div className="flex w-full items-center justify-between gap-4">
          <h1 className="text-2xl font-semibold hidden md:block dashboard-title-font" style={{ color: '#FFFFFF' }}>
            Workspaces
          </h1>
          <div></div>
          <button
            onClick={() => setIsModalOpen(true)}
            className="flex items-center gap-1.5 px-4 py-2 h-9 rounded-lg transition-all hover:scale-[1.01] active:scale-[0.985]"
            style={{
              backgroundColor: '#6155F5',
              color: '#FFFFFF',
            }}
          >
            <Plus className="h-4 w-4" />
            <span className="text-sm font-medium">New workspace</span>
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="mx-auto mt-4 w-full flex-1 px-4 md:px-8 lg:mt-6 max-w-4xl min-h-0 flex flex-col pb-0">
        <h1 className="text-xl font-semibold mb-4 md:hidden dashboard-title-font" style={{ color: '#FFFFFF' }}>
          Workspaces
        </h1>

        <div className="overflow-auto pb-20 px-1">
          <div className="sticky top-0 z-[5] flex flex-col gap-4 pb-4 md:pb-8">
            {/* Search Bar */}
            <div className="w-full">
              <div
                className="flex items-center gap-2 h-11 px-3 rounded-xl border transition-colors"
                style={{
                  backgroundColor: 'rgba(0, 0, 0, 0.3)',
                  borderColor: 'rgba(255, 255, 255, 0.1)',
                }}
              >
                <Search className="h-5 w-5 flex-shrink-0" style={{ color: '#FFFFFF', opacity: 0.4 }} />
                <input
                  className="w-full bg-transparent outline-none text-sm"
                  style={{ color: '#FFFFFF' }}
                  placeholder="Search workspaces..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
              </div>
            </div>

            {/* Sort By */}
            <div className="flex w-full gap-4 justify-between items-center">
              <div></div>
              <div className="flex items-center gap-2.5">
                <span className="text-sm hidden md:inline" style={{ color: '#FFFFFF', opacity: 0.5 }}>
                  Sort by
                </span>
                <button
                  onClick={() => setSortBy(sortBy === 'activity' ? 'name' : 'activity')}
                  className="flex items-center gap-1 md:gap-1.5 px-2 md:px-3 py-1 h-9 rounded-lg border transition-colors hover:bg-white/5"
                  style={{ borderColor: 'rgba(255, 255, 255, 0.1)', color: '#FFFFFF', opacity: 0.7 }}
                >
                  <ArrowDownUp className="h-4 w-4 md:hidden" />
                  <span className="text-sm hidden md:inline">{sortBy === 'activity' ? 'Activity' : 'Name'}</span>
                  <span className="text-sm md:hidden">{sortBy === 'activity' ? 'Activity' : 'Name'}</span>
                </button>
              </div>
            </div>
          </div>

          {/* Projects Grid */}
          {filteredAndSortedWorkspaces.length === 0 ? (
            // Empty state
            <div className="flex flex-col items-center justify-center py-12">
              <p className="text-sm mb-2" style={{ color: '#FFFFFF', opacity: 0.65 }}>
                {searchQuery ? 'No workspaces found' : 'No workspaces yet'}
              </p>
              {!searchQuery && (
                <button
                  onClick={() => setIsModalOpen(true)}
                  className="mt-4 flex items-center gap-2 px-6 py-3 rounded-lg transition-colors"
                  style={{
                    backgroundColor: '#6155F5',
                    color: '#FFFFFF',
                  }}
                >
                  <Plus className="h-5 w-5" />
                  <span className="font-medium">Create Workspace</span>
                </button>
              )}
            </div>
          ) : (
            <div className="grid gap-3 md:grid-cols-2 md:gap-6 grid-cols-1 auto-rows-fr mb-3 md:mb-6">
              {filteredAndSortedWorkspaces.map((workspace) => (
                <div key={workspace.workspace_id} className="h-full">
                  <div className="relative group h-full">
                    <div
                      onClick={() => onWorkspaceSelect(workspace.workspace_id)}
                      className="relative flex cursor-pointer flex-col overflow-hidden rounded-xl py-4 pl-5 pr-4 transition-all ease-in-out hover:shadow-sm active:scale-[0.98] h-full w-full"
                      style={{
                        background: 'linear-gradient(to bottom, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0.01))',
                        border: '0.5px solid rgba(255, 255, 255, 0.1)',
                      }}
                    >
                      <div className="flex flex-col flex-grow gap-4">
                        <div className="flex items-center pr-10 overflow-hidden">
                          <div className="font-medium truncate" style={{ color: '#FFFFFF' }}>
                            {workspace.name}
                          </div>
                        </div>
                        <div className="text-sm line-clamp-3 flex-grow" style={{ color: '#FFFFFF', opacity: 0.6 }}>
                          {workspace.description || ''}
                        </div>
                        <div className="text-xs mt-auto pt-3 flex justify-between" style={{ color: '#FFFFFF', opacity: 0.4 }}>
                          <span>
                            Updated {workspace.updated_at ? new Date(workspace.updated_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : 'recently'}
                          </span>
                        </div>
                      </div>
                    </div>
                    {/* Three dots menu */}
                    {workspace.name !== DEFAULT_WORKSPACE_NAME && (
                      <div className="absolute top-3 right-3 z-10 transition-opacity opacity-0 group-focus-within:opacity-100 group-hover:opacity-100">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDeleteClick(workspace);
                          }}
                          className="h-8 w-8 rounded-md transition-colors hover:bg-white/10 flex items-center justify-center"
                          style={{ color: '#FFFFFF', opacity: 0.7 }}
                        >
                          <MoreHorizontal className="h-5 w-5" />
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </main>

      {/* Create Workspace Modal */}
      <CreateWorkspaceModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onCreate={handleCreateWorkspace}
        onComplete={(wsId) => onWorkspaceSelect(wsId)}
      />

      {/* Delete Confirmation Modal */}
      <DeleteConfirmModal
        isOpen={deleteModal.isOpen}
        workspaceName={deleteModal.workspace?.name || ''}
        onConfirm={handleConfirmDelete}
        onCancel={handleCancelDelete}
        isDeleting={isDeleting}
        error={deleteError}
      />
    </div>
  );
}

export default WorkspaceGallery;
