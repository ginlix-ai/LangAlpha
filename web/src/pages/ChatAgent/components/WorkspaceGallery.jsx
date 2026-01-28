import React, { useState, useEffect, useRef } from 'react';
import { Plus, Loader2 } from 'lucide-react';
import { useNavigate, useParams } from 'react-router-dom';
import WorkspaceCard from './WorkspaceCard';
import CreateWorkspaceModal from './CreateWorkspaceModal';
import DeleteConfirmModal from './DeleteConfirmModal';
import { getWorkspaces, createWorkspace, deleteWorkspace } from '../utils/api';

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
   */
  const loadWorkspaces = async () => {
    try {
      setIsLoading(true);
      setError(null);
      const data = await getWorkspaces();
      setWorkspaces(data.workspaces || []);
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
      const newWorkspace = await createWorkspace(
        workspaceData.name,
        workspaceData.description
      );
      // Add new workspace to the list
      setWorkspaces((prev) => [newWorkspace, ...prev]);
      // Automatically navigate to the new workspace
      onWorkspaceSelect(newWorkspace.workspace_id);
    } catch (err) {
      console.error('Error creating workspace:', err);
      throw err; // Let modal handle the error display
    }
  };

  /**
   * Handles delete icon click - opens confirmation modal
   * @param {Object} workspace - The workspace to delete
   */
  const handleDeleteClick = (workspace) => {
    setDeleteModal({ isOpen: true, workspace });
    setDeleteError(null);
  };

  /**
   * Handles confirmed workspace deletion
   */
  const handleConfirmDelete = async () => {
    if (!deleteModal.workspace) return;

    const workspaceToDelete = deleteModal.workspace;
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
    <div className="h-full flex flex-col" style={{ backgroundColor: '#1B1D25' }}>
      {/* Header */}
      <div
        className="flex items-center justify-between px-6 py-4 flex-shrink-0"
        style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}
      >
        <h1 className="text-lg font-semibold" style={{ color: '#FFFFFF' }}>
          Workspaces
        </h1>
        <button
          onClick={() => setIsModalOpen(true)}
          className="flex items-center gap-2 px-4 py-2 rounded-md transition-colors"
          style={{
            backgroundColor: '#6155F5',
            color: '#FFFFFF',
          }}
        >
          <Plus className="h-4 w-4" />
          <span className="text-sm font-medium">Create Workspace</span>
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto p-6">
        {workspaces.length === 0 ? (
          // Empty state
          <div className="flex flex-col items-center justify-center h-full py-12">
            <div
              className="w-16 h-16 rounded-full flex items-center justify-center mb-4"
              style={{ backgroundColor: 'rgba(97, 85, 245, 0.2)' }}
            >
              <Plus className="h-8 w-8" style={{ color: '#6155F5' }} />
            </div>
            <p className="text-base font-medium mb-2" style={{ color: '#FFFFFF' }}>
              No workspaces yet
            </p>
            <p className="text-sm mb-6 text-center max-w-md" style={{ color: '#FFFFFF', opacity: 0.65 }}>
              Create your first workspace to start chatting with the agent
            </p>
            <button
              onClick={() => setIsModalOpen(true)}
              className="flex items-center gap-2 px-6 py-3 rounded-md transition-colors"
              style={{
                backgroundColor: '#6155F5',
                color: '#FFFFFF',
              }}
            >
              <Plus className="h-5 w-5" />
              <span className="font-medium">Create Workspace</span>
            </button>
          </div>
        ) : (
          // Workspace grid
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {workspaces.map((workspace) => (
              <WorkspaceCard
                key={workspace.workspace_id}
                workspace={workspace}
                onClick={() => onWorkspaceSelect(workspace.workspace_id)}
                onDelete={handleDeleteClick}
              />
            ))}
          </div>
        )}
      </div>

      {/* Create Workspace Modal */}
      <CreateWorkspaceModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onCreate={handleCreateWorkspace}
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
