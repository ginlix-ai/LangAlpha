import React, { useState } from 'react';
import { Info, Trash2 } from 'lucide-react';
import { DEFAULT_WORKSPACE_NAME } from '../../Dashboard/utils/workspace';

/**
 * WorkspaceCard Component
 * 
 * Displays a single workspace as a card with:
 * - Workspace name
 * - Info icon that shows description on click
 * - Delete icon that triggers deletion confirmation
 * - Click handler to navigate to the workspace chat
 * 
 * @param {Object} workspace - Workspace object with workspace_id, name, description, etc.
 * @param {Function} onClick - Callback when card is clicked
 * @param {Function} onDelete - Callback when delete icon is clicked (receives workspace)
 */
function WorkspaceCard({ workspace, onClick, onDelete }) {
  const [showDescription, setShowDescription] = useState(false);

  const handleInfoClick = (e) => {
    e.stopPropagation(); // Prevent card click when clicking info icon
    setShowDescription(!showDescription);
  };

  const handleDeleteClick = (e) => {
    e.stopPropagation(); // Prevent card click when clicking delete icon
    if (onDelete) {
      onDelete(workspace);
    }
  };

  return (
    <div
      className="relative cursor-pointer transition-all hover:scale-105"
      onClick={onClick}
      style={{
        backgroundColor: 'rgba(10, 10, 10, 0.65)',
        border: '1px solid rgba(255, 255, 255, 0.1)',
        borderRadius: '8px',
        padding: '20px',
        minHeight: '120px',
      }}
    >
      {/* Action icons */}
      <div className="absolute top-3 right-3 flex items-center gap-1">
        {/* Delete icon - Hide for default workspace (LangAlpha) */}
        {onDelete && workspace.name !== DEFAULT_WORKSPACE_NAME && (
          <button
            onClick={handleDeleteClick}
            className="p-1.5 rounded-full transition-colors hover:bg-red-500/20"
            style={{ color: '#FF383C' }}
            title="Delete workspace"
          >
            <Trash2 className="h-4 w-4" />
          </button>
        )}
        {/* Info icon */}
        <button
          onClick={handleInfoClick}
          className="p-1.5 rounded-full transition-colors hover:bg-white/10"
          style={{ color: '#6155F5' }}
          title="Show workspace info"
        >
          <Info className="h-4 w-4" />
        </button>
      </div>

      {/* Workspace name */}
      <h3 className="text-lg font-semibold pr-16" style={{ color: '#FFFFFF' }}>
        {workspace.name}
      </h3>

      {/* Info panel */}
      {showDescription && (
        <div
          className="absolute top-12 right-3 z-10 p-3 rounded-md shadow-lg max-w-xs"
          style={{
            backgroundColor: '#1C1917',
            border: '1px solid rgba(255, 255, 255, 0.2)',
            color: '#FFFFFF',
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Workspace ID */}
          <div className="mb-2">
            <p className="text-xs mb-1" style={{ color: '#FFFFFF', opacity: 0.6 }}>
              Workspace ID
            </p>
            <p className="text-xs font-mono break-all" style={{ color: '#FFFFFF', opacity: 0.9 }}>
              {workspace.workspace_id}
            </p>
          </div>
          
          {/* Description */}
          {workspace.description && (
            <div className="mt-3 pt-3" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)' }}>
              <p className="text-xs mb-1" style={{ color: '#FFFFFF', opacity: 0.6 }}>
                Description
              </p>
              <p className="text-sm" style={{ color: '#FFFFFF', opacity: 0.9 }}>
                {workspace.description}
              </p>
            </div>
          )}
        </div>
      )}

      {/* Status badge */}
      {workspace.status && (
        <div
          className="mt-3 inline-block px-2 py-1 rounded text-xs font-medium"
          style={{
            backgroundColor: workspace.status === 'running' ? 'rgba(15, 237, 190, 0.2)' : 'rgba(255, 255, 255, 0.1)',
            color: workspace.status === 'running' ? '#0FEDBE' : '#999999',
          }}
        >
          {workspace.status}
        </div>
      )}
    </div>
  );
}

export default WorkspaceCard;
