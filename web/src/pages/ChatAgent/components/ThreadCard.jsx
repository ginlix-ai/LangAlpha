import React from 'react';
import { Trash2, Edit2 } from 'lucide-react';

/**
 * ThreadCard Component
 * 
 * Displays a single thread as a card with:
 * - Thread title or index as the name
 * - Status badge
 * - Edit icon that triggers rename modal
 * - Delete icon that triggers deletion confirmation
 * - Click handler to navigate to the thread conversation
 * 
 * @param {Object} thread - Thread object with thread_id, current_status, thread_index, title, etc.
 * @param {Function} onClick - Callback when card is clicked
 * @param {Function} onDelete - Callback when delete icon is clicked (receives thread)
 * @param {Function} onRename - Callback when edit icon is clicked (receives thread)
 */
function ThreadCard({ thread, onClick, onDelete, onRename }) {
  const handleDeleteClick = (e) => {
    e.stopPropagation(); // Prevent card click when clicking delete icon
    if (onDelete) {
      onDelete(thread);
    }
  };

  const handleRenameClick = (e) => {
    e.stopPropagation(); // Prevent card click when clicking edit icon
    if (onRename) {
      onRename(thread);
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
      {(onRename || onDelete) && (
        <div className="absolute top-3 right-3 flex items-center gap-1">
          {/* Edit/Rename icon */}
          {onRename && (
            <button
              onClick={handleRenameClick}
              className="p-1.5 rounded-full transition-colors hover:bg-white/10"
              style={{ color: '#6155F5' }}
              title="Rename thread"
            >
              <Edit2 className="h-4 w-4" />
            </button>
          )}
          {/* Delete icon */}
          {onDelete && (
            <button
              onClick={handleDeleteClick}
              className="p-1.5 rounded-full transition-colors hover:bg-red-500/20"
              style={{ color: '#FF383C' }}
              title="Delete thread"
            >
              <Trash2 className="h-4 w-4" />
            </button>
          )}
        </div>
      )}

      {/* Thread title or index */}
      <h3 className={`text-lg font-semibold ${(onRename || onDelete) ? 'pr-20' : 'pr-4'}`} style={{ color: '#FFFFFF' }}>
        {thread.title || `Thread ${thread.thread_index !== undefined ? thread.thread_index + 1 : ''}`}
      </h3>

      {/* Status badge */}
      {thread.current_status && (
        <div
          className="mt-3 inline-block px-2 py-1 rounded text-xs font-medium"
          style={{
            backgroundColor: thread.current_status === 'completed' 
              ? 'rgba(15, 237, 190, 0.2)' 
              : thread.current_status === 'running'
              ? 'rgba(97, 85, 245, 0.2)'
              : 'rgba(255, 255, 255, 0.1)',
            color: thread.current_status === 'completed' 
              ? '#0FEDBE' 
              : thread.current_status === 'running'
              ? '#6155F5'
              : '#999999',
          }}
        >
          {thread.current_status}
        </div>
      )}

      {/* Timestamp info */}
      {thread.updated_at && (
        <div className="mt-2">
          <p className="text-xs" style={{ color: '#FFFFFF', opacity: 0.5 }}>
            Updated: {new Date(thread.updated_at).toLocaleDateString()}
          </p>
        </div>
      )}
    </div>
  );
}

export default ThreadCard;
