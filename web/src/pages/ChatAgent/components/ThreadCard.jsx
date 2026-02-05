import React from 'react';

/**
 * ThreadCard Component
 * 
 * Displays a single thread as a card with:
 * - Thread ID as the name
 * - Status badge
 * - Click handler to navigate to the thread conversation
 * 
 * Note: No delete or info icons as per requirements
 * 
 * @param {Object} thread - Thread object with thread_id, current_status, etc.
 * @param {Function} onClick - Callback when card is clicked
 */
function ThreadCard({ thread, onClick }) {
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
      {/* Thread ID as name */}
      <h3 className="text-lg font-semibold pr-4" style={{ color: '#FFFFFF' }}>
        {thread.thread_id}
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
