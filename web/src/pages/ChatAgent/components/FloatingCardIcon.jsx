import React from 'react';
import { Bookmark } from 'lucide-react';

/**
 * FloatingCardIcon Component
 * 
 * Displays a floating card as a bookmark icon with title in the top bar.
 * Always visible regardless of card's minimized state.
 * Clicking the icon will toggle the card's minimized/maximized state.
 * Shows a green color when there's an unread update.
 * Shows a grey color when the subagent card is inactive.
 * 
 * @param {Object} props
 * @param {string} props.id - Unique identifier for the card
 * @param {string} props.title - Title/name of the card to display
 * @param {Function} props.onClick - Callback when icon is clicked to toggle card state
 * @param {boolean} props.hasUnreadUpdate - Whether the card has an unread update (shows green color)
 * @param {boolean} props.isActive - Whether the subagent card is active (shows grey if false)
 */
function FloatingCardIcon({ id, title, onClick, hasUnreadUpdate = false, isActive = true }) {
  // Determine icon color based on state:
  // 1. Green if there's an unread update
  // 2. Grey if inactive (for subagent cards)
  // 3. Purple otherwise (default)
  let iconColor = '#6155F5'; // Default purple
  if (hasUnreadUpdate) {
    iconColor = '#0FEDBE'; // Green for unread updates
  } else if (isActive === false) {
    iconColor = '#808080'; // Grey for inactive subagents
  }
  
  return (
    <button
      onClick={onClick}
      className="flex items-center gap-2 px-3 py-2 rounded-md transition-colors hover:bg-white/10"
      style={{ color: iconColor }}
      title={`${title || 'Card'}${hasUnreadUpdate ? ' (has updates)' : ''}`}
    >
      <Bookmark className="h-4 w-4 flex-shrink-0" style={{ color: iconColor }} />
      <span className="text-sm font-medium whitespace-nowrap" style={{ color: '#FFFFFF' }}>
        {title || 'Card'}
      </span>
      {/* Unread update indicator dot */}
      {hasUnreadUpdate && (
        <div
          className="w-2 h-2 rounded-full flex-shrink-0"
          style={{ backgroundColor: '#0FEDBE' }}
        />
      )}
    </button>
  );
}

export default FloatingCardIcon;
