import React, { useState, useRef, useEffect } from 'react';
import { Minimize2 } from 'lucide-react';

/**
 * FloatingCard Component
 * 
 * A draggable floating card that displays special content in the ChatView.
 * Features:
 * - Draggable by clicking and dragging the header
 * - Minimizable to an icon in the top bar
 * - Position persists during the session
 * 
 * @param {Object} props
 * @param {string} props.id - Unique identifier for the card
 * @param {string} props.title - Title displayed in the card header
 * @param {React.ReactNode} props.children - Content to display in the card
 * @param {boolean} props.isMinimized - Whether the card is minimized
 * @param {Function} props.onMinimize - Callback when minimize button is clicked
 * @param {Function} props.onMaximize - Callback when card should be maximized (from icon click)
 * @param {Object} props.initialPosition - Initial position { x, y }
 * @param {Function} props.onPositionChange - Callback when position changes
 * @param {number} props.zIndex - Z-index value for stacking order
 * @param {Function} props.onBringToFront - Callback when card should be brought to front
 */
function FloatingCard({
  id,
  title,
  children,
  isMinimized,
  onMinimize,
  onMaximize,
  initialPosition = { x: 100, y: 100 },
  onPositionChange,
  zIndex = 50,
  onBringToFront,
}) {
  const [position, setPosition] = useState(initialPosition);
  const [isDragging, setIsDragging] = useState(false);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const cardRef = useRef(null);
  const headerRef = useRef(null);
  const hasInitializedRef = useRef(false);

  // Update position only on initial mount or when initialPosition actually changes (not just object reference)
  useEffect(() => {
    // Only set position on first mount
    if (!hasInitializedRef.current && initialPosition) {
      setPosition(initialPosition);
      hasInitializedRef.current = true;
    } else if (hasInitializedRef.current && initialPosition) {
      // After initialization, only update if position values actually changed
      // This prevents resetting position when card content updates but position hasn't changed
      setPosition((prevPos) => {
        if (prevPos.x !== initialPosition.x || prevPos.y !== initialPosition.y) {
          return initialPosition;
        }
        return prevPos; // Keep current position if values are the same
      });
    }
  }, [initialPosition]);

  // Handle mouse down on header to start dragging
  const handleMouseDown = (e) => {
    if (isMinimized) return;
    
    // Only start drag if clicking on header (not buttons)
    if (e.target.closest('button')) return;

    // Bring card to front when interacted with
    if (onBringToFront) {
      onBringToFront(id);
    }

    setIsDragging(true);
    const rect = cardRef.current.getBoundingClientRect();
    setDragOffset({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
    });
    e.preventDefault();
  };

  // Handle click on card to bring to front
  const handleCardClick = (e) => {
    if (isMinimized) return;
    // Don't bring to front if clicking on buttons
    if (e.target.closest('button')) return;
    if (onBringToFront) {
      onBringToFront(id);
    }
  };

  // Handle mouse move for dragging
  useEffect(() => {
    if (!isDragging) return;

    const handleMouseMove = (e) => {
      const newX = e.clientX - dragOffset.x;
      const newY = e.clientY - dragOffset.y;

      // Constrain to viewport bounds
      const maxX = window.innerWidth - (cardRef.current?.offsetWidth || 300);
      const maxY = window.innerHeight - (cardRef.current?.offsetHeight || 200);
      const constrainedX = Math.max(0, Math.min(newX, maxX));
      const constrainedY = Math.max(0, Math.min(newY, maxY));

      const newPosition = { x: constrainedX, y: constrainedY };
      setPosition(newPosition);
      if (onPositionChange) {
        onPositionChange(id, newPosition);
      }
    };

    const handleMouseUp = () => {
      setIsDragging(false);
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isDragging, dragOffset, id, onPositionChange]);

  // Don't render if minimized
  if (isMinimized) {
    return null;
  }

  return (
    <div
      ref={cardRef}
      className="fixed shadow-lg rounded-lg"
      style={{
        left: `${position.x}px`,
        top: `${position.y}px`,
        width: '520px',
        maxWidth: '125vw',
        backgroundColor: '#1B1D25',
        border: '1px solid rgba(255, 255, 255, 0.1)',
        cursor: isDragging ? 'grabbing' : 'default',
        zIndex: zIndex,
      }}
      onClick={handleCardClick}
    >
      {/* Header - draggable area */}
      <div
        ref={headerRef}
        className="flex items-center justify-between px-4 py-3 rounded-t-lg cursor-grab active:cursor-grabbing select-none"
        style={{
          backgroundColor: 'rgba(97, 85, 245, 0.1)',
          borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
        }}
        onMouseDown={handleMouseDown}
      >
        <h3 className="text-sm font-semibold" style={{ color: '#FFFFFF' }}>
          {title}
        </h3>
        <button
          onClick={onMinimize}
          className="p-1 rounded transition-colors hover:bg-white/10"
          style={{ color: '#FFFFFF' }}
          title="Minimize"
        >
          <Minimize2 className="h-4 w-4" />
        </button>
      </div>

      {/* Content */}
      <div
        className="p-4 rounded-b-lg overflow-auto"
        style={{
          maxHeight: '520px',
          color: '#FFFFFF',
        }}
      >
        {children}
      </div>
    </div>
  );
}

export default FloatingCard;
