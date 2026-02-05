import { useState } from 'react';
import { useNavigate } from 'react-router-dom';

/**
 * Custom hook for handling chat input in ThreadGallery
 * Manages message state, plan mode, and navigation to new thread
 * 
 * @param {string} workspaceId - The workspace ID to create thread in
 * @returns {Object} Input state and handlers
 */
export function useThreadGalleryInput(workspaceId) {
  const [message, setMessage] = useState('');
  const [planMode, setPlanMode] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const navigate = useNavigate();

  /**
   * Handles sending a message and navigating to a new thread
   * Creates a new thread (using '__default__') and navigates with the message
   */
  const handleSend = async () => {
    if (!message.trim() || isLoading || !workspaceId) {
      return;
    }

    setIsLoading(true);
    try {
      // Navigate to ChatAgent page with workspace, new thread, and message in state
      // Use '__default__' as threadId to create a new thread
      navigate(`/chat/${workspaceId}/__default__`, {
        state: {
          initialMessage: message.trim(),
          planMode: planMode,
        },
      });

      // Clear input
      setMessage('');
    } catch (error) {
      console.error('Error navigating to thread:', error);
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Handles key press events (Enter key to send)
   */
  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return {
    message,
    setMessage,
    planMode,
    setPlanMode,
    isLoading,
    handleSend,
    handleKeyPress,
  };
}
