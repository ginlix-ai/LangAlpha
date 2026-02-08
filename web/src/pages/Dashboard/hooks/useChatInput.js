import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useToast } from '../../../components/ui/use-toast';
import { findOrCreateDefaultWorkspace } from '../utils/workspace';

/**
 * Custom hook for handling chat input functionality
 * Manages message state, plan mode, loading state, and workspace creation dialog
 * Handles sending messages and navigating to ChatAgent workspace
 * 
 * @returns {Object} Chat input state and handlers
 */
export function useChatInput() {
  const [message, setMessage] = useState('');
  const [planMode, setPlanMode] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [showCreatingDialog, setShowCreatingDialog] = useState(false);
  const navigate = useNavigate();
  const { toast } = useToast();

  /**
   * Handles sending a message and navigating to the ChatAgent workspace
   * Finds or creates the "LangAlpha" workspace, then navigates with the message
   */
  const handleSend = async () => {
    if (!message.trim() || isLoading) {
      return;
    }

    setIsLoading(true);
    try {
      // Find or create "LangAlpha" workspace
      const workspaceId = await findOrCreateDefaultWorkspace(
        () => setShowCreatingDialog(true),
        () => setShowCreatingDialog(false)
      );

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
      console.error('Error with workspace:', error);
      toast({
        variant: 'destructive',
        title: 'Error',
        description: 'Failed to access workspace. Please try again.',
      });
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
    showCreatingDialog,
    handleSend,
    handleKeyPress,
  };
}
