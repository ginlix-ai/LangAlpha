import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useToast } from '../../../components/ui/use-toast';
import { findOrCreateDefaultWorkspace } from '../utils/workspace';
import { getFlashWorkspace, getWorkspaces } from '../../ChatAgent/utils/api';

/**
 * Custom hook for handling chat input functionality
 * Manages mode (fast/deep), workspace selection, loading state, and workspace creation dialog
 * Message and planMode are managed internally by ChatInput and passed via handleSend.
 *
 * @returns {Object} Chat input state and handlers
 */
export function useChatInput() {
  const [mode, setMode] = useState('fast'); // 'fast' or 'deep'
  const [isLoading, setIsLoading] = useState(false);
  const [showCreatingDialog, setShowCreatingDialog] = useState(false);
  const [workspaces, setWorkspaces] = useState([]);
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState(null);
  const navigate = useNavigate();
  const { toast } = useToast();

  // Fetch workspaces on mount for the workspace selector
  useEffect(() => {
    let cancelled = false;
    getWorkspaces(50, 0)
      .then((data) => {
        if (cancelled) return;
        const list = (data.workspaces || []).filter((ws) => ws.status !== 'flash');
        setWorkspaces(list);
        if (list.length > 0 && !selectedWorkspaceId) {
          setSelectedWorkspaceId(list[0].workspace_id);
        }
      })
      .catch(() => {});
    return () => { cancelled = true; };
  }, []);

  /**
   * Handles sending a message and navigating to the ChatAgent workspace
   * Fast mode: uses flash workspace (agent_mode: flash)
   * Deep mode: uses selected workspace or falls back to default LangAlpha workspace
   *
   * @param {string} message - The message text
   * @param {boolean} planMode - Whether plan mode is enabled
   * @param {Array} attachments - File attachments from ChatInput
   */
  const handleSend = async (message, planMode = false, attachments = []) => {
    const hasContent = message.trim() || (attachments && attachments.length > 0);
    if (!hasContent || isLoading) {
      return;
    }

    setIsLoading(true);
    try {
      // Build additional context and attachment metadata from attachments
      let additionalContext = null;
      let attachmentMeta = null;
      if (attachments && attachments.length > 0) {
        additionalContext = attachments.map((a) => ({
          type: 'image',
          data: a.dataUrl,
          description: a.file.name,
        }));
        attachmentMeta = attachments.map((a) => ({
          name: a.file.name,
          type: a.type,
          size: a.file.size,
          preview: a.preview || null,
          dataUrl: a.dataUrl,
        }));
      }

      if (mode === 'fast') {
        // Flash mode: get/create flash workspace and navigate
        const flashWs = await getFlashWorkspace();
        const workspaceId = flashWs.workspace_id;

        navigate(`/chat/${workspaceId}/__default__`, {
          state: {
            initialMessage: message.trim(),
            planMode: false,
            agentMode: 'flash',
            workspaceStatus: 'flash',
            ...(additionalContext ? { additionalContext } : {}),
            ...(attachmentMeta ? { attachmentMeta } : {}),
          },
        });
      } else {
        // Deep mode: use selected workspace or fall back to default
        let workspaceId = selectedWorkspaceId;
        if (!workspaceId) {
          workspaceId = await findOrCreateDefaultWorkspace(
            () => setShowCreatingDialog(true),
            () => setShowCreatingDialog(false)
          );
        }

        navigate(`/chat/${workspaceId}/__default__`, {
          state: {
            initialMessage: message.trim(),
            planMode: planMode,
            ...(additionalContext ? { additionalContext } : {}),
            ...(attachmentMeta ? { attachmentMeta } : {}),
          },
        });
      }
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

  return {
    mode,
    setMode,
    isLoading,
    showCreatingDialog,
    handleSend,
    workspaces,
    selectedWorkspaceId,
    setSelectedWorkspaceId,
  };
}
