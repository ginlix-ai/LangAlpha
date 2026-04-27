import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useToast } from '../../../components/ui/use-toast';
import { getFlashWorkspace } from '../../ChatAgent/utils/api';
import { attachmentsToContexts, widgetSnapshotsToContexts } from '../../ChatAgent/utils/fileUpload';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { ContextBus } from '@/lib/contextBus';
import type { WidgetContextSnapshot } from '../widgets/framework/contextSnapshot';
import type { Workspace } from '@/types/api';

type ChatMode = 'fast' | 'ptc';

interface ChatAttachment {
  file: File;
  type: string;
  preview?: string | null;
  dataUrl: string | null;
}

interface SlashCommand {
  type: string;
  name: string;
  skillName?: string;
  description?: string;
  aliases?: string[];
}

interface SendOptions {
  model?: string | null;
  reasoningEffort?: string | null;
  fastMode?: boolean;
  widgetSnapshots?: WidgetContextSnapshot[];
}

const MAX_LOCATION_STATE_BYTES = 5 * 1024 * 1024; // ~5MB structured-clone safety net

/**
 * Manages dashboard chat input state: mode (fast/ptc), workspace selection,
 * loading, and the send handler. Message and planMode are owned by ChatInput
 * and passed through via handleSend.
 */
export function useChatInput() {
  const [mode, setModeRaw] = useState<ChatMode>('ptc');
  const [isLoading, setIsLoading] = useState(false);
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState<string | null>(null);
  const navigate = useNavigate();
  const { toast } = useToast();

  // Fetch workspaces for the workspace selector. The 100-limit matches the other
  // dashboard widgets (RecentThreads, ConversationWidget, WorkspacePicker) so all
  // four share a single React Query cache entry instead of four separate ones.
  const { data: wsData } = useWorkspaces({ limit: 100, offset: 0 });
  const workspaces = ((wsData as { workspaces?: Workspace[] })?.workspaces || []).filter((ws: Workspace) => ws.status !== 'flash');

  // Auto-select first workspace when data arrives
  useEffect(() => {
    if (workspaces.length > 0 && !selectedWorkspaceId) {
      setSelectedWorkspaceId(workspaces[0].workspace_id);
    }
  }, [workspaces, selectedWorkspaceId]);

  // Fall back to Fast mode for new users with zero real workspaces. PTC requires
  // a workspace and would dead-end their first send with "No workspace selected".
  // Once the user explicitly picks a mode, stop auto-switching.
  const userPickedModeRef = useRef(false);
  const setMode = useCallback((next: ChatMode) => {
    userPickedModeRef.current = true;
    setModeRaw(next);
  }, []);
  useEffect(() => {
    if (userPickedModeRef.current) return;
    if (wsData === undefined) return; // still loading
    if (workspaces.length === 0 && mode !== 'fast') setModeRaw('fast');
  }, [wsData, workspaces.length, mode]);

  /**
   * Navigates to the ChatAgent workspace with the composed message payload.
   * Fast mode: uses the flash workspace. PTC mode: uses the selected workspace.
   */
  const handleSend = async (
    message: string,
    planMode = false,
    attachments: ChatAttachment[] = [],
    // Slash commands are accepted to match ChatInput.onSend's signature but
    // dropped here — they apply only inside an active chat session, not on
    // the dashboard handoff.
    _slashCommands: SlashCommand[] = [],
    { model, reasoningEffort, widgetSnapshots }: SendOptions = {},
  ): Promise<void> => {
    const hasContent = message.trim() || (attachments && attachments.length > 0) || (widgetSnapshots && widgetSnapshots.length > 0);
    if (!hasContent || isLoading) {
      return;
    }

    setIsLoading(true);
    try {
      // Build additional context and attachment metadata from attachments
      let additionalContext: Array<Record<string, unknown>> | null = null;
      const toRecord = <T extends object>(x: T): Record<string, unknown> => x as unknown as Record<string, unknown>;
      let attachmentMeta: Array<{ name: string; type: string; size: number; preview: string | null; dataUrl: string | null }> | null = null;
      if (attachments && attachments.length > 0) {
        additionalContext = attachmentsToContexts(attachments as any).map(toRecord);
        attachmentMeta = attachments.map((a) => ({
          name: a.file.name,
          type: a.type,
          size: a.file.size,
          preview: a.preview || null,
          dataUrl: a.dataUrl,
        }));
      }
      // Append widget snapshot items (one widget directive + optional sibling
      // image item per snapshot). Co-located with attachments so backend reads
      // a single uniform `additional_context` array.
      if (widgetSnapshots && widgetSnapshots.length > 0) {
        const widgetItems = widgetSnapshotsToContexts(widgetSnapshots).map(toRecord);
        additionalContext = [...(additionalContext ?? []), ...widgetItems];
      }
      // Pre-flight size check on the location.state payload before navigate.
      // Structured clone fails on payloads >50MB and dashboard → chat handoff
      // would crash silently. We cap at ~5MB; if oversized, drop the local
      // copy of widget snapshots from state (the additional_context still
      // carries them via the request body).
      let stateWidgetSnapshots: WidgetContextSnapshot[] | undefined = widgetSnapshots && widgetSnapshots.length > 0 ? widgetSnapshots : undefined;
      if (stateWidgetSnapshots) {
        try {
          const sz = new Blob([JSON.stringify(stateWidgetSnapshots)]).size;
          if (sz > MAX_LOCATION_STATE_BYTES) {
            console.warn('[useChatInput] widgetSnapshots state too large, dropping', sz);
            stateWidgetSnapshots = undefined;
          }
        } catch {
          stateWidgetSnapshots = undefined;
        }
      }

      if (mode === 'fast') {
        // Flash mode: get/create flash workspace and navigate
        const flashWs = await getFlashWorkspace() as { workspace_id: string };
        const workspaceId = flashWs.workspace_id;

        navigate(`/chat/t/__default__`, {
          state: {
            workspaceId,
            initialMessage: message.trim(),
            planMode: false,
            agentMode: 'flash',
            workspaceStatus: 'flash',
            ...(additionalContext ? { additionalContext } : {}),
            ...(attachmentMeta ? { attachmentMeta } : {}),
            ...(model ? { model } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
            ...(stateWidgetSnapshots ? { widgetSnapshots: stateWidgetSnapshots } : {}),
          },
        });
      } else {
        // PTC mode: use selected workspace or prompt user to create one
        let workspaceId = selectedWorkspaceId;
        if (!workspaceId) {
          toast({
            variant: 'destructive',
            title: 'No workspace selected',
            description: 'Please create a workspace first to use PTC mode.',
          });
          return;
        }

        navigate(`/chat/t/__default__`, {
          state: {
            workspaceId,
            initialMessage: message.trim(),
            planMode: planMode,
            ...(additionalContext ? { additionalContext } : {}),
            ...(attachmentMeta ? { attachmentMeta } : {}),
            ...(model ? { model } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
            ...(stateWidgetSnapshots ? { widgetSnapshots: stateWidgetSnapshots } : {}),
          },
        });
      }
      // Clear the dashboard deck so cards don't linger after navigate.
      // Snapshots ride `location.state` and are consumed inline by the
      // chat-side auto-send effect (no deck re-seed on this path).
      if (widgetSnapshots && widgetSnapshots.length > 0) {
        ContextBus.clear();
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
    handleSend,
    workspaces,
    selectedWorkspaceId,
    setSelectedWorkspaceId,
  };
}
