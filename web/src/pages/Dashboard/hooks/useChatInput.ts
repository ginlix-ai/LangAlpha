import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { useToast } from '../../../components/ui/use-toast';
import type { ComposerScope } from '../../../components/ui/chat-input.types';
import { attachmentsToContexts, widgetSnapshotsToContexts } from '../../ChatAgent/utils/fileUpload';
import { warmWorkspace } from '../../ChatAgent/utils/warmWorkspace';
import { useWorkspaces } from '../../../hooks/useWorkspaces';
import { FLASH_ROUTE_STATE, flashWorkspaceQuery } from '@/hooks/useFlashWorkspace';
import { ALL_WORKSPACES_AGENT, useAllWorkspacesAgent } from '@/hooks/useAllWorkspacesAgent';
import { featuresQuery, isFeatureEnabled } from '@/hooks/useFeatures';
import { ContextBus } from '@/lib/contextBus';
import type { WidgetContextSnapshot } from '../widgets/framework/contextSnapshot';

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
  subagentsAllowed?: boolean;
}

const MAX_LOCATION_STATE_BYTES = 5 * 1024 * 1024; // ~5MB structured-clone safety net

/**
 * Manages dashboard chat input state: mode (fast/ptc), workspace selection,
 * loading, and the send handler. The message and the Subagents pick are owned
 * by ChatInput and passed through via handleSend.
 *
 * Under the all-workspaces agent the composer picks a scope instead of a mode:
 * All workspaces (the user's flash row, which the server runs as Home) or the
 * selected workspace, both on the full agent. `composerProps` carries
 * whichever the flag calls for. `composing` is the host's report that the user
 * is writing a message, which warms a picked workspace's computer.
 */
export function useChatInput({ composing = false }: { composing?: boolean } = {}) {
  const [mode, setModeRaw] = useState<ChatMode>('ptc');
  const [isLoading, setIsLoading] = useState(false);
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState<string | null>(null);
  const navigate = useNavigate();
  const { toast } = useToast();
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const allWorkspaces = useAllWorkspacesAgent();
  // Nothing picked yet means All workspaces, which needs no workspace to exist.
  const [scope, setScope] = useState<ComposerScope>('all');

  // Fetch workspaces for the workspace selector. The 100-limit matches the other
  // dashboard widgets (RecentThreads, ConversationWidget, WorkspacePicker) so all
  // four share a single React Query cache entry instead of four separate ones.
  const { data: wsData } = useWorkspaces({ limit: 100, offset: 0 });
  const workspaces = (wsData?.workspaces || []).filter((ws) => ws.status !== 'flash');

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

  // Start a picked workspace's computer while the user is still typing, so the
  // send lands on a running one. All workspaces needs nothing here: the click
  // or key that focused the composer has already warmed Home (WarmHome). Once
  // per workspace per composing spell: stopping forgets what was warmed, and
  // picking another meanwhile warms that one. With the flag off nothing here
  // warms.
  const warmedRef = useRef(new Set<string>());
  const warmTargetId = allWorkspaces && scope === 'workspace' ? selectedWorkspaceId : null;
  useEffect(() => {
    if (!composing) {
      warmedRef.current.clear();
      return;
    }
    if (!warmTargetId || warmedRef.current.has(warmTargetId)) return;
    warmedRef.current.add(warmTargetId);
    void warmWorkspace(warmTargetId, queryClient);
  }, [composing, warmTargetId, queryClient]);

  /**
   * Navigates to the ChatAgent workspace with the composed message payload:
   * the flash row for Fast mode or All workspaces, else the selected workspace.
   */
  const handleSend = async (
    message: string,
    attachments: ChatAttachment[] = [],
    // Slash commands are accepted to match ChatInput.onSend's signature but
    // dropped here — they apply only inside an active chat session, not on
    // the dashboard handoff.
    _slashCommands: SlashCommand[] = [],
    { model, reasoningEffort, widgetSnapshots, subagentsAllowed }: SendOptions = {},
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

      // Until the flags answer, the composer shows Flash's controls. A send
      // made then waits for them, so it goes to the user's default rather
      // than where those point; a read that fails leaves the flag off, as the
      // controls show.
      const flags = await queryClient.ensureQueryData(featuresQuery).catch(() => undefined);
      const onAllWorkspaces = isFeatureEnabled(flags, ALL_WORKSPACES_AGENT);
      // The flash row is Home under the all-workspaces agent, which runs the
      // full agent, so the Subagents pick rides there as to any workspace. A
      // Flash composer names none.
      const toFlashRow = onAllWorkspaces ? scope === 'all' : mode === 'fast';
      const workspaceId = toFlashRow
        ? (await queryClient.ensureQueryData(flashWorkspaceQuery(queryClient))).workspace_id
        : selectedWorkspaceId;
      if (!workspaceId) {
        toast(onAllWorkspaces
          ? { variant: 'destructive', title: t('agents.workspaceRequired') }
          : {
            variant: 'destructive',
            title: 'No workspace selected',
            description: 'Please create a workspace first to use PTC mode.',
          });
        return;
      }
      navigate(`/chat/t/__default__`, {
        state: {
          workspaceId,
          ...(toFlashRow ? FLASH_ROUTE_STATE : {}),
          initialMessage: message.trim(),
          ...(subagentsAllowed !== undefined ? { subagentsAllowed } : {}),
          ...(additionalContext ? { additionalContext } : {}),
          ...(attachmentMeta ? { attachmentMeta } : {}),
          ...(model ? { model } : {}),
          ...(reasoningEffort ? { reasoningEffort } : {}),
          ...(stateWidgetSnapshots ? { widgetSnapshots: stateWidgetSnapshots } : {}),
        },
      });
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

  // The composer's mode or scope props, by the flag, so a host never re-derives
  // which one it shows.
  const composerProps = allWorkspaces
    ? { scope, onScopeChange: setScope }
    : { mode, onModeChange: setMode };

  return {
    mode,
    scope,
    composerProps,
    isLoading,
    handleSend,
    workspaces,
    selectedWorkspaceId,
    setSelectedWorkspaceId,
  };
}
