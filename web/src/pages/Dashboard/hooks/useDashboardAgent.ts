import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { toast } from '@/components/ui/use-toast';
import type { SavedDashboard } from '@/lib/dashboards';
import { resolveChatWorkspace } from '@/hooks/useHomeWorkspace';
import { apiErrorStatus } from '@/api/errors';
import { queryKeys } from '@/lib/queryKeys';
import { getShareLink } from '@/pages/ChatAgent/utils/api/shareLinks';
import { getThread } from '@/pages/ChatAgent/utils/api/threads';

/**
 * The two ways to the agent from a dashboard: build a new one, or change the
 * one on screen. Both open a chat with the request half written, for the user
 * to finish and send, since only they know what the page should show.
 */
export function useDashboardAgent(): { build: () => void; edit: (dashboard: SavedDashboard) => void } {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  // A new dashboard starts in Home: it is the user's own page rather than a
  // workspace's deliverable, and Home is on the computer they use most.
  const build = async () => {
    try {
      const workspaceId = await resolveChatWorkspace(queryClient, 'all');
      navigate('/chat/t/__default__', { state: { workspaceId, draftMessage: t('customDashboard.buildDraft') } });
    } catch {
      toast({ variant: 'destructive', title: t('customDashboard.agentFailed') });
    }
  };

  // The thread that built it knows its files and how it is fed, so an edit
  // goes back there, after anything already typed in it. Without one, or once
  // it is deleted, a new thread in its workspace, told which page by its path
  // or which app by its port.
  const edit = async (app: SavedDashboard) => {
    const name = app.title || t('customDashboard.untitled');
    const threadGone = app.threadId
      ? await getThread(app.threadId).then(() => false, (err: unknown) => apiErrorStatus(err) === 404)
      : true;
    if (app.threadId && !threadGone) {
      navigate(`/chat/t/${app.threadId}`, {
        state: { workspaceId: app.workspaceId, draftMessage: t('customDashboard.editDraft', { name }), draftAppend: true },
      });
      return;
    }
    try {
      // Under the key the app's own view reads it by, so the two share one entry.
      const link = await queryClient.fetchQuery({
        queryKey: queryKeys.shareLinks.byCode(app.code),
        queryFn: () => getShareLink(app.code),
      });
      const draftMessage = link.kind === 'file'
        ? t('customDashboard.editDraftPath', { name, path: link.path })
        : t('customDashboard.editDraftPort', { name, port: link.port });
      navigate('/chat/t/__default__', { state: { workspaceId: link.workspace_id, draftMessage } });
    } catch {
      toast({ variant: 'destructive', title: t('customDashboard.agentFailed') });
    }
  };

  return { build: () => void build(), edit: (app) => void edit(app) };
}
