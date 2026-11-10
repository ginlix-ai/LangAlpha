import { useLocation, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useChatRoute } from '@/components/Sidebar/useChatRoute';
import { toast } from '@/components/ui/use-toast';
import { useAllWorkspaces } from '@/hooks/useAllWorkspaces';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { copyText } from '@/lib/clipboard';
import { isDesktopShell } from '@/lib/desktop';
import { relativeTime } from '@/lib/format';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { buildMarketViewUrl } from '@/pages/MarketView/utils/marketRoute';
import { fileHref, type Names, type NewWork, type Verbs } from './rows';

/** The user's workspaces by id, and Home as its own lookup knows it. */
function useWorkspaces() {
  const { data, isPending: homePending } = useQuery(homeWorkspaceQuery(useQueryClient()));
  const { data: all } = useAllWorkspaces({ sortBy: 'activity', includeHome: true });
  const workspaces = all?.workspaces ?? [];
  return { workspaces, byId: new Map(workspaces.map((ws) => [ws.workspace_id, ws])), home: data, homePending };
}

/** How rows (and the preview) name workspaces, agents and times. */
export function useNames(): Names {
  const locale = useLocale();
  const now = useNow();
  const label = useWorkspaceLabel();
  const { byId, home } = useWorkspaces();
  return {
    workspaceName: (id, given = '') => {
      const ws = byId.get(id);
      return ws ? label(ws) : given;
    },
    isHome: (id) => (byId.has(id) ? isHomeWorkspace(byId.get(id)) : id === home?.workspace_id),
    when: (iso) => relativeTime(iso, locale, now),
  };
}

/**
 * Where new work goes: the workspace on screen, else Home and its Chief of
 * Staff. A thread opened by its link names its workspace only once looked
 * up; until then the place is pending, so Enter waits rather than send new
 * work to Home, and no row names a place yet.
 */
export function usePlace(): NewWork {
  const { t } = useTranslation();
  const label = useWorkspaceLabel();
  const { currentWorkspaceId, resolvingWorkspace } = useChatRoute();
  const { workspaces, byId, home, homePending } = useWorkspaces();
  const workspaceId = currentWorkspaceId ?? home?.workspace_id ?? workspaces.find(isHomeWorkspace)?.workspace_id;
  // Outside a workspace on a cold load, Home may not be read yet either.
  const pending = resolvingWorkspace || (!workspaceId && homePending);
  const workspace = workspaceId ? byId.get(workspaceId) : undefined;
  return {
    workspaceId,
    pending,
    // With Home unread for good (its lookup failed), a new thread would open
    // in whichever workspace was in view last, so rows that start one are
    // left out. (A thread whose lookup fails is left for the chat home,
    // where Home is the place.)
    canStartThread: pending || !!workspaceId,
    where: pending
      ? undefined
      : (workspace ? isHomeWorkspace(workspace) : !currentWorkspaceId)
        ? t('agents.chiefOfStaff')
        : t('palette.analystIn', { workspace: label(workspace) }),
  };
}

/** What the palette's rows do in the app. Each runs after the palette has closed. */
export function useVerbs(): Verbs {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const location = useLocation();
  const label = useWorkspaceLabel();
  const { currentWorkspaceId, currentThreadId } = useChatRoute();
  const { byId } = useWorkspaces();
  const names = useNames();
  const newWork = usePlace();

  return {
    ...names,
    t,
    newWork,
    // A thread opens in its workspace without looking it up, as a restored
    // session does; a match opens it at the turn (ChatView reads `focusTurn`).
    openThread: (threadId, workspaceId, also = {}) => navigate(`/chat/t/${threadId}`, { state: { workspaceId, ...also } }),
    newThread: (workspaceId = newWork.workspaceId, draft = {}) => {
      const ws = workspaceId ? byId.get(workspaceId) : undefined;
      navigate('/chat/t/__default__', {
        state: {
          workspaceId,
          workspaceName: label(ws),
          workspaceStatus: ws?.status ?? null,
          draftMessage: draft.message,
          draftCommands: draft.commands,
        },
      });
    },
    // In the chat on screen when the file is in its workspace, else on the workspace's page.
    openFile: (workspaceId, path) => {
      if (currentThreadId && currentWorkspaceId === workspaceId) {
        navigate({ pathname: location.pathname, search: `?${new URLSearchParams({ file: path })}` }, { state: location.state });
      } else {
        navigate(fileHref(workspaceId, path), { state: { workspaceName: label(byId.get(workspaceId)) || undefined } });
      }
    },
    openChart: (symbol) => navigate(buildMarketViewUrl({ symbol })),
    copy: (text) =>
      void copyText(text).then(
        () => toast({ title: t('common.copied') }),
        () => toast({ variant: 'destructive', title: t('palette.copyFailed') }),
      ),
    openInNewTab: isDesktopShell() ? null : (path) => void window.open(path, '_blank', 'noopener'),
  };
}
