import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { apiErrorReason } from '@/api/errors';
import { ToastAction } from '@/components/ui/toast';
import { toast } from '@/components/ui/use-toast';
import { artifactPathKey, useEnsureArtifact } from '@/hooks/useArtifact';
import { pageLoadsOf } from '@/lib/pageLoads';
import { listArtifactVersions } from '@/pages/ChatAgent/utils/api/artifacts';
import { keepCopy, releaseCopy, setKept } from './keptCopy';
import type { SavedDashboard } from './saved';
import { useSavedDashboards } from './useSavedDashboards';

/** Says the dashboard changed, with the way there. */
function usePinnedToast(): (name: string) => void {
  const { t } = useTranslation();
  const navigate = useNavigate();
  return (name) =>
    toast({
      description: t('customDashboard.pinned', { name }),
      action: (
        <ToastAction altText={t('customDashboard.view')} onClick={() => navigate('/dashboard')}>
          {t('customDashboard.view')}
        </ToastAction>
      ),
    });
}

/** Where a dashboard was set from, which is where an edit of it goes. */
export interface PinOrigin {
  workspaceId: string | null;
  /** The thread beside it; a route's placeholder for a thread not yet started counts as none. */
  threadId: string | null | undefined;
}

const startedThread = (id: string | null | undefined): string | null => (id && id !== '__default__' ? id : null);

/**
 * Set a running app's link as the dashboard, for the surfaces that show one.
 * `pinned` says it is the dashboard now, at any page of it.
 */
export function usePinAppAsDashboard(
  link: { code: string } | null | undefined,
  title: string,
  path: string | null | undefined,
  origin: PinOrigin,
): { pinned: boolean; pinIt: () => void } {
  const pinnedToast = usePinnedToast();
  const { current, add } = useSavedDashboards();
  const code = link?.code;
  const pinned = !!code && current?.code === code;
  const pinIt = () => {
    if (!code || pinned) return;
    void add({
      kind: 'app',
      code,
      title,
      path: path || null,
      workspaceId: origin.workspaceId,
      threadId: startedThread(origin.threadId),
    }).then((status) => {
      if (status === 'saved') pinnedToast(title);
    });
  };
  return { pinned, pinIt };
}

/**
 * Set an HTML file as the dashboard: keep it (the server cuts a version now,
 * reading the file as the owner's views do) and show it. `pending` while the
 * version is cut, which may start a stopped computer. `root` is the
 * computer's, so a path under a custom root names the same file.
 */
export function usePinPageAsDashboard(
  path: string,
  origin: PinOrigin & { workspaceId: string },
  root?: string | null,
): { pinned: boolean; pending: boolean; pinIt: () => void } {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const ensure = useEnsureArtifact();
  const pinnedToast = usePinnedToast();
  const { current, add, canAdd } = useSavedDashboards();
  const [pending, setPending] = useState(false);
  const { workspaceId } = origin;
  const key = artifactPathKey(path, root);
  const isThis = (d: SavedDashboard) => d.kind === 'page' && d.workspaceId === workspaceId && d.path === key;
  const pinned = !!current && isThis(current);

  const pinIt = async () => {
    // Asked before the keep, which may start a computer for nothing.
    if (pinned || pending || !canAdd(isThis)) return;
    setPending(true);
    let artifactId: string | null = null;
    try {
      const record = await ensure(workspaceId, key);
      artifactId = record.artifact_id;
      keepCopy(artifactId);
      const kept = await setKept(qc, artifactId, true, pageLoadsOf(record.workspace_id, record.path));
      // Named as the page names itself; the file's name is often `index`.
      const latest = (await listArtifactVersions(kept.artifact_id).catch(() => []))[0];
      const title = latest?.title || kept.name.replace(/\.html?$/i, '');
      const status = await add({
        kind: 'page',
        code: kept.code,
        title,
        path: kept.path,
        artifactId: kept.artifact_id,
        workspaceId,
        threadId: startedThread(origin.threadId),
      });
      if (status === 'saved') pinnedToast(title);
      // Kept but not listed would cut versions for nothing.
      else releaseCopy(qc, kept.artifact_id);
    } catch (err) {
      // The release this pin called off (a page removed moments ago) still has to happen.
      if (artifactId) releaseCopy(qc, artifactId);
      toast({ variant: 'destructive', title: t('customDashboard.keepFailed'), description: apiErrorReason(err) ?? undefined });
    } finally {
      setPending(false);
    }
  };
  return { pinned, pending, pinIt: () => void pinIt() };
}
