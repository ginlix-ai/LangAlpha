import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { ToastAction } from '@/components/ui/toast';
import { toast } from '@/components/ui/use-toast';
import { usePreferenceKey } from '@/hooks/usePreferenceKey';
import { holdRelease, keepCopy, releaseCopy, UNDO_MS } from './keptCopy';
import {
  addDashboard,
  MAX_DASHBOARDS,
  readCachedDashboards,
  readSavedDashboards,
  removeDashboard,
  restoreDashboard,
  SAVED_DASHBOARDS_KEY,
  selectDashboard,
  type Removed,
  type SavedDashboard,
} from './saved';

/** Route state that opens the dashboard's switcher, for "remove one to add this". */
export const OPEN_SWITCHER = 'openDashboardSwitcher';

export type AddResult = 'saved' | 'full' | 'notLoaded' | 'failed';

export interface SavedDashboardsApi {
  list: SavedDashboard[];
  /** The one shown as the dashboard, or null for the built-in one. */
  current: SavedDashboard | null;
  /** Show this one, or the built-in dashboard for null. */
  select: (code: string | null) => void;
  /** Add it (or refresh what is saved for it) and show it; says why when it could not. */
  add: (dashboard: SavedDashboard) => Promise<AddResult>;
  /** Take it off the list now, with an Undo; a page's copy is let go once that is gone. */
  remove: (code: string) => void;
  /** Whether an entry like this would fit, telling the user why not. Checked before slow work. */
  canAdd: (listed: (dashboard: SavedDashboard) => boolean) => boolean;
}

function useListToasts() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const full = () =>
    toast({
      description: t('customDashboard.full', { max: MAX_DASHBOARDS }),
      action: (
        <ToastAction
          altText={t('customDashboard.manage')}
          onClick={() => navigate('/dashboard', { state: { [OPEN_SWITCHER]: true } })}
        >
          {t('customDashboard.manage')}
        </ToastAction>
      ),
    });
  /** Why a write did not land, when the user should hear it. */
  const unsaved = (result: { status: string }) => {
    if (result.status === 'notLoaded') toast({ description: t('customDashboard.notLoaded') });
    else if (result.status === 'failed') toast({ variant: 'destructive', title: t('customDashboard.saveFailed') });
  };
  return { full, unsaved, notLoaded: () => unsaved({ status: 'notLoaded' }) };
}

/**
 * The user's saved dashboards. The list is one preference key written whole,
 * so every change is built from the latest row at write time (`usePreferenceKey`).
 */
export function useSavedDashboards(): SavedDashboardsApi {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const toasts = useListToasts();
  const { value: saved, isLoaded, write } = usePreferenceKey(SAVED_DASHBOARDS_KEY, readSavedDashboards);

  // The list rides along with `current`, so it is read first: one added on
  // another device since this tab last read would be written away.
  const select = async (code: string | null) => toasts.unsaved(await write((latest) => selectDashboard(latest, code)));

  const add = async (dashboard: SavedDashboard): Promise<AddResult> => {
    const result = await write((latest) => addDashboard(latest, dashboard));
    if (result.status === 'saved') return 'saved';
    if (result.status === 'refused') {
      toasts.full();
      return 'full';
    }
    toasts.unsaved(result);
    return result.status;
  };

  const offerUndo = (removed: Removed) => {
    const { gone } = removed;
    const artifactId = gone.kind === 'page' ? gone.artifactId : null;
    const hold = artifactId ? holdRelease(qc, artifactId) : null;
    const undo = async () => {
      // The window ends at the press, not when the write lands.
      if (artifactId) keepCopy(artifactId);
      const result = await write((latest) => restoreDashboard(latest, removed));
      // Back, by this or by another tab: keep it, even past the window (a hovered toast stays up).
      if (result.status === 'saved' || (result.status === 'refused' && result.reason === 'listed')) {
        hold?.reclaim();
        return;
      }
      if (artifactId) releaseCopy(qc, artifactId);
      if (result.status === 'refused') toasts.full();
      else toasts.unsaved(result);
    };
    toast({
      description: t('customDashboard.removed', { name: gone.title || t('customDashboard.untitled') }),
      duration: UNDO_MS,
      action: (
        <ToastAction altText={t('customDashboard.undo')} onClick={() => void undo()}>
          {t('customDashboard.undo')}
        </ToastAction>
      ),
    });
  };

  const remove = async (code: string) => {
    const result = await write((latest) => removeDashboard(latest, code));
    // A failed write puts the entry back, and nothing was let go.
    if (result.status === 'saved') offerUndo(result.removed);
    else toasts.unsaved(result);
  };

  const canAdd = (listed: (dashboard: SavedDashboard) => boolean) => {
    if (!isLoaded) {
      toasts.notLoaded();
      return false;
    }
    const { list } = readCachedDashboards(qc);
    if (list.some(listed) || list.length < MAX_DASHBOARDS) return true;
    toasts.full();
    return false;
  };

  return {
    list: saved.list,
    current: saved.list.find((d) => d.code === saved.current) ?? null,
    select: (code) => void select(code),
    add,
    remove: (code) => void remove(code),
    canAdd,
  };
}
