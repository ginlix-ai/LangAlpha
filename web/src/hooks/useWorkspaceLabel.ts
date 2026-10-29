import { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import type { Workspace } from '@/types/api';

interface LabelledWorkspace extends Pick<Workspace, 'is_home'> {
  status?: string | null;
  name?: string | null;
  description?: string | null;
}

export interface WorkspaceText {
  name: string;
  description: string;
  /** The short kind a tile shows beside the date. */
  tag: string;
}

/**
 * A workspace's display text. Home, where the Chief of Staff runs, keeps its
 * stored name and description for older clients; the UI names it after the
 * agent you talk to there, "Chief of Staff", and says what that agent does.
 */
export function useWorkspaceText(): (workspace: LabelledWorkspace | null | undefined) => WorkspaceText {
  const { t } = useTranslation();
  return useCallback(
    (workspace) => {
      if (isHomeWorkspace(workspace)) {
        return {
          name: t('agents.chiefOfStaff'),
          description: t('agents.widgets.chiefOfStaffDesc'),
          tag: t('agents.widgets.tagChiefOfStaff'),
        };
      }
      return {
        name: workspace?.name ?? '',
        description: workspace?.description ?? '',
        tag: t('dashboard.widgets.workspacePicker.tagWorkspace'),
      };
    },
    [t],
  );
}

/** A workspace's display name, for a surface that shows only that. */
export function useWorkspaceLabel(): (workspace: LabelledWorkspace | null | undefined) => string {
  const text = useWorkspaceText();
  return useCallback((workspace) => text(workspace).name, [text]);
}
