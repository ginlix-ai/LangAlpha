import { useTranslation } from 'react-i18next';
import {
  deleteSkill,
  deleteWorkspaceSkill,
  moveSkill,
  setSkillEnabled,
  setWorkspaceSkillEnabled,
} from '@/pages/ChatAgent/utils/api';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import type { BulkAction } from '../components/BulkActionBar';
import type { BulkScopeSpec } from '../components/BulkScopeMenu';
import type { ScopeWorkspace } from '../components/ScopeControl';
import type { BulkTarget } from '../components/useBulkSelection';
import type { PluginListSurface } from './usePluginListSurface';
import { useScopeBulk } from './useScopeBulk';
import { skillRowKey } from './useSkillActions';
import { isPluginOwned } from '../utils/provenance';

/** The select-mode actions for the Skills tab: enable, disable, delete, scope. */
export function useSkillBulkActions(
  /** The rows on screen, not the whole account: see useBulkSelection. */
  visibleSkills: readonly SkillInfo[],
  surface: PluginListSurface,
  workspaces: ScopeWorkspace[],
): { actions: BulkAction[]; scope: BulkScopeSpec; count: number } {
  const { t } = useTranslation();
  const selected = visibleSkills.filter((s) =>
    surface.selection.selected.has(skillRowKey(s)),
  );

  function target(skill: SkillInfo, enabled: boolean): BulkTarget {
    return {
      key: skillRowKey(skill),
      run: () =>
        skill.origin === 'workspace' && skill.workspace_id
          ? setWorkspaceSkillEnabled(skill.workspace_id, skill.name, enabled)
          : setSkillEnabled(skill.name, enabled),
    };
  }

  // Same eligibility as each row's own ScopeControl: the deny-list checklist
  // exists on enabled user-tier rows; tier moves exist for the user's own
  // uploads and workspace rows (plugin skills stay put, a shadowing workspace
  // row can't surface to a tier where its name is taken).
  const scope = useScopeBulk<SkillInfo>(selected, {
    workspaces,
    run: surface.run,
    key: skillRowKey,
    denyMarkers: (s) =>
      s.origin !== 'workspace' && s.enabled ? (s.disabled_workspace_ids ?? []) : null,
    setWorkspaceEnabled: (s, workspaceId, enabled) =>
      setWorkspaceSkillEnabled(workspaceId, s.name, enabled),
    promote: (s) =>
      s.origin === 'workspace' && s.workspace_id && !s.shadows_inherited
        ? () => moveSkill(s.name, s.workspace_id as string, null)
        : null,
    movable: (s) =>
      (s.origin === 'user' && !isPluginOwned(s)) ||
      (s.origin === 'workspace' && !!s.workspace_id),
    moveTo: (s, workspaceId) => {
      if (s.origin === 'user' && !isPluginOwned(s)) {
        return () => moveSkill(s.name, null, workspaceId);
      }
      if (s.origin === 'workspace' && s.workspace_id && s.workspace_id !== workspaceId) {
        return () => moveSkill(s.name, s.workspace_id as string, workspaceId);
      }
      return null;
    },
  });

  // Mirrors the row toggle's lock: a workspace row disabled at the user tier
  // cannot be flipped from this surface.
  const enableTargets = selected.filter(
    (s) => !s.enabled && s.disabled_scope !== 'user',
  );
  const disableTargets = selected.filter((s) => s.enabled);
  const deleteTargets = selected.filter(
    (s) => s.deletable && !isPluginOwned(s),
  );

  const actions: BulkAction[] = [
    {
      id: 'enable',
      label: t('plugins.bulk.enable', { count: enableTargets.length }),
      disabled: enableTargets.length === 0,
      run: () => surface.run(enableTargets.map((s) => target(s, true))),
    },
    {
      id: 'disable',
      label: t('plugins.bulk.disable', { count: disableTargets.length }),
      disabled: disableTargets.length === 0,
      run: () => surface.run(disableTargets.map((s) => target(s, false))),
    },
    {
      id: 'delete',
      label: t('plugins.bulk.delete', { count: deleteTargets.length }),
      destructive: true,
      disabled: deleteTargets.length === 0,
      confirmMessage: t('plugins.bulk.confirmDelete', { count: deleteTargets.length }),
      run: () =>
        surface.run(
          deleteTargets.map((s) => ({
            key: skillRowKey(s),
            run: () =>
              s.origin === 'workspace' && s.workspace_id
                ? deleteWorkspaceSkill(s.workspace_id, s.name)
                : deleteSkill(s.name),
          })),
        ),
    },
  ];

  return { actions, scope, count: selected.length };
}
