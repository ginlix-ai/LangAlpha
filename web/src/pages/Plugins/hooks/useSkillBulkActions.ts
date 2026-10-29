import { useTranslation } from 'react-i18next';
import {
  deleteSkill,
  deleteWorkspaceSkill,
  moveSkill,
  setSkillEnabled,
  setWorkspaceSkillEnabled,
} from '@/pages/ChatAgent/utils/api';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import type { BulkScopeSpec } from '../components/BulkScopeMenu';
import { scopeLocked, type ScopeWorkspace } from '../components/ScopeControl';
import { buildBulkActions, type BulkActions } from '../utils/bulkActions';
import { isEffectivelyEnabled, isPluginOwned, isPluginSuppressed } from '../utils/provenance';
import { skillRowKey } from '../utils/rowKeys';
import type { PluginListSurface } from './usePluginListSurface';
import { useScopeBulk } from './useScopeBulk';

/** The select-mode actions for the Skills tab: enable, disable, delete, scope. */
export function useSkillBulkActions(
  /** The rows on screen, not the whole account: see useBulkSelection. */
  visibleSkills: readonly SkillInfo[],
  surface: PluginListSurface,
  workspaces: ScopeWorkspace[],
): BulkActions & { scope: BulkScopeSpec } {
  const { t } = useTranslation();
  const homeId = useHomeWorkspaceId() ?? null;
  const selected = visibleSkills.filter((s) =>
    surface.selection.selected.has(skillRowKey(s)),
  );
  // A workspace row goes to its workspace's endpoint, everything else to the account's.
  const workspaceOf = (s: SkillInfo) => (s.origin === 'workspace' ? s.workspace_id : null);

  // Same eligibility as each row's own ScopeControl: the deny-list checklist
  // exists on user-tier rows it does not lock; tier moves exist for the user's own
  // uploads and workspace rows (plugin skills stay put, a shadowing workspace
  // row can't surface to a tier where its name is taken).
  const scope = useScopeBulk<SkillInfo>(selected, {
    workspaces,
    run: (targets) => surface.run(targets, { verb: 'scope', noun: 'skill' }),
    key: skillRowKey,
    name: (s) => s.name,
    denyMarkers: (s) =>
      s.origin !== 'workspace' && !scopeLocked(s) ? (s.disabled_workspace_ids ?? []) : null,
    // Home, which every deny checklist lists.
    homeWorkspaceId: () => homeId,
    setWorkspaceEnabled: (s, workspaceId, enabled) =>
      setWorkspaceSkillEnabled(workspaceId, s.name, enabled),
    promote: (s) =>
      s.origin === 'workspace' && s.workspace_id && !s.shadows_inherited
        ? () => moveSkill(s.name, s.workspace_id as string, null)
        : null,
    move: {
      movable: (s) =>
        (s.origin === 'user' && !isPluginOwned(s)) ||
        (s.origin === 'workspace' && !!s.workspace_id),
      to: (s, workspaceId) => {
        if (s.origin === 'user' && !isPluginOwned(s)) {
          return () => moveSkill(s.name, null, workspaceId);
        }
        if (s.origin === 'workspace' && s.workspace_id && s.workspace_id !== workspaceId) {
          return () => moveSkill(s.name, s.workspace_id as string, workspaceId);
        }
        return null;
      },
    },
  });

  return {
    ...buildBulkActions(t, surface, selected, {
      noun: 'skill',
      key: skillRowKey,
      name: (s) => s.name,
      // Decided on the state the pills show, not on the row's own flag.
      isOn: isEffectivelyEnabled,
      held: isPluginSuppressed,
      // The row toggle's lock: a workspace row disabled at the user tier
      // cannot be flipped from this surface.
      enableLocked: (s) => s.disabled_scope === 'user',
      setEnabled: (s, on) => {
        const workspaceId = workspaceOf(s);
        return workspaceId
          ? setWorkspaceSkillEnabled(workspaceId, s.name, on)
          : setSkillEnabled(s.name, on);
      },
      // The server's rule, as the cell menu reads it: a plugin's skill goes
      // with its plugin, and a built-in one is switched off instead.
      remove: {
        verb: 'remove',
        labelKey: 'plugins.bulk.remove',
        confirmKey: 'plugins.bulk.confirmRemoveSkills',
        eligible: (s) => s.deletable,
        run: (s) => {
          const workspaceId = workspaceOf(s);
          return workspaceId ? deleteWorkspaceSkill(workspaceId, s.name) : deleteSkill(s.name);
        },
      },
      keptKey: 'plugins.bulk.keptBundled',
    }),
    scope,
  };
}
