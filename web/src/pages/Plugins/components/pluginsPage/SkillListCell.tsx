import { useTranslation } from 'react-i18next';
import { Layers, Pencil, Power, Trash2 } from 'lucide-react';
import { ConfirmStrip } from '@/components/mcp/McpPrimitives';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { isPluginOwned, isPluginSuppressed, skillScopeId } from '../../utils/provenance';
import { useWorkspaceOptions } from '../../hooks/useWorkspaceOptions';
import { CellMenu, CellState } from '../surface/Surface';
import { SkillCell } from './componentCells';

/**
 * A skill in Skills → Yours. No switch on the cell: on/off, the workspaces it
 * reaches, its command and removal live in its menu, and its page carries the
 * full controls. Only a skill the user added can be removed; one a plugin
 * brought goes with the plugin, and a built-in one is switched off instead.
 */
export function SkillListCell({
  skill,
  meta,
  toggling,
  confirmingRemove,
  removing,
  onOpen,
  onToggle,
  onEditCommand,
  onRemove,
  onConfirmRemove,
  onCancelRemove,
}: {
  skill: SkillInfo;
  /** "3 of 5 workspaces", or the one workspace it lives in. */
  meta?: string | null;
  toggling: boolean;
  confirmingRemove: boolean;
  removing: boolean;
  onOpen: () => void;
  onToggle: (enabled: boolean) => void;
  onEditCommand: () => void;
  /** Absent: this skill cannot be removed here. */
  onRemove?: () => void;
  onConfirmRemove: () => void;
  onCancelRemove: () => void;
}) {
  const { t } = useTranslation();
  // A disable recorded at the account tier cannot be undone from a workspace
  // view, so the switch holds there.
  const lockedByUserTier = skill.disabled_scope === 'user';

  if (confirmingRemove) {
    return (
      <div className="pp-confirm">
        <RemoveConfirm
          skill={skill}
          removing={removing}
          onConfirm={onConfirmRemove}
          onCancel={onCancelRemove}
        />
      </div>
    );
  }

  return (
    <SkillCell
      name={skill.name}
      skill={skill}
      meta={meta}
      trailing={
        isPluginSuppressed(skill) ? (
          <CellState state="off" label={t('plugins.skillsPage.pluginOff')} />
        ) : undefined
      }
      onOpen={onOpen}
      menu={
        <CellMenu label={t('mcp.row.actionsAria', { name: skill.name })}>
          <DropdownMenuItem
            disabled={toggling || lockedByUserTier}
            onSelect={() => onToggle(!skill.enabled)}
          >
            <Power className="h-3.5 w-3.5" />
            {skill.enabled ? t('plugins.pluginsPage.switchOff') : t('plugins.pluginsPage.switchOn')}
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={onOpen}>
            <Layers className="h-3.5 w-3.5" />
            {t('plugins.skillsPage.workspaces')}
          </DropdownMenuItem>
          {skill.command && (
            <DropdownMenuItem onSelect={onEditCommand}>
              <Pencil className="h-3.5 w-3.5" />
              {t('plugins.skillsPage.editCommand')}
            </DropdownMenuItem>
          )}
          <DropdownMenuSeparator />
          {onRemove ? (
            <DropdownMenuItem variant="destructive" onSelect={onRemove}>
              <Trash2 className="h-3.5 w-3.5" />
              {t('plugins.skillsPage.remove')}
            </DropdownMenuItem>
          ) : (
            <p className="pp-menu-note">
              {isPluginOwned(skill) && skill.origin !== 'platform'
                ? t('plugins.skillsPage.pluginNote', { plugin: skill.plugin_name })
                : t('plugins.skillsPage.builtinNote')}
            </p>
          )}
        </CellMenu>
      }
    />
  );
}

/**
 * The menu's "Remove", asked in the same words the skill's page asks it. A
 * workspace's own skill is that workspace's loss alone, so the question names
 * the workspace rather than promising every one of them stops.
 */
function RemoveConfirm({
  skill,
  removing,
  onConfirm,
  onCancel,
}: {
  skill: SkillInfo;
  removing: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const { nameById } = useWorkspaceOptions();
  const workspaceId = skillScopeId(skill);
  const message = workspaceId
    ? t('plugins.skillsPage.removeConfirmIn', {
        skill: skill.name,
        workspace: nameById.get(workspaceId) ?? t('plugins.skillsPage.thisWorkspace'),
      })
    : t('plugins.skillsPage.removeConfirm', { skill: skill.name });
  return (
    <ConfirmStrip
      message={message}
      confirmLabel={removing ? t('common.loading') : t('plugins.skillsPage.removeYes')}
      cancelLabel={t('plugins.skills.deleteConfirmNo')}
      pending={removing}
      onConfirm={onConfirm}
      onCancel={onCancel}
    />
  );
}
