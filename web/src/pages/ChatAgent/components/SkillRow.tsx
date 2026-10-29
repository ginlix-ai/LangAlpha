import { useTranslation } from 'react-i18next';
import { Trash2 } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import {
  EnabledToggle,
  KebabTrigger,
  ServerNameLine,
  ServerRowShell,
  TagBadge,
} from '@/components/mcp/McpPrimitives';
import { PluginOriginBadge, PluginSuppressedBadge } from '@/pages/Plugins/components/PluginBadges';
import { BrandMark } from './mcp/BrandMark';
import type { SkillInfo } from '../utils/api';

/**
 * One skill on the workspace Skills tab. Rows carry the toggle; a row the tab
 * can remove adds a menu. The `/command` chip is the skill's slash-menu
 * identity, shown where it differs from the name so the row and the menu
 * obviously name the same thing. Where a skill came from is the group
 * header's to say, so no origin badge restates it, but the plugin's name
 * stays: no header names which plugin. `disabled_scope: 'user'` marks a
 * disable this tab cannot undo (the toggle locks), and `shadows_inherited`
 * marks a workspace row overriding a same-named skill of the account.
 */
export function SkillRow({
  skill,
  toggling,
  onToggle,
  onDelete,
  onOpen,
}: {
  skill: SkillInfo;
  toggling: boolean;
  onToggle: (enabled: boolean) => void;
  onDelete?: () => void;
  /** Open this skill's detail view (name button + row-body click). */
  onOpen?: () => void;
}) {
  const { t } = useTranslation();
  const lockedByUserTier = skill.disabled_scope === 'user';
  return (
    <ServerRowShell
      testid={`skill-row-${skill.name}`}
      onOpen={onOpen}
      tile={<BrandMark name={skill.name} kind="skill" size="sm" />}
      main={
        <>
          <ServerNameLine name={skill.name} onOpen={onOpen}>
            {/* A command that only repeats the name is left off, as the
                Plugins page leaves it: on a phone it costs each row a line. */}
            {skill.command && skill.command !== skill.name && (
              <TagBadge>/{skill.command}</TagBadge>
            )}
            <PluginOriginBadge plugin={skill.plugin_name} />
            {skill.shadows_inherited && (
              <TagBadge soft>{t('plugins.skills.shadowsBadge')}</TagBadge>
            )}
            {lockedByUserTier && (
              <TagBadge soft>{t('plugins.skills.userDisabledBadge')}</TagBadge>
            )}
            <PluginSuppressedBadge row={skill} />
          </ServerNameLine>
          {skill.description && (
            <p
              className="text-[0.6875rem] line-clamp-2"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {skill.description}
            </p>
          )}
        </>
      }
      actions={
        <>
          <EnabledToggle
            enabled={skill.enabled}
            name={skill.name}
            disabled={toggling || lockedByUserTier}
            onToggle={() => onToggle(!skill.enabled)}
          />
          {skill.deletable && onDelete && (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <KebabTrigger aria-label={t('mcp.row.actionsAria', { name: skill.name })} />
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onSelect={onDelete} variant="destructive">
                  <Trash2 className="h-3.5 w-3.5 mr-2" />
                  {t('plugins.skillsPage.remove')}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </>
      }
    />
  );
}
