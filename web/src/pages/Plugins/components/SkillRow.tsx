import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { BookOpen, Trash2 } from 'lucide-react';
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
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';

/**
 * One skill on the shared row shell. Platform rows carry only the account-wide
 * disable toggle; user rows add delete. The `/command` chip is the skill's
 * slash-menu identity, shown so the row and the menu obviously name the same
 * thing; where the surface passes `onCommandSave` it is click-to-edit.
 * In workspace views, `disabled_scope: 'user'` marks a disable this
 * surface cannot undo (the toggle locks), and `shadows_inherited` marks a
 * workspace row overriding a same-named user skill.
 */

function CommandChip({
  command,
  onSave,
  saving,
}: {
  command: string;
  onSave?: (command: string | null) => void;
  saving?: boolean;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState('');

  if (!onSave) return <TagBadge>/{command}</TagBadge>;

  if (!editing) {
    return (
      <button
        type="button"
        onClick={() => {
          setValue(command);
          setEditing(true);
        }}
        disabled={saving}
        title={t('plugins.skills.editCommandHint')}
        className="cursor-pointer disabled:cursor-default"
      >
        <TagBadge>/{command}</TagBadge>
      </button>
    );
  }
  return (
    <input
      autoFocus
      value={value}
      onChange={(e) => setValue(e.target.value)}
      onKeyDown={(e) => {
        if (e.key === 'Enter') {
          const next = value.trim().replace(/^\/+/, '').trim();
          setEditing(false);
          if (next !== command) onSave(next || null);
        } else if (e.key === 'Escape') {
          setEditing(false);
        }
      }}
      onBlur={() => setEditing(false)}
      spellCheck={false}
      aria-label={t('plugins.skills.commandInputAria')}
      className="text-[0.6875rem] px-1.5 py-0.5 rounded w-28 outline-none"
      style={{
        color: 'var(--color-text-secondary)',
        backgroundColor: 'var(--color-bg-input)',
        border: '1px solid var(--color-border-muted)',
      }}
    />
  );
}

export function SkillRow({
  skill,
  toggling,
  onToggle,
  onDelete,
  onCommandSave,
  scopeControl,
  selection,
}: {
  skill: SkillInfo;
  toggling: boolean;
  onToggle: (enabled: boolean) => void;
  onDelete?: () => void;
  /** Present = the `/command` chip becomes click-to-edit (Enter saves, Esc
   * cancels, empty clears back to the name). Absent = read-only chip. */
  onCommandSave?: (command: string | null) => void;
  scopeControl?: React.ReactNode;
  /** ServerRowShell select-mode props, spread through untouched. */
  selection?: { selecting?: boolean; selected?: boolean; onSelectToggle?: () => void };
}) {
  const { t } = useTranslation();
  const lockedByUserTier = skill.disabled_scope === 'user';
  return (
    <ServerRowShell
      testid={`skill-row-${skill.name}`}
      {...(selection ?? {})}
      main={
        <>
          <ServerNameLine icon={BookOpen} name={skill.name}>
            {skill.command && (
              <CommandChip
                command={skill.command}
                onSave={onCommandSave}
                saving={toggling}
              />
            )}
            {skill.origin === 'platform' && (
              <TagBadge soft>{t('plugins.skills.platformBadge')}</TagBadge>
            )}
            {skill.plugin_name && (
              <TagBadge
                soft
                title={t('plugins.component.fromPlugin', {
                  plugin: skill.plugin_name,
                })}
              >
                {skill.plugin_name}
              </TagBadge>
            )}
            {skill.shadows_inherited && (
              <TagBadge soft>{t('plugins.skills.shadowsBadge')}</TagBadge>
            )}
            {lockedByUserTier && (
              <TagBadge soft>{t('plugins.skills.userDisabledBadge')}</TagBadge>
            )}
            {skill.plugin_name && skill.plugin_enabled === false && (
              <TagBadge
                soft
                title={t('plugins.component.suppressed', {
                  plugin: skill.plugin_name,
                })}
              >
                {t('plugins.component.suppressedBadge')}
              </TagBadge>
            )}
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
          {scopeControl}
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
                  {t('mcp.row.delete')}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </>
      }
    />
  );
}
