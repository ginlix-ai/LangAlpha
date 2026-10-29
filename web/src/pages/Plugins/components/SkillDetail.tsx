import { useEffect, useId, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Check, Pencil, Trash2, X } from 'lucide-react';
import { ConfirmStrip } from '@/components/mcp/McpPrimitives';
import { useSkillContent } from '@/hooks/useSkills';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes, mediumDate } from '@/lib/format';
import type { PluginInfo, SkillInfo } from '@/pages/ChatAgent/utils/api';
import { isPluginOwned, isPluginSuppressed, skillScopeId } from '../utils/provenance';
import type { useSkillActions } from '../hooks/useSkillActions';
import { skillRowKey } from '../utils/rowKeys';
import {
  DetailAsideBlock,
  DetailFacts,
  DetailHeader,
  DetailPage,
} from './DetailPage';
import { scopeOff } from '../utils/scopeOff';
import { ScopeControl, type ScopeWorkspace } from './ScopeControl';
import { MetaLine, OnOffControl, PageButton } from './pluginsPage/parts';
import { sourceLabel } from './pluginsPage/pluginStatus';
import { useCatalogPlugins } from '../hooks/useCatalogPlugins';
import { useHomeOption } from '../hooks/useWorkspaceOptions';
import { SkillDocument } from './pluginsPage/SkillDocument';
import { SkillFiles } from './pluginsPage/SkillFiles';

/**
 * A skill's page. Its tile is the skill kind's glyph, never a vendor's: a
 * skill is its instructions, so the SKILL.md sits at the center, rendered,
 * with the raw file a click away. Beside it, the files that come with it
 * (references, scripts), the command that runs it, the workspaces it reaches,
 * and where it came from.
 *
 * The switch stays live while the skill's plugin is off. A suppressed row
 * still owns its own `enabled`, and that flag decides whether it comes back
 * with the plugin, so the switch is the one way to leave a single skill out
 * before switching its plugin back on.
 */

export function SkillDetail({
  skill,
  onClose,
  actions,
  workspaces,
  workspacesLoading,
  workspaceName,
  plugin,
  startEditingCommand = false,
  onOpenPlugin,
}: {
  skill: SkillInfo;
  onClose: () => void;
  actions: ReturnType<typeof useSkillActions>;
  workspaces: ScopeWorkspace[];
  workspacesLoading: boolean;
  /** The one workspace a workspace-tier skill lives in. */
  workspaceName: string | null;
  /** The package that brought it, when one did. */
  plugin: PluginInfo | null;
  /** Opened from "Edit command": the command box starts open. */
  startEditingCommand?: boolean;
  onOpenPlugin: (name: string) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const catalogPlugins = useCatalogPlugins();
  const home = useHomeOption();
  const labelId = useId();
  const [file, setFile] = useState('SKILL.md');
  // SKILL.md carries the folder's listing; the open file is fetched on its own.
  const listingQuery = useSkillContent(skill.name, skill.workspace_id ?? null);
  const contentQuery = useSkillContent(skill.name, skill.workspace_id ?? null, file);
  const files = listingQuery.data?.files ?? [];
  const key = skillRowKey(skill);
  const toggling = actions.togglingName === key;
  const lockedByUserTier = skill.disabled_scope === 'user';
  const wsId = skillScopeId(skill);
  // A workspace's own skill runs nowhere else, so removing it is that
  // workspace's loss alone; the account-tier copy said every workspace.
  const hereName = workspaceName ?? t('plugins.skillsPage.thisWorkspace');
  const pluginOwned = isPluginOwned(skill);
  const pluginName = skill.plugin_name;
  const confirmingRemove =
    !!actions.deleting && actions.deleting.name === skill.name && actions.deleting.workspaceId === wsId;

  const source =
    skill.origin === 'workspace'
      ? t('plugins.skillsPage.oneWorkspace')
      : plugin
        ? sourceLabel(t, plugin, catalogPlugins)
        : skill.origin === 'platform'
          ? t('plugins.skillsPage.builtIn')
          : pluginOwned
            ? t('plugins.skillsPage.fromCatalog')
            : t('plugins.skillsPage.addedByYou');

  const scope =
    skill.origin === 'workspace' && wsId ? (
      <ScopeControl
        workspaces={workspaces}
        loading={workspacesLoading}
        scopeWorkspaceId={wsId}
        busy={actions.movingName === key}
        moveToAllBlockedReason={
          // The move 409s on the known destination collision, so a shadowing
          // row is not offered a move that is certain to fail.
          skill.shadows_inherited ? t('plugins.scope.moveShadowBlocked') : null
        }
        onMove={(to) => void actions.move(skill, to)}
      />
    ) : (
      <ScopeControl
        workspaces={workspaces}
        loading={workspacesLoading}
        scopeWorkspaceId={null}
        disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
        off={scopeOff(skill)}
        homeWorkspace={home}
        busy={toggling || actions.movingName === key}
        moveBlockedReason={
          // A package's skills live at the account tier; moving one into a
          // workspace would detach it from its owner as a side effect.
          pluginOwned ? t('plugins.scope.movePluginBlocked', { plugin: skill.plugin_name }) : null
        }
        onSetWorkspaceDisabled={(id, disabled) => void actions.setWorkspaceDisabled(skill, id, disabled)}
        onMove={
          skill.origin === 'user' && !pluginOwned ? (to) => void actions.move(skill, to) : undefined
        }
      />
    );

  const pluginFact =
    pluginName && plugin ? (
      <button
        type="button"
        className="pp-note-link pp-fact-button"
        onClick={() => onOpenPlugin(pluginName)}
      >
        {skill.plugin_name}
      </button>
    ) : (
      (skill.plugin_name ?? null)
    );

  const footer = skill.deletable ? (
    confirmingRemove ? (
      <ConfirmStrip
        message={
          wsId
            ? t('plugins.skillsPage.removeConfirmIn', { skill: skill.name, workspace: hereName })
            : t('plugins.skillsPage.removeConfirm', { skill: skill.name })
        }
        confirmLabel={actions.deletePending ? t('common.loading') : t('plugins.skillsPage.removeYes')}
        cancelLabel={t('plugins.skills.deleteConfirmNo')}
        pending={actions.deletePending}
        onConfirm={() => void actions.confirmDelete()}
        onCancel={actions.cancelDelete}
      />
    ) : (
      <div className="pp-verbs">
        <PageButton
          variant="danger"
          icon={<Trash2 />}
          onClick={() => actions.requestDelete(skill.name, wsId)}
        >
          {t('plugins.skillsPage.removeNamed', { skill: skill.name })}
        </PageButton>
        <p className="pp-note">
          {wsId
            ? t('plugins.skillsPage.removeNoteIn', { workspace: hereName })
            : t('plugins.skillsPage.removeNote')}
        </p>
      </div>
    )
  ) : (
    <p className="pp-note">
      {pluginOwned && skill.origin !== 'platform'
        ? t('plugins.skillsPage.pluginNote', { plugin: skill.plugin_name })
        : t('plugins.skillsPage.builtinNote')}
    </p>
  );

  const metaParts = [
    pluginName && plugin ? (
      <span key="plugin">
        <Trans
          i18nKey="plugins.skillsPage.fromPlugin"
          values={{ plugin: skill.plugin_name }}
          // Not `link`: the parser reads that as the void HTML element and
          // drops the name it wraps, leaving an empty button.
          components={{
            open: (
              <button
                type="button"
                className="pp-note-link pp-meta-link"
                onClick={() => onOpenPlugin(pluginName)}
              />
            ),
          }}
        />
      </span>
    ) : skill.plugin_name ? (
      <span key="plugin">{skill.plugin_name}</span>
    ) : null,
    <span key="source">
      {/* A bare workspace name reads as a tag; this says it runs nowhere else. */}
      {skill.origin === 'workspace' && workspaceName
        ? t('plugins.skillsPage.workspaceNote', { name: workspaceName })
        : source}
    </span>,
    isPluginSuppressed(skill) ? <span key="off">{t('plugins.skillsPage.pluginOff')}</span> : null,
    lockedByUserTier ? <span key="locked">{t('plugins.skills.userDisabledBadge')}</span> : null,
  ];

  return (
    <DetailPage
      labelId={labelId}
      onClose={onClose}
      crumb={skill.name}
      header={
        <DetailHeader
          name={skill.name}
          labelId={labelId}
          kind="skill"
          meta={<MetaLine parts={metaParts} />}
          description={skill.description || undefined}
          controls={
            <OnOffControl
              enabled={skill.enabled}
              name={skill.name}
              disabled={toggling || lockedByUserTier}
              onToggle={() => void actions.toggle(skill, !skill.enabled)}
            />
          }
        />
      }
      aside={
        <>
          {files.length > 1 && (
            <DetailAsideBlock title={t('plugins.skillsPage.files')}>
              <SkillFiles files={files} selected={file} onSelect={setFile} />
            </DetailAsideBlock>
          )}
          <DetailAsideBlock title={t('plugins.skillsPage.command')}>
            {skill.command ? (
              <CommandBox
                // A save lands as a new command; the box closes on it.
                key={skill.command}
                command={skill.command}
                startEditing={startEditingCommand}
                saving={toggling}
                onSave={(next) => void actions.saveCommand(skill, next)}
              />
            ) : (
              <p className="pp-note">{t('plugins.skillsPage.noCommand')}</p>
            )}
          </DetailAsideBlock>
          <DetailAsideBlock title={t('plugins.skillsPage.workspacesTitle')}>{scope}</DetailAsideBlock>
          <DetailAsideBlock title={t('plugins.detail.info')}>
            <DetailFacts
              items={[
                [t('plugins.skillsPage.plugin'), pluginFact],
                [t('plugins.detail.sourceLabel'), source],
                [t('plugins.detail.author'), plugin?.author ?? null],
                [t('plugins.detail.version'), plugin?.version ?? null],
                [t('plugins.detail.tools'), skill.tools.length > 0 ? skill.tools.join(', ') : null],
                [t('plugins.detail.size'), skill.size_bytes > 0 ? formatBytes(skill.size_bytes, locale) : null],
                [t('plugins.detail.updated'), skill.updated_at ? mediumDate(new Date(skill.updated_at), locale) : null],
              ]}
            />
          </DetailAsideBlock>
        </>
      }
      footer={footer}
    >
      <SkillDocument
        key={file}
        path={file}
        content={contentQuery.data?.content}
        size={contentQuery.data?.size}
        loading={contentQuery.isLoading}
        failed={contentQuery.isError}
      />
    </DetailPage>
  );
}

/** The command that runs the skill, edited in place: Enter saves, Esc
 *  cancels, and an empty box resets it to the skill's name. */
function CommandBox({
  command,
  startEditing,
  saving,
  onSave,
}: {
  command: string;
  startEditing: boolean;
  saving: boolean;
  onSave: (command: string | null) => void;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(startEditing);
  const [value, setValue] = useState(command);
  const inputRef = useRef<HTMLInputElement>(null);

  // Opened from a cell's menu, the menu hands focus back to its trigger as it
  // closes, after this box mounted; take it back once that has happened.
  useEffect(() => {
    if (!startEditing) return;
    const id = window.setTimeout(() => inputRef.current?.focus(), 60);
    return () => window.clearTimeout(id);
  }, [startEditing]);

  function save() {
    const next = value.trim().replace(/^\/+/, '').trim();
    setEditing(false);
    if (next !== command) onSave(next || null);
  }

  function cancel() {
    setValue(command);
    setEditing(false);
  }

  if (!editing) {
    return (
      <>
        <div className="pp-cmdbox">
          <span>/{command}</span>
          <button
            type="button"
            onClick={() => setEditing(true)}
            disabled={saving}
            aria-label={t('plugins.skillsPage.commandEdit')}
            title={t('plugins.skillsPage.commandEdit')}
          >
            <Pencil className="h-3.5 w-3.5" />
          </button>
        </div>
        <p className="pp-note">{t('plugins.skillsPage.commandNote')}</p>
      </>
    );
  }
  return (
    <>
      <div className="pp-cmdbox owns-its-edge">
        <span aria-hidden className="pp-cmd-slash">/</span>
        <input
          ref={inputRef}
          autoFocus
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') save();
            else if (e.key === 'Escape') {
              e.stopPropagation();
              cancel();
            }
          }}
          spellCheck={false}
          aria-label={t('plugins.skills.commandInputAria')}
        />
        <button type="button" onClick={save} aria-label={t('common.save')} title={t('common.save')}>
          <Check className="h-3.5 w-3.5" />
        </button>
        <button type="button" onClick={cancel} aria-label={t('common.cancel')} title={t('common.cancel')}>
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
      <p className="pp-note">{t('plugins.skillsPage.commandResetNote')}</p>
    </>
  );
}
