import { useState, type ComponentType, type CSSProperties, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Blocks, BookOpen, Folder, Upload } from 'lucide-react';
import { useSkills, useUploadSkill } from '@/hooks/useSkills';
import { invalidateSkillFanout } from '@/hooks/usePlugins';
import {
  ConfirmStrip,
  HeaderButton,
  ListEmpty,
  ListError,
  ListSkeleton,
  SectionHeader,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { groupBy, matchesFilter } from '../utils/groupOrigins';
import { withDetail } from '../utils/detailParam';
import { useAddIntent } from '../hooks/useAddIntent';
import { useDetailParam } from '../hooks/useDetailParam';
import { usePluginListSurface } from '../hooks/usePluginListSurface';
import { useSkillActions, skillRowKey } from '../hooks/useSkillActions';
import { useSkillBulkActions } from '../hooks/useSkillBulkActions';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { BulkActionBar } from './BulkActionBar';
import { EmptyState } from './EmptyState';
import { GroupDeck } from './GroupDeck';
import { ListControls } from './ListControls';
import { PluginSuppressedBadge } from './PluginBadges';
import { ScopeControl, scopeLocked } from './ScopeControl';
import { SkillDetail } from './SkillDetail';
import { SkillRow } from './SkillRow';
import { SkillUploadModal } from './SkillUploadModal';
import { rowSelection } from './useBulkSelection';
import {
  isEffectivelyEnabled,
  isPluginOwned,
  isPluginSuppressed,
} from '../utils/provenance';

/**
 * The Plugins → Skills tab, in the all-scopes inventory shape: platform
 * skills, the user's own uploads, one deck per plugin's skills, then one
 * deck per workspace. Origin groups past a few rows stack into decks; the
 * management list asks for disabled rows too — the slash menu elsewhere
 * reads the enabled-only default, so a row toggled off here disappears
 * there, not here.
 */

export function SkillsList() {
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const { data: skills, isLoading, error } = useSkills(null, {
    includeDisabled: true,
    allScopes: true,
  });
  const uploadMutation = useUploadSkill();
  const [uploadOpen, setUploadOpen] = useState(false);

  // Bulk delete already excludes plugin-owned rows and the rest of the actions
  // are enable/disable/scope, so no bulk run here can change plugin identity.
  const surface = usePluginListSurface({ invalidate: invalidateSkillFanout });
  const { selection } = surface;
  const { workspaces: wsOptions, nameById: wsNameById } = useWorkspaceOptions();
  const actions = useSkillActions();

  useAddIntent({ skill: () => setUploadOpen(true) });

  const allSkills = skills ?? [];
  const visible = allSkills.filter(
    (s) =>
      matchesFilter(surface.filter, s.name, s.description, s.plugin_name) &&
      surface.matchesState(s.enabled, isPluginSuppressed(s)),
  );

  // Grouped by the package that owns the row, not by which tier it lives in:
  // a shipped bundle and an installed plugin are the same claim on a skill,
  // and splitting them put four bundles' worth of rows under one anonymous
  // "Platform skills" heading. What is left there is a skill no package
  // claims — an operator's own skills root, and nothing in a stock build.
  const platformSkills = visible.filter(
    (s) => s.origin === 'platform' && !s.plugin_name,
  );
  const ownSkills = visible.filter((s) => s.origin === 'user' && !s.plugin_name);
  const pluginSections = [
    ...groupBy(
      visible.filter((s) => s.origin !== 'workspace' && s.plugin_name),
      (s) => s.plugin_name as string,
    ).entries(),
  ].sort(([a], [b]) => a.localeCompare(b));
  const workspaceSections = [
    ...groupBy(
      visible.filter((s) => s.origin === 'workspace'),
      (s) => s.workspace_id ?? '',
    ).entries(),
  ].sort(([a], [b]) => (wsNameById.get(a) ?? '').localeCompare(wsNameById.get(b) ?? ''));

  // --- Detail overlay (?detail=skill:NAME [&dws=wsid]) ---
  // Names are unique within a scope; `dws` picks the workspace-tier row when
  // one shadows a same-named user skill.
  const detail = useDetailParam<SkillInfo>(
    'skill',
    (ref) =>
      allSkills.find(
        (s) => s.name === ref.name && (s.workspace_id ?? null) === ref.workspaceId,
      ) ?? null,
    !isLoading && skills !== undefined,
  );
  const detailSkill = detail.target;

  const bulk = useSkillBulkActions(visible, surface, wsOptions);

  function renderRow(
    skill: SkillInfo,
    scopeControl: ReactNode,
    onDelete?: () => void,
    inDeck = false,
  ) {
    return (
      <SkillRow
        key={skillRowKey(skill)}
        skill={skill}
        toggling={actions.togglingName === skillRowKey(skill)}
        onToggle={(enabled) => actions.toggle(skill, enabled)}
        onCommandSave={(command) => actions.saveCommand(skill, command)}
        onDelete={onDelete}
        onOpen={() => detail.open(skill.name, skill.workspace_id ?? null)}
        inDeck={inDeck}
        scopeControl={scopeControl}
        selection={rowSelection(selection, skillRowKey(skill))}
      />
    );
  }

  /** One origin deck: same shell everywhere, only the identity and the row's
   *  scope affordances differ. */
  function renderDeck({
    id,
    title,
    icon,
    rows,
    scopeControl,
    deleteOf,
    badge,
    action,
  }: {
    id: string;
    title: string;
    icon: ComponentType<{ className?: string; style?: CSSProperties }>;
    rows: SkillInfo[];
    scopeControl: (skill: SkillInfo) => ReactNode;
    deleteOf?: (skill: SkillInfo) => (() => void) | undefined;
    badge?: ReactNode;
    action?: ReactNode;
  }) {
    return (
      <GroupDeck
        key={id}
        id={id}
        title={title}
        icon={icon}
        count={rows.length}
        enabledCount={rows.filter(isEffectivelyEnabled).length}
        badge={badge}
        action={action}
        forceExpanded={surface.forceExpanded}
        selection={selection}
        selectionKeys={rows.map(skillRowKey)}
      >
        <AnimatePresence initial={false}>
          {rows.map((skill) =>
            renderRow(skill, scopeControl(skill), deleteOf?.(skill), true),
          )}
        </AnimatePresence>
      </GroupDeck>
    );
  }

  /** Open the plugin's own card. The deck already knows which one it is, so
   *  the detail ref carries that name rather than landing on the bare list.
   *  The overlay open here belongs to this tab, so it goes; every other param
   *  travels. */
  function openPluginDetail(name: string) {
    const next = withDetail(searchParams, {
      kind: 'plugin',
      name,
      workspaceId: null,
    });
    next.set('tab', 'plugins');
    setSearchParams(next, { replace: true });
  }

  const openPluginAction = (name: string) => (
    <button
      type="button"
      title={t('plugins.groups.openPlugin')}
      aria-label={t('plugins.groups.openPlugin')}
      onClick={() => openPluginDetail(name)}
      className="p-1 rounded transition-colors hover:bg-foreground/10"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      <Blocks className="h-3.5 w-3.5" />
    </button>
  );

  if (error) {
    return (
      <ListError>
        {(error as { message?: string })?.message || t('mcp.list.loadFailed')}
      </ListError>
    );
  }
  if (isLoading) return <ListSkeleton />;

  return (
    <div className="flex flex-col gap-3">
      <ListControls
        filter={surface.filter}
        onFilterChange={surface.setFilter}
        stateFilter={surface.stateFilter}
        onStateFilterChange={surface.setStateFilter}
        showAttention
        selecting={selection.selecting}
        onStartSelect={selection.start}
        selectDisabled={allSkills.length === 0}
      />

      {surface.noMatches(visible.length) && (
        <ListEmpty>{t('plugins.filter.noMatches')}</ListEmpty>
      )}

      {platformSkills.length > 0 &&
        renderDeck({
          id: 'skills:platform',
          title: t('plugins.skills.platform'),
          icon: BookOpen,
          rows: platformSkills,
          scopeControl: (skill) => (
            <ScopeControl
              workspaces={wsOptions}
              scopeWorkspaceId={null}
              disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
              checklistLocked={scopeLocked(skill)}
              busy={actions.togglingName === skillRowKey(skill)}
              onSetWorkspaceDisabled={(wsId, disabled) =>
                actions.setWorkspaceDisabled(skill, wsId, disabled)
              }
            />
          ),
        })}

      {/* Filtered-empty hides the whole section: the top-level noMatches
          notice already covers it, and a bare header reads as a glitch. */}
      {surface.keepsSection(ownSkills.length) && (
      <div className="flex flex-col [&>*+*]:mt-1.5">
        <SectionHeader>{t('plugins.skills.yours')}</SectionHeader>
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.skills.inheritHint')}
        </p>
        {ownSkills.length === 0 ? (
          <EmptyState
            message={t('plugins.skills.empty')}
            action={
              <HeaderButton
                variant="primary"
                icon={Upload}
                onClick={() => setUploadOpen(true)}
              >
                {t('plugins.skills.upload')}
              </HeaderButton>
            }
          />
        ) : (
          <AnimatePresence initial={false}>
            {ownSkills.map((skill) =>
              renderRow(
                skill,
                <ScopeControl
                  workspaces={wsOptions}
                  scopeWorkspaceId={null}
                  disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
                  checklistLocked={scopeLocked(skill)}
                  busy={
                    actions.togglingName === skillRowKey(skill) ||
                    actions.movingName === skillRowKey(skill)
                  }
                  onSetWorkspaceDisabled={(wsId, disabled) =>
                    actions.setWorkspaceDisabled(skill, wsId, disabled)
                  }
                  onMove={(toWorkspaceId) => actions.move(skill, toWorkspaceId)}
                />,
                () => actions.requestDelete(skill.name, null),
              ),
            )}
          </AnimatePresence>
        )}
      </div>
      )}

      {pluginSections.map(([pluginName, rows]) =>
        renderDeck({
          id: `skills:plugin:${pluginName}`,
          title: pluginName,
          icon: Blocks,
          rows,
          badge: <PluginSuppressedBadge row={rows[0]} />,
          action: openPluginAction(pluginName),
          scopeControl: (skill) => (
            <ScopeControl
              workspaces={wsOptions}
              scopeWorkspaceId={null}
              disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
              checklistLocked={scopeLocked(skill)}
              busy={actions.togglingName === skillRowKey(skill)}
              moveBlockedReason={
                // A package's skills live at the account tier — moving one
                // into a workspace would detach it from its owner as a side
                // effect of a scope change.
                t('plugins.scope.movePluginBlocked', { plugin: pluginName })
              }
              onSetWorkspaceDisabled={(wsId, disabled) =>
                actions.setWorkspaceDisabled(skill, wsId, disabled)
              }
            />
          ),
          // `deletable` means "managed through this surface", not "unowned" —
          // a plugin's skill is listed here and comes back deletable. Same
          // gate as the bulk bar, which refuses them.
          deleteOf: (skill) =>
            skill.deletable && !isPluginOwned(skill)
              ? () => actions.requestDelete(skill.name, null)
              : undefined,
        }),
      )}

      {workspaceSections.map(([wsId, wsSkills]) =>
        renderDeck({
          id: `skills:ws:${wsId}`,
          title: t('plugins.scope.inWorkspace', {
            name: wsNameById.get(wsId) ?? t('plugins.scope.unknownWorkspace'),
          }),
          icon: Folder,
          rows: wsSkills,
          scopeControl: (skill) => (
            <ScopeControl
              workspaces={wsOptions}
              scopeWorkspaceId={wsId}
              busy={actions.movingName === skillRowKey(skill)}
              moveToAllBlockedReason={
                // move_user_skill 409s on the known destination collision, so
                // don't advertise a move that is guaranteed to fail for a
                // shadowing row.
                skill.shadows_inherited ? t('plugins.scope.moveShadowBlocked') : null
              }
              onMove={(toWorkspaceId) => actions.move(skill, toWorkspaceId)}
            />
          ),
          deleteOf: (skill) => () => actions.requestDelete(skill.name, wsId),
        }),
      )}

      {actions.deleting && (
        <ConfirmStrip
          message={t('plugins.skills.deleteConfirm', { skill: actions.deleting.name })}
          confirmLabel={
            actions.deletePending
              ? t('common.loading')
              : t('plugins.skills.deleteConfirmYes')
          }
          cancelLabel={t('plugins.skills.deleteConfirmNo')}
          pending={actions.deletePending}
          onConfirm={actions.confirmDelete}
          onCancel={actions.cancelDelete}
        />
      )}

      {selection.selecting && (
        <BulkActionBar
          count={bulk.count}
          actions={bulk.actions}
          scope={bulk.scope}
          progress={surface.progress}
          onExit={selection.exit}
        />
      )}

      {uploadOpen && (
        <SkillUploadModal
          onClose={() => setUploadOpen(false)}
          onUpload={(file, onProgress) => uploadMutation.mutateAsync({ file, onProgress })}
        />
      )}

      <AnimatePresence>
        {detailSkill && (
          <SkillDetail
            key={skillRowKey(detailSkill)}
            skill={detailSkill}
            onClose={detail.close}
            toggling={actions.togglingName === skillRowKey(detailSkill)}
            onToggle={(enabled) => actions.toggle(detailSkill, enabled)}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
