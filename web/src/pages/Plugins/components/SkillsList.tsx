import { useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Blocks, BookOpen, Folder, Upload } from 'lucide-react';
import { toast } from '@/components/ui/use-toast';
import {
  useSkills,
  useUploadSkill,
  useToggleSkill,
  useDeleteSkill,
  useMoveSkill,
  useSetSkillCommand,
  useToggleWorkspaceSkill,
  useDeleteWorkspaceSkill,
} from '@/hooks/useSkills';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import {
  ConfirmStrip,
  HeaderButton,
  ListEmpty,
  ListError,
  ListSkeleton,
  SectionHeader,
  TagBadge,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import {
  deleteSkill,
  deleteWorkspaceSkill,
  formatApiErrorDetail,
  setSkillEnabled,
  setWorkspaceSkillEnabled,
} from '@/pages/ChatAgent/utils/api';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { matchesFilter } from '../utils/groupOrigins';
import { BulkActionBar, type BulkAction } from './BulkActionBar';
import { GroupDeck } from './GroupDeck';
import { ListControls } from './ListControls';
import { ScopeControl } from './ScopeControl';
import type { ScopeWorkspace } from './ScopeControl';
import { SkillRow } from './SkillRow';
import { SkillUploadModal } from './SkillUploadModal';
import {
  rowSelection,
  useBulkRunner,
  useBulkSelection,
  type BulkTarget,
} from './useBulkSelection';

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
  const [, setSearchParams] = useSearchParams();
  const { data: skills, isLoading, error } = useSkills(null, {
    includeDisabled: true,
    allScopes: true,
  });
  const { data: wsData } = useWorkspaces({ limit: 100 });
  const uploadMutation = useUploadSkill();
  const toggleMutation = useToggleSkill();
  const deleteMutation = useDeleteSkill();
  const moveMutation = useMoveSkill();
  const commandMutation = useSetSkillCommand();
  const wsToggleMutation = useToggleWorkspaceSkill();
  const wsDeleteMutation = useDeleteWorkspaceSkill();

  const [uploadOpen, setUploadOpen] = useState(false);
  const [filter, setFilter] = useState('');
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [movingName, setMovingName] = useState<string | null>(null);
  const selection = useBulkSelection();
  const { progress, run } = useBulkRunner(selection);
  // Scope-qualified: a workspace row may share its name with the inherited
  // user-tier row it shadows, and only the mutated row should read busy.
  const rowKey = (s: SkillInfo) => `${s.workspace_id ?? ''}:${s.name}`;
  const [deleting, setDeleting] = useState<{
    name: string;
    workspaceId: string | null;
  } | null>(null);

  const workspaces = (
    (wsData as { workspaces?: { workspace_id: string; name?: string }[] })
      ?.workspaces ?? []
  );
  const wsOptions: ScopeWorkspace[] = workspaces.map((w) => ({
    id: w.workspace_id,
    name: w.name || t('plugins.scope.unknownWorkspace'),
  }));
  const wsNameById = new Map(wsOptions.map((w) => [w.id, w.name]));

  const allSkills = skills ?? [];
  const visible = allSkills.filter((s) =>
    matchesFilter(filter, s.name, s.description, s.plugin_name),
  );
  const forceExpanded = selection.selecting || !!filter.trim();

  const platformSkills = visible.filter((s) => s.origin === 'platform');
  const ownSkills = visible.filter((s) => s.origin === 'user' && !s.plugin_name);
  const pluginSkills = visible.filter((s) => s.origin === 'user' && s.plugin_name);
  const byPlugin = new Map<string, SkillInfo[]>();
  for (const s of pluginSkills) {
    const plugin = s.plugin_name!;
    byPlugin.set(plugin, [...(byPlugin.get(plugin) ?? []), s]);
  }
  const pluginSections = [...byPlugin.entries()].sort(([a], [b]) => a.localeCompare(b));

  const workspaceSkills = visible.filter((s) => s.origin === 'workspace');
  const byWorkspace = new Map<string, SkillInfo[]>();
  for (const s of workspaceSkills) {
    const wsId = s.workspace_id ?? '';
    byWorkspace.set(wsId, [...(byWorkspace.get(wsId) ?? []), s]);
  }
  const workspaceSections = [...byWorkspace.entries()].sort(([a], [b]) =>
    (wsNameById.get(a) ?? '').localeCompare(wsNameById.get(b) ?? ''),
  );

  async function handleToggle(skill: SkillInfo, enabled: boolean) {
    setTogglingName(rowKey(skill));
    try {
      if (skill.origin === 'workspace' && skill.workspace_id) {
        await wsToggleMutation.mutateAsync({
          workspaceId: skill.workspace_id,
          name: skill.name,
          enabled,
        });
      } else {
        await toggleMutation.mutateAsync({ name: skill.name, enabled });
      }
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.skills.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setTogglingName(null);
    }
  }

  async function handleSetWorkspaceDisabled(
    skill: SkillInfo,
    workspaceId: string,
    disabled: boolean,
  ) {
    setTogglingName(rowKey(skill));
    try {
      await wsToggleMutation.mutateAsync({
        workspaceId,
        name: skill.name,
        enabled: !disabled,
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.skills.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setTogglingName(null);
    }
  }

  async function handleCommandSave(skill: SkillInfo, command: string | null) {
    setTogglingName(rowKey(skill));
    try {
      await commandMutation.mutateAsync({
        name: skill.name,
        command,
        workspaceId: skill.workspace_id ?? null,
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.skills.commandFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setTogglingName(null);
    }
  }

  async function handleMove(skill: SkillInfo, toWorkspaceId: string | null) {
    setMovingName(rowKey(skill));
    try {
      await moveMutation.mutateAsync({
        name: skill.name,
        fromWorkspaceId: skill.workspace_id ?? null,
        toWorkspaceId,
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.scope.moveFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setMovingName(null);
    }
  }

  async function confirmDelete() {
    if (!deleting) return;
    try {
      if (deleting.workspaceId) {
        await wsDeleteMutation.mutateAsync({
          workspaceId: deleting.workspaceId,
          name: deleting.name,
        });
      } else {
        await deleteMutation.mutateAsync(deleting.name);
      }
      setDeleting(null);
    } catch (err) {
      // The strip stays up on failure, to retry or cancel.
      toast({
        variant: 'destructive',
        title: t('plugins.skills.deleteFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  function skillTarget(skill: SkillInfo, enabled: boolean): BulkTarget {
    return {
      key: rowKey(skill),
      run: () =>
        skill.origin === 'workspace' && skill.workspace_id
          ? setWorkspaceSkillEnabled(skill.workspace_id, skill.name, enabled)
          : setSkillEnabled(skill.name, enabled),
    };
  }

  const selectedSkills = allSkills.filter((s) => selection.selected.has(rowKey(s)));
  // Mirrors the row toggle's lock: a workspace row disabled at the user tier
  // cannot be flipped from this surface.
  const enableTargets = selectedSkills.filter(
    (s) => !s.enabled && s.disabled_scope !== 'user',
  );
  const disableTargets = selectedSkills.filter((s) => s.enabled);
  const deleteTargets = selectedSkills.filter((s) => s.deletable && !s.plugin_id);
  const actions: BulkAction[] = [
    {
      id: 'enable',
      label: t('plugins.bulk.enable', { count: enableTargets.length }),
      disabled: enableTargets.length === 0,
      run: () => run(enableTargets.map((s) => skillTarget(s, true))),
    },
    {
      id: 'disable',
      label: t('plugins.bulk.disable', { count: disableTargets.length }),
      disabled: disableTargets.length === 0,
      run: () => run(disableTargets.map((s) => skillTarget(s, false))),
    },
    {
      id: 'delete',
      label: t('plugins.bulk.delete', { count: deleteTargets.length }),
      destructive: true,
      disabled: deleteTargets.length === 0,
      confirmMessage: t('plugins.bulk.confirmDelete', { count: deleteTargets.length }),
      run: () =>
        run(
          deleteTargets.map((s) => ({
            key: rowKey(s),
            run: () =>
              s.origin === 'workspace' && s.workspace_id
                ? deleteWorkspaceSkill(s.workspace_id, s.name)
                : deleteSkill(s.name),
          })),
        ),
    },
  ];

  function renderRow(skill: SkillInfo, scopeControl: ReactNode, onDelete?: () => void) {
    return (
      <SkillRow
        key={rowKey(skill)}
        skill={skill}
        toggling={togglingName === rowKey(skill)}
        onToggle={(enabled) => handleToggle(skill, enabled)}
        onCommandSave={(command) => handleCommandSave(skill, command)}
        onDelete={onDelete}
        scopeControl={scopeControl}
        selection={rowSelection(selection, rowKey(skill))}
      />
    );
  }

  if (error) {
    return (
      <ListError>
        {(error as { message?: string })?.message || t('mcp.list.loadFailed')}
      </ListError>
    );
  }
  if (isLoading) return <ListSkeleton />;

  const deletePending = deleteMutation.isPending || wsDeleteMutation.isPending;

  return (
    <div className="flex flex-col gap-3">
      <ListControls
        filter={filter}
        onFilterChange={setFilter}
        selecting={selection.selecting}
        onStartSelect={selection.start}
        selectDisabled={allSkills.length === 0}
      />

      {filter.trim() && visible.length === 0 && (
        <ListEmpty>{t('plugins.filter.noMatches')}</ListEmpty>
      )}

      <GroupDeck
        id="skills:platform"
        title={t('plugins.skills.platform')}
        icon={BookOpen}
        count={platformSkills.length}
        enabledCount={platformSkills.filter((s) => s.enabled).length}
        forceExpanded={forceExpanded}
        selection={selection}
        selectionKeys={platformSkills.map(rowKey)}
      >
        <AnimatePresence initial={false}>
          {platformSkills.map((skill) =>
            renderRow(
              skill,
              <ScopeControl
                workspaces={wsOptions}
                scopeWorkspaceId={null}
                disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
                checklistLocked={!skill.enabled}
                busy={togglingName === rowKey(skill)}
                onSetWorkspaceDisabled={(wsId, disabled) =>
                  handleSetWorkspaceDisabled(skill, wsId, disabled)
                }
              />,
            ),
          )}
        </AnimatePresence>
      </GroupDeck>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <SectionHeader>{t('plugins.skills.yours')}</SectionHeader>
          <HeaderButton variant="primary" icon={Upload} onClick={() => setUploadOpen(true)}>
            {t('plugins.skills.upload')}
          </HeaderButton>
        </div>
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.skills.inheritHint')}
        </p>
        {ownSkills.length === 0 ? (
          !filter.trim() && <ListEmpty>{t('plugins.skills.empty')}</ListEmpty>
        ) : (
          <AnimatePresence initial={false}>
            {ownSkills.map((skill) =>
              renderRow(
                skill,
                <ScopeControl
                  workspaces={wsOptions}
                  scopeWorkspaceId={null}
                  disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
                  checklistLocked={!skill.enabled}
                  busy={togglingName === rowKey(skill) || movingName === rowKey(skill)}
                  onSetWorkspaceDisabled={(wsId, disabled) =>
                    handleSetWorkspaceDisabled(skill, wsId, disabled)
                  }
                  onMove={(toWorkspaceId) => handleMove(skill, toWorkspaceId)}
                />,
                () => setDeleting({ name: skill.name, workspaceId: null }),
              ),
            )}
          </AnimatePresence>
        )}
      </div>

      {pluginSections.map(([pluginName, rows]) => (
        <GroupDeck
          key={pluginName}
          id={`skills:plugin:${pluginName}`}
          title={pluginName}
          icon={Blocks}
          count={rows.length}
          enabledCount={rows.filter((s) => s.enabled).length}
          badge={
            rows[0]?.plugin_enabled === false ? (
              <TagBadge soft title={t('plugins.component.suppressed', { plugin: pluginName })}>
                {t('plugins.component.suppressedBadge')}
              </TagBadge>
            ) : undefined
          }
          action={
            <button
              type="button"
              title={t('plugins.groups.openPlugin')}
              aria-label={t('plugins.groups.openPlugin')}
              onClick={() => setSearchParams({ tab: 'plugins' }, { replace: true })}
              className="p-1 rounded transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <Blocks className="h-3.5 w-3.5" />
            </button>
          }
          forceExpanded={forceExpanded}
          selection={selection}
          selectionKeys={rows.map(rowKey)}
        >
          <AnimatePresence initial={false}>
            {rows.map((skill) =>
              renderRow(
                skill,
                <ScopeControl
                  workspaces={wsOptions}
                  scopeWorkspaceId={null}
                  disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
                  checklistLocked={!skill.enabled}
                  busy={togglingName === rowKey(skill)}
                  moveBlockedReason={
                    // Plugin-installed skills live at the account tier
                    // (moving one into a workspace would detach it as a
                    // side effect of a scope change).
                    t('plugins.scope.movePluginBlocked', { plugin: pluginName })
                  }
                  onSetWorkspaceDisabled={(wsId, disabled) =>
                    handleSetWorkspaceDisabled(skill, wsId, disabled)
                  }
                />,
                skill.deletable
                  ? () => setDeleting({ name: skill.name, workspaceId: null })
                  : undefined,
              ),
            )}
          </AnimatePresence>
        </GroupDeck>
      ))}

      {workspaceSections.map(([wsId, wsSkills]) => (
        <GroupDeck
          key={wsId}
          id={`skills:ws:${wsId}`}
          title={t('plugins.scope.inWorkspace', {
            name: wsNameById.get(wsId) ?? t('plugins.scope.unknownWorkspace'),
          })}
          icon={Folder}
          count={wsSkills.length}
          enabledCount={wsSkills.filter((s) => s.enabled).length}
          forceExpanded={forceExpanded}
          selection={selection}
          selectionKeys={wsSkills.map(rowKey)}
        >
          <AnimatePresence initial={false}>
            {wsSkills.map((skill) =>
              renderRow(
                skill,
                <ScopeControl
                  workspaces={wsOptions}
                  scopeWorkspaceId={wsId}
                  busy={movingName === rowKey(skill)}
                  moveToAllBlockedReason={
                    // move_user_skill 409s on the known destination
                    // collision, so don't advertise a move that is
                    // guaranteed to fail for a shadowing row.
                    skill.shadows_inherited
                      ? t('plugins.scope.moveShadowBlocked')
                      : null
                  }
                  onMove={(toWorkspaceId) => handleMove(skill, toWorkspaceId)}
                />,
                () => setDeleting({ name: skill.name, workspaceId: wsId }),
              ),
            )}
          </AnimatePresence>
        </GroupDeck>
      ))}

      {deleting && (
        <ConfirmStrip
          message={t('plugins.skills.deleteConfirm', { skill: deleting.name })}
          confirmLabel={
            deletePending ? t('common.loading') : t('plugins.skills.deleteConfirmYes')
          }
          cancelLabel={t('plugins.skills.deleteConfirmNo')}
          pending={deletePending}
          onConfirm={confirmDelete}
          onCancel={() => setDeleting(null)}
        />
      )}

      {selection.selecting && (
        <BulkActionBar
          count={selection.selected.size}
          actions={actions}
          progress={progress}
          onExit={selection.exit}
        />
      )}

      {uploadOpen && (
        <SkillUploadModal
          onClose={() => setUploadOpen(false)}
          onUpload={(file, onProgress) => uploadMutation.mutateAsync({ file, onProgress })}
        />
      )}
    </div>
  );
}
