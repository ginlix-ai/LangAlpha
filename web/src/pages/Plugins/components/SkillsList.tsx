import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Upload } from 'lucide-react';
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
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { formatApiErrorDetail } from '@/pages/ChatAgent/utils/api';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { ScopeControl } from './ScopeControl';
import type { ScopeWorkspace } from './ScopeControl';
import { SkillRow } from './SkillRow';
import { SkillUploadModal } from './SkillUploadModal';

/**
 * The Plugins → Skills tab, in the all-scopes inventory shape: platform skills
 * (account-wide disable + per-workspace deny-list) above the user's uploads
 * (same, plus delete and tier moves), then one section per workspace holding
 * skills scoped there. The management list asks for disabled rows too — the
 * slash menu elsewhere reads the enabled-only default, so a row toggled off
 * here disappears there, not here.
 */

export function SkillsList() {
  const { t } = useTranslation();
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
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [movingName, setMovingName] = useState<string | null>(null);
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

  const platformSkills = (skills ?? []).filter((s) => s.origin === 'platform');
  const userSkills = (skills ?? []).filter((s) => s.origin === 'user');
  const workspaceSkills = (skills ?? []).filter((s) => s.origin === 'workspace');
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
      {platformSkills.length > 0 && (
        <div className="flex flex-col gap-1.5">
          <SectionHeader>{t('plugins.skills.platform')}</SectionHeader>
          <AnimatePresence initial={false}>
            {platformSkills.map((skill) => (
              <SkillRow
                key={skill.name}
                skill={skill}
                toggling={togglingName === rowKey(skill)}
                onToggle={(enabled) => handleToggle(skill, enabled)}
                onCommandSave={(command) => handleCommandSave(skill, command)}
                scopeControl={
                  <ScopeControl
                    workspaces={wsOptions}
                    scopeWorkspaceId={null}
                    disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
                    checklistLocked={!skill.enabled}
                    busy={togglingName === rowKey(skill)}
                    onSetWorkspaceDisabled={(wsId, disabled) =>
                      handleSetWorkspaceDisabled(skill, wsId, disabled)
                    }
                  />
                }
              />
            ))}
          </AnimatePresence>
        </div>
      )}

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
        {userSkills.length === 0 ? (
          <ListEmpty>{t('plugins.skills.empty')}</ListEmpty>
        ) : (
          <AnimatePresence initial={false}>
            {userSkills.map((skill) => (
              <SkillRow
                key={skill.name}
                skill={skill}
                toggling={togglingName === rowKey(skill)}
                onToggle={(enabled) => handleToggle(skill, enabled)}
                onCommandSave={(command) => handleCommandSave(skill, command)}
                onDelete={() => setDeleting({ name: skill.name, workspaceId: null })}
                scopeControl={
                  <ScopeControl
                    workspaces={wsOptions}
                    scopeWorkspaceId={null}
                    disabledWorkspaceIds={skill.disabled_workspace_ids ?? []}
                    checklistLocked={!skill.enabled}
                    busy={togglingName === rowKey(skill) || movingName === rowKey(skill)}
                    moveBlockedReason={
                      // Plugin-installed skills live at the account tier
                      // (moving one into a workspace would detach it as a
                      // side effect of a scope change).
                      skill.plugin_name
                        ? t('plugins.scope.movePluginBlocked', {
                            plugin: skill.plugin_name,
                          })
                        : null
                    }
                    onSetWorkspaceDisabled={(wsId, disabled) =>
                      handleSetWorkspaceDisabled(skill, wsId, disabled)
                    }
                    onMove={(toWorkspaceId) => handleMove(skill, toWorkspaceId)}
                  />
                }
              />
            ))}
          </AnimatePresence>
        )}
      </div>

      {workspaceSections.map(([wsId, wsSkills]) => (
        <div key={wsId} className="flex flex-col gap-1.5">
          <SectionHeader>
            {t('plugins.scope.inWorkspace', {
              name: wsNameById.get(wsId) ?? t('plugins.scope.unknownWorkspace'),
            })}
          </SectionHeader>
          <AnimatePresence initial={false}>
            {wsSkills.map((skill) => (
              <SkillRow
                key={`${wsId}:${skill.name}`}
                skill={skill}
                toggling={togglingName === rowKey(skill)}
                onToggle={(enabled) => handleToggle(skill, enabled)}
                onCommandSave={(command) => handleCommandSave(skill, command)}
                onDelete={() => setDeleting({ name: skill.name, workspaceId: wsId })}
                scopeControl={
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
                  />
                }
              />
            ))}
          </AnimatePresence>
        </div>
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

      {uploadOpen && (
        <SkillUploadModal
          onClose={() => setUploadOpen(false)}
          onUpload={(file, onProgress) => uploadMutation.mutateAsync({ file, onProgress })}
        />
      )}
    </div>
  );
}
