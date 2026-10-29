import { useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from '@/lib/framer';
import { Upload } from 'lucide-react';
import { toast } from '@/components/ui/use-toast';
import { usePlugins } from '@/hooks/usePlugins';
import {
  useDeleteWorkspaceSkill,
  useSkills,
  useToggleWorkspaceSkill,
  useUploadWorkspaceSkill,
} from '@/hooks/useSkills';
import { LoadFailed } from '@/pages/Plugins/components/LoadFailed';
import { SkillUploadModal } from '@/pages/Plugins/components/SkillUploadModal';
import { detailHref } from '@/pages/Plugins/utils/detailParam';
import { skillScopeId, skillSections } from '@/pages/Plugins/utils/provenance';
import {
  ConfirmStrip,
  HeaderButton,
  ListEmpty,
  ListSkeleton,
  SectionHeader,
} from '@/components/mcp/McpPrimitives';
import { formatApiErrorDetail } from '../utils/api';
import type { SkillInfo } from '../utils/api';
import { SkillRow } from './SkillRow';

/**
 * Sandbox settings → Skills tab: this workspace's own skills (upload, toggle,
 * remove; they shadow same-named account skills here) above the inherited
 * ones, grouped as the Plugins page groups them: added by you, one group per
 * plugin, built in. On those the toggle records a workspace-level disable. A skill
 * disabled at the account level renders locked — that disable is not
 * workspace-reversible, mirroring the MCP builtin rule. A row opens the
 * skill's page on Plugins, where everything this panel cannot change lives.
 */

export function SkillsTab({ workspaceId }: { workspaceId: string }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { data: skills, isLoading, error, refetch } = useSkills({
    workspaceId,
    includeDisabled: true,
  });
  const uploadMutation = useUploadWorkspaceSkill(workspaceId);
  const toggleMutation = useToggleWorkspaceSkill();
  const deleteMutation = useDeleteWorkspaceSkill();
  const { data: pluginData } = usePlugins();

  const [uploadOpen, setUploadOpen] = useState(false);
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [deletingName, setDeletingName] = useState<string | null>(null);

  const sections = skillSections(skills ?? [], pluginData?.plugins);
  const workspaceSkills = sections.workspace;
  const inheritedGroups = [
    { key: 'yours', title: t('plugins.skillsPage.addedByYou'), rows: sections.own },
    ...sections.plugins.map((group) => ({ key: `plugin:${group.name}`, title: group.name, rows: group.rows })),
    { key: 'builtIn', title: t('plugins.skillsPage.builtIn'), rows: sections.builtIn },
  ].filter((group) => group.rows.length > 0);

  function openSkill(skill: SkillInfo) {
    // The Plugins page resolves the ref against its every-scope list, where
    // only a workspace's own row carries a workspace id.
    navigate(
      detailHref({
        kind: 'skill',
        name: skill.name,
        workspaceId: skillScopeId(skill),
      }),
    );
  }

  async function handleToggle(skill: SkillInfo, enabled: boolean) {
    // Origin-qualified: a workspace skill may shadow an inherited one with
    // the same name, and only the toggled row should read busy.
    setTogglingName(`${skill.origin}:${skill.name}`);
    try {
      await toggleMutation.mutateAsync({ workspaceId, name: skill.name, enabled });
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

  async function confirmDelete() {
    if (!deletingName) return;
    try {
      await deleteMutation.mutateAsync({ workspaceId, name: deletingName });
      setDeletingName(null);
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
      <LoadFailed what={t('plugins.loadFailed.skills')} error={error} onRetry={() => void refetch()} />
    );
  }
  if (isLoading) return <ListSkeleton />;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <SectionHeader>{t('plugins.skills.workspaceSection')}</SectionHeader>
          <HeaderButton variant="primary" icon={Upload} onClick={() => setUploadOpen(true)}>
            {t('plugins.skills.upload')}
          </HeaderButton>
        </div>
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.skills.workspaceHint')}
        </p>
        {workspaceSkills.length === 0 ? (
          <ListEmpty>{t('plugins.skills.workspaceEmpty')}</ListEmpty>
        ) : (
          <AnimatePresence initial={false}>
            {workspaceSkills.map((skill) => (
              <SkillRow
                key={skill.name}
                skill={skill}
                toggling={togglingName === `${skill.origin}:${skill.name}`}
                onToggle={(enabled) => handleToggle(skill, enabled)}
                onDelete={() => setDeletingName(skill.name)}
                onOpen={() => openSkill(skill)}
              />
            ))}
          </AnimatePresence>
        )}
        {/* Under the list it acts on: only this workspace's own skills can be
            removed, and at the foot of the panel it opened below some twenty
            inherited rows, out of sight of the click that asked for it. */}
        {deletingName && (
          <ConfirmStrip
            message={t('plugins.skills.removeHereConfirm', { skill: deletingName })}
            confirmLabel={
              deleteMutation.isPending ? t('common.loading') : t('plugins.skillsPage.removeYes')
            }
            cancelLabel={t('plugins.skills.deleteConfirmNo')}
            pending={deleteMutation.isPending}
            onConfirm={confirmDelete}
            onCancel={() => setDeletingName(null)}
          />
        )}
      </div>

      {inheritedGroups.length > 0 && (
        <div className="flex flex-col gap-1.5">
          <SectionHeader>{t('plugins.skills.inheritedSection')}</SectionHeader>
          <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('plugins.skills.inheritedHint')}
          </p>
          {inheritedGroups.map((group) => (
            <section key={group.key} className="flex flex-col mt-1.5">
              <h4
                className="flex items-baseline gap-1.5 text-xs font-medium"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {group.title}
                <span
                  className="tabular-nums font-normal"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {group.rows.length}
                </span>
              </h4>
              <AnimatePresence initial={false}>
                {group.rows.map((skill) => (
                  <SkillRow
                    key={`${skill.origin}-${skill.name}`}
                    skill={skill}
                    toggling={togglingName === `${skill.origin}:${skill.name}`}
                    onToggle={(enabled) => handleToggle(skill, enabled)}
                    onOpen={() => openSkill(skill)}
                  />
                ))}
              </AnimatePresence>
            </section>
          ))}
        </div>
      )}

      <AnimatePresence>
        {uploadOpen && (
          <SkillUploadModal
            workspaceId={workspaceId}
            onClose={() => setUploadOpen(false)}
            onUpload={(file, onProgress) => uploadMutation.mutateAsync({ file, onProgress })}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
