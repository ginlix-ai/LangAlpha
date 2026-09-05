import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Upload } from 'lucide-react';
import { toast } from '@/components/ui/use-toast';
import {
  useDeleteWorkspaceSkill,
  useSkills,
  useToggleWorkspaceSkill,
  useUploadWorkspaceSkill,
} from '@/hooks/useSkills';
import { SkillRow } from '@/pages/Plugins/components/SkillRow';
import { SkillUploadModal } from '@/pages/Plugins/components/SkillUploadModal';
import {
  ConfirmStrip,
  HeaderButton,
  ListEmpty,
  ListError,
  ListSkeleton,
  SectionHeader,
} from './mcp/McpPrimitives';
import { formatApiErrorDetail } from '../utils/api';
import type { SkillInfo } from '../utils/api';

/**
 * Sandbox settings → Skills tab: this workspace's own skills (upload, toggle,
 * delete; they shadow same-named user skills here) above the inherited tiers
 * (platform + user), where the toggle records a workspace-level disable. A
 * skill disabled at the user level renders locked — that disable is not
 * workspace-reversible, mirroring the MCP builtin rule.
 */

export function SkillsTab({ workspaceId }: { workspaceId: string }) {
  const { t } = useTranslation();
  const { data: skills, isLoading, error } = useSkills(null, {
    workspaceId,
    includeDisabled: true,
  });
  const uploadMutation = useUploadWorkspaceSkill(workspaceId);
  const toggleMutation = useToggleWorkspaceSkill();
  const deleteMutation = useDeleteWorkspaceSkill();

  const [uploadOpen, setUploadOpen] = useState(false);
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [deletingName, setDeletingName] = useState<string | null>(null);

  const workspaceSkills = (skills ?? []).filter((s) => s.origin === 'workspace');
  const inheritedSkills = (skills ?? []).filter((s) => s.origin !== 'workspace');

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
      <ListError>
        {(error as { message?: string })?.message || t('mcp.list.loadFailed')}
      </ListError>
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
              />
            ))}
          </AnimatePresence>
        )}
      </div>

      {inheritedSkills.length > 0 && (
        <div className="flex flex-col gap-1.5">
          <SectionHeader>{t('plugins.skills.inheritedSection')}</SectionHeader>
          <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('plugins.skills.inheritedHint')}
          </p>
          <AnimatePresence initial={false}>
            {inheritedSkills.map((skill) => (
              <SkillRow
                key={`${skill.origin}-${skill.name}`}
                skill={skill}
                toggling={togglingName === `${skill.origin}:${skill.name}`}
                onToggle={(enabled) => handleToggle(skill, enabled)}
              />
            ))}
          </AnimatePresence>
        </div>
      )}

      {deletingName && (
        <ConfirmStrip
          message={t('plugins.skills.deleteConfirm', { skill: deletingName })}
          confirmLabel={
            deleteMutation.isPending ? t('common.loading') : t('plugins.skills.deleteConfirmYes')
          }
          cancelLabel={t('plugins.skills.deleteConfirmNo')}
          pending={deleteMutation.isPending}
          onConfirm={confirmDelete}
          onCancel={() => setDeletingName(null)}
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
