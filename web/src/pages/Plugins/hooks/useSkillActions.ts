import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import {
  useToggleSkill,
  useDeleteSkill,
  useMoveSkill,
  useSetSkillCommand,
  useToggleWorkspaceSkill,
  useDeleteWorkspaceSkill,
} from '@/hooks/useSkills';
import { formatApiErrorDetail } from '@/pages/ChatAgent/utils/api';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';

/**
 * Every single-row mutation on the Skills tab, with the busy keys the rows and
 * the detail overlay read.
 *
 * Each of these is fire-and-report: the tab has no inline error region, so a
 * rejected promise that isn't turned into a toast is a silent failure the user
 * reads as success. Holding them together is what keeps that treatment uniform.
 */

/**
 * Scope-qualified row key: a workspace row may share its name with the
 * inherited user-tier row it shadows, and only the mutated row should read busy.
 */
export const skillRowKey = (skill: SkillInfo) =>
  `${skill.workspace_id ?? ''}:${skill.name}`;

export interface SkillDeleteRequest {
  name: string;
  workspaceId: string | null;
}

export function useSkillActions() {
  const { t } = useTranslation();
  const toggleMutation = useToggleSkill();
  const deleteMutation = useDeleteSkill();
  const moveMutation = useMoveSkill();
  const commandMutation = useSetSkillCommand();
  const wsToggleMutation = useToggleWorkspaceSkill();
  const wsDeleteMutation = useDeleteWorkspaceSkill();

  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [movingName, setMovingName] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<SkillDeleteRequest | null>(null);

  function failed(titleKey: string, err: unknown) {
    toast({
      variant: 'destructive',
      title: t(titleKey),
      description: formatApiErrorDetail(err),
    });
  }

  async function toggle(skill: SkillInfo, enabled: boolean) {
    setTogglingName(skillRowKey(skill));
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
      failed('plugins.skills.toggleFailed', err);
    } finally {
      setTogglingName(null);
    }
  }

  async function setWorkspaceDisabled(
    skill: SkillInfo,
    workspaceId: string,
    disabled: boolean,
  ) {
    setTogglingName(skillRowKey(skill));
    try {
      await wsToggleMutation.mutateAsync({
        workspaceId,
        name: skill.name,
        enabled: !disabled,
      });
    } catch (err) {
      failed('plugins.skills.toggleFailed', err);
    } finally {
      setTogglingName(null);
    }
  }

  async function saveCommand(skill: SkillInfo, command: string | null) {
    setTogglingName(skillRowKey(skill));
    try {
      await commandMutation.mutateAsync({
        name: skill.name,
        command,
        workspaceId: skill.workspace_id ?? null,
      });
    } catch (err) {
      failed('plugins.skills.commandFailed', err);
    } finally {
      setTogglingName(null);
    }
  }

  async function move(skill: SkillInfo, toWorkspaceId: string | null) {
    setMovingName(skillRowKey(skill));
    try {
      await moveMutation.mutateAsync({
        name: skill.name,
        fromWorkspaceId: skill.workspace_id ?? null,
        toWorkspaceId,
      });
    } catch (err) {
      failed('plugins.scope.moveFailed', err);
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
      failed('plugins.skills.deleteFailed', err);
    }
  }

  return {
    togglingName,
    movingName,
    deleting,
    deletePending: deleteMutation.isPending || wsDeleteMutation.isPending,
    requestDelete: (name: string, workspaceId: string | null) =>
      setDeleting({ name, workspaceId }),
    cancelDelete: () => setDeleting(null),
    confirmDelete,
    toggle,
    setWorkspaceDisabled,
    saveCommand,
    move,
  };
}
