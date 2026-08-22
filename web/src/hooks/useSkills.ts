import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../lib/queryKeys';
import {
  deleteSkill,
  deleteWorkspaceSkill,
  getSkillContent,
  getSkills,
  moveSkill,
  setSkillCommand,
  setSkillEnabled,
  setWorkspaceSkillCommand,
  setWorkspaceSkillEnabled,
  uploadSkill,
  uploadWorkspaceSkill,
} from '../pages/ChatAgent/utils/api';

/**
 * React Query hooks for the merged skills tiers (platform + user +
 * workspace). Lives in hooks/ because chat-input's slash menu consumes the
 * list — the reason this is React Query at all: a mutation here must reach
 * that menu without a page reload. Every mutation invalidates the whole
 * `skills` prefix: a workspace change can alter the user view's shadowing
 * and vice versa, so per-scope invalidation would leave stale menus.
 */

export function useSkills(
  mode: string | null,
  opts: {
    includeDisabled?: boolean;
    workspaceId?: string | null;
    allScopes?: boolean;
  } = {},
) {
  const { includeDisabled = false, workspaceId = null, allScopes = false } = opts;
  return useQuery({
    queryKey: queryKeys.skills.list(mode, includeDisabled, workspaceId, allScopes),
    queryFn: () => getSkills({ mode, includeDisabled, workspaceId, allScopes }),
    staleTime: 60_000,
  });
}

/** Re-scope a skill (user tier ↔ one workspace). Whole-prefix invalidation:
 * a move changes the slash menu, the workspace views, and shadowing at once. */
/** One skill's SKILL.md text — powers the detail overlay's source preview. */
export function useSkillContent(
  name: string | null,
  workspaceId: string | null = null,
) {
  return useQuery({
    queryKey: queryKeys.skills.content(name ?? '', workspaceId),
    queryFn: () => getSkillContent(name!, workspaceId),
    enabled: !!name,
    staleTime: 60_000,
  });
}

export function useMoveSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      fromWorkspaceId,
      toWorkspaceId,
    }: {
      name: string;
      fromWorkspaceId: string | null;
      toWorkspaceId: string | null;
    }) => moveSkill(name, fromWorkspaceId, toWorkspaceId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}

/** Workspace-scoped toggle. The workspace id rides in the vars rather than a
 * hook argument because the all-scopes Plugins list mixes rows from many
 * workspaces in one list; a fixed-workspace page just passes the same id. */
export function useToggleWorkspaceSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      workspaceId,
      name,
      enabled,
    }: {
      workspaceId: string;
      name: string;
      enabled: boolean;
    }) => setWorkspaceSkillEnabled(workspaceId, name, enabled),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}

export function useDeleteWorkspaceSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ workspaceId, name }: { workspaceId: string; name: string }) =>
      deleteWorkspaceSkill(workspaceId, name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}

export function useUploadSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      file,
      onProgress,
    }: {
      file: File;
      onProgress?: (percent: number) => void;
    }) => uploadSkill(file, onProgress ?? null),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}

export function useToggleSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      setSkillEnabled(name, enabled),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}

/** Re-alias a skill's slash trigger in any scope; the workspace id in the
 * vars routes workspace rows to their own endpoint. Whole-prefix
 * invalidation so the slash menu picks the new trigger up immediately. */
export function useSetSkillCommand() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      name,
      command,
      workspaceId,
    }: {
      name: string;
      command: string | null;
      workspaceId?: string | null;
    }) =>
      workspaceId
        ? setWorkspaceSkillCommand(workspaceId, name, command)
        : setSkillCommand(name, command),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}

export function useDeleteSkill() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteSkill(name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}

export function useUploadWorkspaceSkill(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      file,
      onProgress,
    }: {
      file: File;
      onProgress?: (percent: number) => void;
    }) => uploadWorkspaceSkill(workspaceId, file, onProgress ?? null),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}
