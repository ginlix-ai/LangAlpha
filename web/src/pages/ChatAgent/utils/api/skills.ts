/**
 * Skills API — the merged platform + user + workspace tiers.
 *
 * The default (enabled-only) list feeds the slash-command menu; the
 * management view passes `includeDisabled` to also render disabled rows.
 * A `workspaceId` selects the workspace-effective view (workspace rows
 * shadow same-named user skills there); the workspace CRUD lives under
 * /api/v1/workspaces/{id}/skills, mirroring workspace MCP servers.
 */
import { api } from '@/api/client';

export interface SkillInfo {
  name: string;
  description: string;
  tool_count: number;
  tools: string[];
  command: string | null;
  origin: 'platform' | 'user' | 'workspace';
  enabled: boolean;
  editable: boolean;
  deletable: boolean;
  confirmed: boolean;
  plugin_id: string | null;
  /** Set when the skill was installed by an Agent Plugins package. */
  plugin_name?: string | null;
  /** The owning plugin's enabled state; false = suppressed everywhere. */
  plugin_enabled?: boolean | null;
  size_bytes: number;
  updated_at: string | null;
  disabled_scope: 'user' | 'workspace' | null;
  shadows_inherited: boolean;
  /** The scope a workspace-tier row belongs to (null = user/platform tier). */
  workspace_id?: string | null;
  /** Workspaces where an all-workspaces skill is switched off (deny-list);
   * populated in the all-scopes view only. */
  disabled_workspace_ids?: string[];
}

function uploadConfig(onProgress: ((percent: number) => void) | null) {
  return {
    headers: { 'Content-Type': 'multipart/form-data' },
    onUploadProgress: onProgress
      ? (e: { loaded: number; total?: number }) => {
          if (e.total) onProgress(Math.round((e.loaded / e.total) * 100));
        }
      : undefined,
  };
}

export async function getSkills(opts?: {
  mode?: string | null;
  includeDisabled?: boolean;
  workspaceId?: string | null;
  allScopes?: boolean;
}): Promise<SkillInfo[]> {
  const params: Record<string, string | boolean> = {};
  if (opts?.mode) params.mode = opts.mode;
  if (opts?.includeDisabled) params.include_disabled = true;
  if (opts?.workspaceId) params.workspace_id = opts.workspaceId;
  if (opts?.allScopes) params.all_scopes = true;
  const { data } = await api.get('/api/v1/skills', { params });
  return data.skills || [];
}

/** Re-scope a skill row: user tier (every workspace) ↔ one workspace. Both
 * scopes are explicit because names are only unique within one scope. */
export async function moveSkill(
  name: string,
  fromWorkspaceId: string | null,
  toWorkspaceId: string | null,
): Promise<SkillInfo> {
  const { data } = await api.post<SkillInfo>(
    `/api/v1/skills/${encodeURIComponent(name)}/move`,
    { from_workspace_id: fromWorkspaceId, to_workspace_id: toWorkspaceId },
  );
  return data;
}

export async function uploadSkill(
  file: File,
  onProgress: ((percent: number) => void) | null = null,
): Promise<SkillInfo> {
  const formData = new FormData();
  formData.append('file', file);
  const { data } = await api.post<SkillInfo>(
    '/api/v1/skills',
    formData,
    uploadConfig(onProgress),
  );
  return data;
}

/** Toggles either tier by name: user rows flip in place, builtin names write
 * the per-user disable. */
export async function setSkillEnabled(name: string, enabled: boolean): Promise<SkillInfo> {
  const { data } = await api.patch<SkillInfo>(
    `/api/v1/skills/${encodeURIComponent(name)}`,
    { enabled },
  );
  return data;
}

/** Re-alias a skill's slash trigger; null clears back to the name. Same
 * tier dispatch as the enabled toggle (builtin names write a preference). */
export async function setSkillCommand(
  name: string,
  command: string | null,
): Promise<SkillInfo> {
  const { data } = await api.patch<SkillInfo>(
    `/api/v1/skills/${encodeURIComponent(name)}`,
    { command },
  );
  return data;
}

export async function deleteSkill(name: string): Promise<void> {
  await api.delete(`/api/v1/skills/${encodeURIComponent(name)}`);
}

export async function uploadWorkspaceSkill(
  workspaceId: string,
  file: File,
  onProgress: ((percent: number) => void) | null = null,
): Promise<SkillInfo> {
  const formData = new FormData();
  formData.append('file', file);
  const { data } = await api.post<SkillInfo>(
    `/api/v1/workspaces/${workspaceId}/skills`,
    formData,
    uploadConfig(onProgress),
  );
  return data;
}

/** Workspace rows flip in place; inherited names (platform or user tier)
 * write a workspace-level disable. */
export async function setWorkspaceSkillEnabled(
  workspaceId: string,
  name: string,
  enabled: boolean,
): Promise<SkillInfo> {
  const { data } = await api.patch<SkillInfo>(
    `/api/v1/workspaces/${workspaceId}/skills/${encodeURIComponent(name)}`,
    { enabled },
  );
  return data;
}

/** Re-alias a workspace-scoped row's trigger; inherited names 409. */
export async function setWorkspaceSkillCommand(
  workspaceId: string,
  name: string,
  command: string | null,
): Promise<SkillInfo> {
  const { data } = await api.patch<SkillInfo>(
    `/api/v1/workspaces/${workspaceId}/skills/${encodeURIComponent(name)}`,
    { command },
  );
  return data;
}

export async function deleteWorkspaceSkill(
  workspaceId: string,
  name: string,
): Promise<void> {
  await api.delete(
    `/api/v1/workspaces/${workspaceId}/skills/${encodeURIComponent(name)}`,
  );
}
