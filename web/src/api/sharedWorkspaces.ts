import { api } from '@/api/client';
import type { MembersView, SharedRole, ShareOut, SharedWorkspaceCreate, SharedWorkspaceEntry, SharedWorkspaceList, TeamView } from '@/types/sharedWorkspaces';

const BASE = '/api/v1/shared-workspaces';
const members = (workspaceId: string) => `/api/v1/workspaces/${encodeURIComponent(workspaceId)}/members`;

export async function listSharedWorkspaces(): Promise<SharedWorkspaceList> {
  const { data } = await api.get<SharedWorkspaceList>(BASE);
  return data;
}

export async function createSharedWorkspace(body: SharedWorkspaceCreate): Promise<SharedWorkspaceEntry> {
  const { data } = await api.post<SharedWorkspaceEntry>(BASE, body);
  return data;
}

export async function getMembers(workspaceId: string): Promise<MembersView> {
  const { data } = await api.get<MembersView>(members(workspaceId));
  return data;
}

export async function addMember(workspaceId: string, email: string, role: SharedRole = 'member'): Promise<MembersView> {
  const { data } = await api.post<MembersView>(members(workspaceId), { email, role });
  return data;
}

export async function setEveryone(workspaceId: string, everyone: boolean): Promise<MembersView> {
  const { data } = await api.patch<MembersView>(members(workspaceId), { everyone });
  return data;
}

/** Name a team the caller is in on the workspace, which closes it to
 *  everyone, or with null name none. */
export async function setTeam(workspaceId: string, teamId: string | null): Promise<MembersView> {
  const { data } = await api.patch<MembersView>(members(workspaceId), { team_id: teamId });
  return data;
}

/** The team the workspace names and its people, read from the auth service
 *  when asked. Fails (503) when that service cannot say. */
export async function getTeam(workspaceId: string): Promise<TeamView> {
  const { data } = await api.get<TeamView>(`/api/v1/workspaces/${encodeURIComponent(workspaceId)}/team`);
  return data;
}

export async function setShareOut(workspaceId: string, sharing: ShareOut): Promise<MembersView> {
  const { data } = await api.patch<MembersView>(members(workspaceId), { sharing });
  return data;
}

export async function setMemberRole(workspaceId: string, userId: string, role: SharedRole): Promise<MembersView> {
  const { data } = await api.patch<MembersView>(`${members(workspaceId)}/${encodeURIComponent(userId)}`, { role });
  return data;
}

/** Remove a member, or leave when `userId` is the caller's own. */
export async function removeMember(workspaceId: string, userId: string): Promise<void> {
  await api.delete(`${members(workspaceId)}/${encodeURIComponent(userId)}`);
}

/** Where a shared workspace this page held stands now: whether it was
 *  deleted, or the caller no longer reaches it (said as ``gone`` for anyone
 *  who would otherwise learn whether it still exists). */
export type SharedStanding = 'active' | 'deleted' | 'gone';

export async function getSharedStanding(workspaceId: string): Promise<SharedStanding> {
  const { data } = await api.get<{ state: SharedStanding }>(`${BASE}/${encodeURIComponent(workspaceId)}/standing`);
  return data.state;
}
