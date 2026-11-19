/** What a shared workspace's team means for how it reads to the viewer. */
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';

/** Whether the viewer runs the workspace by role: a listed manager, or an
 *  admin of the team it names, who manages it without being listed. A member
 *  who manages only while no manager is active is not one. */
export function managesIt(entry: SharedWorkspaceEntry): boolean {
  const { shared } = entry;
  return shared.role === 'manager' || shared.team_admin === true;
}
