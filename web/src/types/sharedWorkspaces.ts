/** Shared workspaces: one workspace several members of an organization each
 *  run their own turns in. Shapes as `/api/v1/shared-workspaces` and
 *  `/api/v1/workspaces/{id}/members` send them. */
import type { Workspace } from './api';

export type SharedRole = 'manager' | 'member';

/** Who may share a shared workspace's work outside it: managers only, any
 *  member, or no one (its links then open only for its people). Kept, and
 *  not applied, while the workspace is open to everyone. */
export type ShareOut = 'managers' | 'members' | 'none';

/** The one team a shared workspace may name, beside its listed people. */
export interface TeamRef {
  id: string;
  /** Known when the caller is in it; the team view gives it to anyone else. */
  name: string | null;
}

/** What the server tells the caller of a shared workspace beside its row. */
export interface SharedBlock {
  role: SharedRole;
  /** A manager, or every member while no manager is active. */
  can_manage: boolean;
  /** Open to everyone in the organization, beside the listed people. */
  everyone: boolean;
  created_by: string | null;
  sharing: ShareOut;
  /** The caller may share its work with the organization and publish it. */
  can_share_out: boolean;
  /** The caller may turn a thread's or file's link on or off. */
  can_change_links: boolean;
  /** On the workspace's own list; false for someone in only through its team
   *  or everyone, who cannot leave it. Absent from an older server: listed. */
  listed?: boolean;
  /** The team it names; absent or null for none. */
  team?: TeamRef | null;
  /** An admin of that team, who manages the workspace without being listed. */
  team_admin?: boolean;
}

export interface SharedWorkspaceEntry {
  workspace: Workspace;
  shared: SharedBlock;
  /** In the caller's list: a thread there they have not read. */
  has_unread?: boolean;
}

export interface SharedWorkspaceList {
  workspaces: SharedWorkspaceEntry[];
  /** The `shared_workspaces.create` rule allows the caller to create one. */
  can_create: boolean;
}

export interface SharedWorkspaceCreate {
  name: string;
  description?: string;
  everyone: boolean;
  /** Email addresses of active members of the caller's organization. */
  members: string[];
  /** A team the caller is in; never with `everyone`. */
  team_id?: string;
}

export interface SharedMember {
  user_id: string;
  name: string | null;
  email: string | null;
  avatar_url: string | null;
  role: SharedRole;
  /** False once their access through the organization ended (suspended or
   *  removed); null while their account cannot be read. */
  active: boolean | null;
  added_at: string;
}

/** Whether members' own answer preferences and personal memory reach the agent here. */
export interface MemberContextView {
  /** The caller's organization grants members' own context, and this deployment lets it. */
  organization: boolean;
  /** The workspace's managers have not turned it off here. */
  workspace: boolean;
  /** This deployment offers it, so a manager sees the switch whatever their own grant. */
  offered: boolean;
}

export interface MembersView {
  workspace_id: string;
  everyone: boolean;
  sharing: ShareOut;
  member_context: MemberContextView;
  members: SharedMember[];
  you: Pick<SharedBlock, 'role' | 'can_manage' | 'can_share_out' | 'can_change_links' | 'listed'>;
  team?: TeamRef | null;
}

export interface TeamPerson {
  user_id: string;
  name: string | null;
  email: string | null;
  avatar_url: string | null;
  /** Their role in the team: its admins manage the workspace. */
  role: 'admin' | 'member';
  /** An active member of the organization now. */
  active: boolean;
  /** Also added to the workspace individually, so leaving the team keeps them in. */
  listed: boolean;
}

/** The team a shared workspace names, as the auth service says now. */
export interface TeamView {
  id: string;
  name: string;
  people: TeamPerson[];
}

/** Who a shared workspace's thread or turn names. */
export interface ThreadSender {
  user_id: string;
  name: string | null;
}

/** On a thread of a shared workspace only. */
export interface SharedThreadInfo {
  created_by?: ThreadSender | null;
  last_sender?: ThreadSender | null;
  /** Shared with everyone in the workspace; false while its starter keeps it
   *  to themselves. */
  shared_with_workspace: boolean;
  /** Someone other than its starter has sent a turn there, so it can no
   *  longer be made private. */
  others_sent: boolean;
}
