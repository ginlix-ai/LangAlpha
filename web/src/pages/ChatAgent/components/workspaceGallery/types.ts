import type { Workspace } from '@/types/api';

/** Gallery view of a workspace: the shared DTO plus manual-order metadata. */
export interface WorkspaceRecord extends Workspace {
  is_pinned?: boolean;
  sort_order?: number;
}
