import type { Workspace } from '@/types/api';

/** Whether a workspace row is the user's Home, where the Chief of Staff runs.
 *  Read `is_home`, not the status: Home's status is its lifecycle, like
 *  any workspace's. */
export function isHomeWorkspace(row: Pick<Workspace, 'is_home'> | null | undefined): boolean {
  return row?.is_home === true;
}
