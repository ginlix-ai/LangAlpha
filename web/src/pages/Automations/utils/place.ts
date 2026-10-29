import type { Automation } from '@/types/automation';

/**
 * Where an automation runs. Every automation names a workspace, and Home's id
 * means the Chief of Staff, which runs there. The server still sends
 * null for a row whose workspace is gone.
 */
export type PlaceRow = Pick<Automation, 'workspace_id'>;

/** Whether the row runs in Home, and so reads as the Chief of Staff. */
export function runsInHome(row: PlaceRow, homeId: string | undefined): boolean {
  return !!homeId && row.workspace_id === homeId;
}
