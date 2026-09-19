import type { FileOperationArtifactPayload } from '@/types/api';

/**
 * The path a file-operation event should be classified by.
 *
 * `file_path` is workspace relative on the current wire. An older event carries
 * only the raw spelling, and one that names a machine root still classifies
 * because `classifyAgentPath` strips those roots itself; only the folder
 * qualifier is opaque to it, which is why the relative form comes first.
 */
export function fileArtifactPath(payload: Partial<FileOperationArtifactPayload> | undefined): string {
  if (!payload) return '';
  return payload.file_path || payload.sandbox_path || '';
}
