/**
 * Workspace file endpoints (list/read/write/delete/backup/upload).
 */
import { api } from '@/api/client';

/**
 * Read a text file from workspace sandbox
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.md"
 */
export async function readWorkspaceFile(workspaceId: string, filePath: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath },
  });
  return data; // { workspace_id, path, content, mime, truncated }
}

/**
 * Download a file from workspace sandbox (returns blob URL)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<string>} Blob URL for the file
 */
export async function downloadWorkspaceFile(workspaceId: string, filePath: string) {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'blob',
  });
  return URL.createObjectURL(response.data as Blob);
}

/**
 * Download a file from workspace sandbox as ArrayBuffer (for client-side parsing)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<ArrayBuffer>}
 */
export async function downloadWorkspaceFileAsArrayBuffer(workspaceId: string, filePath: string) {
  const response = await api.get(`/api/v1/workspaces/${workspaceId}/files/download`, {
    params: { path: filePath },
    responseType: 'arraybuffer',
  });
  return response.data as ArrayBuffer;
}

/**
 * Trigger file download in browser
 * @param {string} workspaceId
 * @param {string} filePath
 */
export async function triggerFileDownload(workspaceId: string, filePath: string) {
  const blobUrl = await downloadWorkspaceFile(workspaceId, filePath);
  const fileName = filePath.split('/').pop() || 'download';
  const a = document.createElement('a');
  a.href = blobUrl;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(blobUrl);
}

/**
 * Backup workspace files from sandbox to DB for offline access
 * @param {string} workspaceId
 * @returns {Promise<Object>} { synced, skipped, deleted, errors, total_size }
 */
export async function backupWorkspaceFiles(workspaceId: string) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/files/backup`);
  return data;
}

/**
 * Get backup status: which files are saved in DB
 * @param {string} workspaceId
 * @returns {Promise<Object>} { persisted_files: {path: hash}, total_size }
 */
export async function getBackupStatus(workspaceId: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/backup-status`);
  return data;
}

/**
 * Write full file content to a sandbox file
 * @param {string} workspaceId
 * @param {string} filePath - e.g. "results/report.py"
 * @param {string} content - File content to write
 * @returns {Promise<Object>} { workspace_id, path, size }
 */
export async function writeWorkspaceFile(workspaceId: string, filePath: string, content: string) {
  const { data } = await api.put(`/api/v1/workspaces/${workspaceId}/files/write`,
    { content },
    { params: { path: filePath } }
  );
  return data;
}

/**
 * Read a file without line-limit pagination (for edit mode)
 * @param {string} workspaceId
 * @param {string} filePath
 * @returns {Promise<Object>} { workspace_id, path, content, mime }
 */
export async function readWorkspaceFileFull(workspaceId: string, filePath: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/files/read`, {
    params: { path: filePath, unlimited: true },
  });
  return data;
}

export async function deleteWorkspaceFiles(workspaceId: string, paths: string[]) {
  const { data } = await api.delete(`/api/v1/workspaces/${workspaceId}/files`, {
    data: { paths },
  });
  return data;
}

// --- Sandbox ---

export async function uploadWorkspaceFile(
  workspaceId: string,
  file: File,
  destPath: string | null = null,
  onProgress: ((percent: number) => void) | null = null
) {
  const formData = new FormData();
  formData.append('file', file);
  const params = destPath ? { path: destPath } : {};
  const { data } = await api.post(
    `/api/v1/workspaces/${workspaceId}/files/upload`,
    formData,
    {
      params,
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: onProgress
        ? (e) => onProgress(Math.round((e.loaded * 100) / (e.total || 1)))
        : undefined,
    }
  );
  return data;
}

// --- Vault Secrets ---
