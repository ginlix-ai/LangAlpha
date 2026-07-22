/**
 * Agent long-term memory endpoints (LangGraph store).
 */
import { api } from '@/api/client';

export interface MemoryEntry {
  key: string;
  size: number;
  created_at: string | null;
  modified_at: string | null;
}

export interface MemoryListResponse {
  tier: 'user' | 'workspace';
  entries: MemoryEntry[];
}

export interface MemoryReadResponse {
  tier: 'user' | 'workspace';
  key: string;
  content: string;
  encoding: string;
  created_at: string | null;
  modified_at: string | null;
}

export async function listUserMemory(): Promise<MemoryListResponse> {
  const { data } = await api.get<MemoryListResponse>('/api/v1/memory/user');
  return data;
}

export async function readUserMemory(key: string): Promise<MemoryReadResponse> {
  const { data } = await api.get<MemoryReadResponse>('/api/v1/memory/user/read', {
    params: { key },
  });
  return data;
}

export async function listWorkspaceMemory(workspaceId: string): Promise<MemoryListResponse> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get<MemoryListResponse>(
    `/api/v1/memory/workspaces/${workspaceId}`,
  );
  return data;
}

export async function readWorkspaceMemory(
  workspaceId: string,
  key: string,
): Promise<MemoryReadResponse> {
  if (!workspaceId) throw new Error('Workspace ID is required');
  const { data } = await api.get<MemoryReadResponse>(
    `/api/v1/memory/workspaces/${workspaceId}/read`,
    { params: { key } },
  );
  return data;
}

// --- Memo (user-managed document store) -----------------------------------
