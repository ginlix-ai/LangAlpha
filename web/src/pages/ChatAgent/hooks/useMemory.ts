import { useCallback } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../../lib/queryKeys';
import {
  listUserMemory,
  listWorkspaceMemory,
  readUserMemory,
  readWorkspaceMemory,
  type MemoryEntry,
  type MemoryReadResponse,
} from '../utils/api';

interface ListResult {
  entries: MemoryEntry[];
  loading: boolean;
  error: string | null;
  refresh: () => void;
}

/** List the user-tier memory entries for the current user. */
export function useUserMemory(enabled: boolean = true): ListResult {
  const queryClient = useQueryClient();
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.memory.user(),
    queryFn: listUserMemory,
    enabled,
    staleTime: 30_000,
  });

  const refresh = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: queryKeys.memory.user() });
  }, [queryClient]);

  return {
    entries: data?.entries ?? [],
    loading: isLoading,
    error: error ? (error as Error).message || 'Failed to load memory' : null,
    refresh,
  };
}

/** List the workspace-tier memory entries for a specific workspace. */
export function useWorkspaceMemory(
  workspaceId: string | null,
  enabled: boolean = true,
): ListResult {
  const queryClient = useQueryClient();
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.memory.workspace(workspaceId ?? ''),
    queryFn: () => listWorkspaceMemory(workspaceId!),
    enabled: enabled && !!workspaceId,
    staleTime: 30_000,
  });

  const refresh = useCallback(() => {
    if (!workspaceId) return;
    queryClient.invalidateQueries({ queryKey: queryKeys.memory.workspace(workspaceId) });
  }, [queryClient, workspaceId]);

  return {
    entries: data?.entries ?? [],
    loading: isLoading,
    error: error ? (error as Error).message || 'Failed to load memory' : null,
    refresh,
  };
}

interface ReadResult {
  data: MemoryReadResponse | undefined;
  loading: boolean;
  error: string | null;
}

export function useReadUserMemory(key: string | null): ReadResult {
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.memory.userRead(key ?? ''),
    queryFn: () => readUserMemory(key!),
    enabled: !!key,
    staleTime: 30_000,
  });
  return {
    data,
    loading: isLoading,
    error: error ? (error as Error).message || 'Failed to read memory file' : null,
  };
}

export function useReadWorkspaceMemory(
  workspaceId: string | null,
  key: string | null,
): ReadResult {
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.memory.workspaceRead(workspaceId ?? '', key ?? ''),
    queryFn: () => readWorkspaceMemory(workspaceId!, key!),
    enabled: !!workspaceId && !!key,
    staleTime: 30_000,
  });
  return {
    data,
    loading: isLoading,
    error: error ? (error as Error).message || 'Failed to read memory file' : null,
  };
}
