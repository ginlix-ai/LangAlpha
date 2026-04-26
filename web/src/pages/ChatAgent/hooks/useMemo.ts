import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '../../../lib/queryKeys';
import {
  deleteUserMemo,
  listUserMemos,
  readUserMemo,
  regenerateUserMemo,
  uploadUserMemo,
  writeUserMemo,
  type MemoListResponse,
  type MemoReadResponse,
  type MemoUploadResponse,
  type MemoUploadSource,
} from '../utils/api';

export interface MemoUploadInput {
  file: File;
  source?: MemoUploadSource | null;
}

/**
 * List all user memos. When any entry is still generating metadata
 * (`metadata_status === "pending"`) the query polls every 3 s so the UI
 * sees the status transition. Once everything settles, polling stops.
 */
export function useUserMemoList(enabled: boolean = true) {
  const query = useQuery<MemoListResponse>({
    queryKey: queryKeys.memo.list(),
    queryFn: listUserMemos,
    enabled,
    staleTime: 30_000,
    refetchInterval: (q) => {
      const data = q.state.data as MemoListResponse | undefined;
      if (!data) return false;
      const hasPending = data.entries.some(
        (e) => e.metadata_status === 'pending',
      );
      return hasPending ? 3_000 : false;
    },
  });
  return query;
}

/** Read a single memo's content + metadata. `enabled` controls dispatch. */
export function useReadUserMemo(
  key: string | null,
  enabled: boolean = true,
) {
  return useQuery<MemoReadResponse>({
    queryKey: queryKeys.memo.read(key ?? ''),
    queryFn: () => readUserMemo(key!),
    enabled: enabled && !!key,
    staleTime: 30_000,
    refetchInterval: (q) => {
      const data = q.state.data as MemoReadResponse | undefined;
      return data?.metadata_status === 'pending' ? 3_000 : false;
    },
  });
}

/** Upload a new memo. Invalidates the list so the new row shows up.
 *
 * Accepts either a bare ``File`` (legacy callers — the memo upload tab) or
 * an object ``{file, source}`` so callers can tag the upload's provenance
 * (e.g. ``source_kind: 'sandbox'`` from the file panel "Add to memo" button).
 * When a sandbox source matches an existing memo, the server replaces it
 * in place and ``response.replaced === true``.
 */
export function useUploadUserMemo() {
  const queryClient = useQueryClient();
  return useMutation<MemoUploadResponse, Error, File | MemoUploadInput>({
    mutationFn: (input) => {
      if (input instanceof File) return uploadUserMemo(input);
      return uploadUserMemo(input.file, null, input.source ?? null);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.memo.all });
    },
  });
}

/** Overwrite the text content of an existing memo. */
export function useWriteUserMemo() {
  const queryClient = useQueryClient();
  return useMutation<
    MemoUploadResponse,
    Error,
    { key: string; content: string }
  >({
    mutationFn: ({ key, content }) => writeUserMemo(key, content),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: queryKeys.memo.all });
      queryClient.invalidateQueries({
        queryKey: queryKeys.memo.read(variables.key),
      });
    },
  });
}

/** Delete a memo by key. */
export function useDeleteUserMemo() {
  const queryClient = useQueryClient();
  return useMutation<void, Error, string>({
    mutationFn: (key) => deleteUserMemo(key),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.memo.all });
    },
  });
}

/** Retry metadata generation for a memo (used when status is "failed"). */
export function useRegenerateUserMemo() {
  const queryClient = useQueryClient();
  return useMutation<MemoUploadResponse, Error, string>({
    mutationFn: (key) => regenerateUserMemo(key),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.memo.all });
    },
  });
}
