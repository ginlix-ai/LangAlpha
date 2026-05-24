/**
 * React Query hooks for the template system.
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import {
  getTemplate,
  getTemplateEntry,
  instantiateTemplateEntry,
  listTemplateEntries,
  listTemplates,
  rerunTemplateEntry,
  deleteTemplateEntry,
} from '../utils/api';
import type { TemplateInstantiateRequest } from '@/types/template';

const DASHBOARD_POLL_MS = 5000; // dashboard polls every 5s while entries are analyzing

/** All registered templates. */
export function useTemplateManifests() {
  return useQuery({
    queryKey: queryKeys.templates.manifests(),
    queryFn: async () => (await listTemplates()).data,
    staleTime: 5 * 60_000,
  });
}

/** Single template manifest. */
export function useTemplateManifest(templateId: string | undefined) {
  return useQuery({
    queryKey: queryKeys.templates.manifest(templateId ?? ''),
    queryFn: async () => (await getTemplate(templateId!)).data,
    enabled: !!templateId,
    staleTime: 5 * 60_000,
  });
}

/** List entries for a template; polls while any entry is in pending/analyzing. */
export function useTemplateEntries(
  templateId: string | undefined,
  opts: { status?: string; limit?: number; offset?: number } = {},
) {
  return useQuery({
    queryKey: queryKeys.templates.entries(templateId ?? '', opts),
    queryFn: async () =>
      (await listTemplateEntries(templateId!, opts)).data,
    enabled: !!templateId,
    refetchInterval: (q) => {
      const data = q.state.data;
      if (!data) return false;
      const hasInflight = data.entries.some(
        (e) => e.status === 'pending' || e.status === 'analyzing',
      );
      return hasInflight ? DASHBOARD_POLL_MS : false;
    },
  });
}

/** Single entry detail; polls while in-flight. */
export function useTemplateEntry(
  templateId: string | undefined,
  entryId: string | undefined,
) {
  return useQuery({
    queryKey: queryKeys.templates.entry(templateId ?? '', entryId ?? ''),
    queryFn: async () => (await getTemplateEntry(templateId!, entryId!)).data,
    enabled: !!templateId && !!entryId,
    refetchInterval: (q) => {
      const data = q.state.data;
      if (!data) return false;
      return data.status === 'pending' || data.status === 'analyzing'
        ? DASHBOARD_POLL_MS
        : false;
    },
  });
}

/** Create a new entry (kicks off agent run). */
export function useInstantiateEntry(templateId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: TemplateInstantiateRequest) =>
      instantiateTemplateEntry(templateId, body).then((r) => r.data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.templates.entries(templateId) });
    },
  });
}

/** Re-trigger analysis on an existing entry. */
export function useRerunEntry(templateId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (entryId: string) =>
      rerunTemplateEntry(templateId, entryId).then((r) => r.data),
    onSuccess: (_data, entryId) => {
      qc.invalidateQueries({ queryKey: queryKeys.templates.entries(templateId) });
      qc.invalidateQueries({ queryKey: queryKeys.templates.entry(templateId, entryId) });
    },
  });
}

/** Delete an entry and its workspace. */
export function useDeleteEntry(templateId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (entryId: string) =>
      deleteTemplateEntry(templateId, entryId).then((r) => r.data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.templates.entries(templateId) });
    },
  });
}
