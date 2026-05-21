/**
 * Template system API client.
 */
import type { AxiosResponse } from 'axios';
import { api } from '@/api/client';
import type {
  TemplateEntry,
  TemplateEntryListResponse,
  TemplateInstantiateRequest,
  TemplateListResponse,
  TemplateManifest,
} from '@/types/template';

const BASE = '/api/v1/templates';

export const listTemplates = (): Promise<AxiosResponse<TemplateListResponse>> =>
  api.get(BASE);

export const getTemplate = (templateId: string): Promise<AxiosResponse<TemplateManifest>> =>
  api.get(`${BASE}/${templateId}`);

export const listTemplateEntries = (
  templateId: string,
  params: { status?: string; limit?: number; offset?: number } = {},
): Promise<AxiosResponse<TemplateEntryListResponse>> =>
  api.get(`${BASE}/${templateId}/entries`, { params });

/**
 * Find the template entry bound to a specific workspace_id.
 * Uses the list endpoint and finds the matching entry client-side
 * (each workspace can have at most one entry — enforced by UNIQUE constraint).
 */
export const getTemplateEntryByWorkspace = async (
  templateId: string,
  workspaceId: string,
): Promise<TemplateEntry | null> => {
  const res = await listTemplateEntries(templateId, { limit: 200 });
  return res.data.entries.find((e) => e.workspace_id === workspaceId) ?? null;
};

export const getTemplateEntry = (
  templateId: string,
  entryId: string,
): Promise<AxiosResponse<TemplateEntry>> =>
  api.get(`${BASE}/${templateId}/entries/${entryId}`);

export const instantiateTemplateEntry = (
  templateId: string,
  body: TemplateInstantiateRequest,
): Promise<AxiosResponse<TemplateEntry>> =>
  api.post(`${BASE}/${templateId}/entries`, body);

export const rerunTemplateEntry = (
  templateId: string,
  entryId: string,
): Promise<AxiosResponse<TemplateEntry>> =>
  api.post(`${BASE}/${templateId}/entries/${entryId}/rerun`);

export const deleteTemplateEntry = (
  templateId: string,
  entryId: string,
  deleteWorkspace = true,
): Promise<AxiosResponse<{ deleted: boolean; entry_id: string }>> =>
  api.delete(`${BASE}/${templateId}/entries/${entryId}`, {
    params: { delete_workspace: deleteWorkspace },
  });
