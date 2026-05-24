/**
 * Type definitions for the template system.
 * Mirrors src/server/models/template.py on the backend.
 */

export type TemplateEntryStatus = 'pending' | 'analyzing' | 'completed' | 'partial' | 'failed';

export interface TemplateField {
  name: string;
  label: string;
  type: 'text' | 'select' | 'number';
  required: boolean;
  placeholder?: string | null;
  options?: Array<{ value: string; label: string }> | null;
}

export interface TemplateManifest {
  id: string;
  name: string;
  description: string;
  icon?: string | null;
  version: string;
  fields: TemplateField[];
  estimated_minutes?: number | null;
}

export interface TemplateEntry {
  entry_id: string;
  user_id: string;
  template_id: string;
  workspace_id: string;
  entry_key: string;
  display_name?: string | null;
  status: TemplateEntryStatus;
  progress: Record<string, unknown>;
  summary: Record<string, unknown>;
  payload: Record<string, unknown>;
  params: Record<string, unknown>;
  error_message?: string | null;
  created_at: string;
  updated_at: string;
  completed_at?: string | null;
}

export interface TemplateListResponse {
  templates: TemplateManifest[];
}

export interface TemplateEntryListResponse {
  entries: TemplateEntry[];
  total: number;
  limit: number;
  offset: number;
}

export interface TemplateInstantiateRequest {
  entry_key: string;
  display_name?: string;
  params: Record<string, unknown>;
}
