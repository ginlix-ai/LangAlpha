/**
 * Model and BYOK API key endpoints.
 * Extracted from Dashboard/utils/api.ts for shared use.
 */
import { api } from '@/api/client';
import type { KeyCandidates, ProviderCatalogEntry, ReasoningDeclaration } from '@/components/model/types';
import type { GatewayIndex, ModelMetadataEntry } from '@/hooks/useFilteredModels';
import type { CompactionProfileCatalog, SearchProviderCatalog, SystemDefaults } from '@/hooks/useAllModels';

/** `GET /api/v1/models`: the deployment's catalog, the same for every user. */
export interface ModelsResponse {
  /** The manifest's models, grouped by the provider that lists them. */
  models: Record<string, { display_name?: string; models?: string[] }>;
  model_metadata: Record<string, ModelMetadataEntry>;
  system_defaults: SystemDefaults;
  compaction_profiles: CompactionProfileCatalog;
  search_providers: SearchProviderCatalog;
  /** Every provider a key, sign-in or local server can be connected for. */
  provider_catalog: ProviderCatalogEntry[];
  /** Where a turn looks for each provider's stored key. */
  key_candidates: Record<string, KeyCandidates>;
  /** How a pick from a gateway's list meets the built-in it is tuned as. */
  gateway_index?: GatewayIndex;
}

export async function getAvailableModels(): Promise<ModelsResponse> {
  const { data } = await api.get('/api/v1/models');
  return data as ModelsResponse;
}

export async function getUserApiKeys(): Promise<Record<string, unknown>> {
  const { data } = await api.get('/api/v1/users/me/api-keys');
  return data;
}

export async function updateUserApiKeys(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
  const { data } = await api.put('/api/v1/users/me/api-keys', payload);
  return data;
}

export async function deleteUserApiKey(provider: string): Promise<Record<string, unknown>> {
  const { data } = await api.delete(`/api/v1/users/me/api-keys/${encodeURIComponent(provider)}`);
  return data;
}

export interface TestResult {
  success: boolean;
  model?: string;
  models_found?: number;
  latency_ms?: number;
  error?: string;
  /** OpenRouter only: what the key may still spend, in USD. */
  credits?: {
    limit: number | null;
    usage: number | null;
    remaining: number | null;
    is_free_tier: boolean | null;
    limit_reset: string | null;
  };
}

/** Checks a key against the provider before it is saved. A refusal by the
 *  provider resolves `success: false`; only our own route failing throws. */
export async function testApiKey(provider: string, apiKey: string, baseUrl?: string): Promise<TestResult> {
  const { data } = await api.post('/api/v1/keys/test', {
    provider,
    api_key: apiKey,
    base_url: baseUrl || undefined,
  });
  return data as TestResult;
}

/** One model a provider lists that the manifest does not tune. */
export interface ListedModel {
  id: string;
  display_name?: string;
  context_window?: number;
  input_modalities?: string[];
  /** USD per million tokens (OpenRouter). */
  pricing?: { input?: number; output?: number; cache_read?: number; cache_write?: number };
  supported_parameters?: string[];
  /** OpenRouter: the result is a model the manifest knows, under `manifest_key`. */
  known?: boolean;
  manifest_key?: string;
  /** OpenRouter: the levels the model takes there, saved with the pick. */
  reasoning?: ReasoningDeclaration;
  /** A gateway's catalog: the provider the model is served on (one of the
   *  connection's `routes`), and what that API needs sent, saved with the pick. */
  route?: string;
  /** The catalog describes the model, so the pick runs tuned by it. */
  described?: boolean;
  parameters?: Record<string, unknown>;
  prompt_guidance?: string;
}

export type AvailableModelsReason =
  | 'unauthorized'
  | 'unreachable'
  | 'bad_response'
  | 'endpoint_refused'
  | 'no_match'
  | 'empty_list'
  | 'needs_reauth'
  | 'refresh_unavailable'
  /** Signed in, but the plan carries no API access (Grok): no model on it runs. */
  | 'no_api_access';

/** What a connected key or subscription can run, from
 *  `GET /api/v1/providers/{p}/available-models`. */
export interface AvailableModels {
  provider: string;
  /** `fallback`: the list could not be read, so `ready` is the manifest's. */
  status: 'ok' | 'fallback';
  reason?: AvailableModelsReason | string;
  fetched_at: string | null;
  /** Manifest keys the credential can run. */
  ready: string[];
  /** Manifest keys for this provider that the credential's list lacks. */
  missing: string[];
  context_windows: Record<string, number>;
  others: ListedModel[];
  total: number;
  cached: boolean;
}

export async function getProviderAvailableModels(
  provider: string,
  opts: { refresh?: boolean; q?: string; signal?: AbortSignal } = {},
): Promise<AvailableModels> {
  const params: Record<string, string> = {};
  if (opts.refresh) params.refresh = '1';
  if (opts.q) params.q = opts.q;
  const { data } = await api.get(`/api/v1/providers/${encodeURIComponent(provider)}/available-models`, {
    params,
    signal: opts.signal,
  });
  return data as AvailableModels;
}

/** A local server's own list (Ollama, LM Studio, vLLM). */
export async function getLocalProviderModels(provider: string): Promise<Array<{ id: string; name: string }>> {
  const { data } = await api.get(`/api/v1/providers/${encodeURIComponent(provider)}/models`);
  return ((data as { models?: Array<{ id: string; name: string }> }).models ?? []);
}

/** A model a custom endpoint lists that the manifest has tuned. */
export interface RecognizedModel {
  /** The manifest key, which the saved entry takes as its name. */
  name: string;
  /** What the endpoint calls it. */
  model_id: string;
  display_name: string;
  /** Present only where the endpoint serves a smaller window than the manifest's. */
  context?: number;
  /** Present only where the endpoint takes fewer inputs than the manifest declares. */
  input_modalities?: string[];
  /** An empty ladder where the endpoint's API cannot take the model's effort control. */
  reasoning?: ReasoningDeclaration;
}

export interface EndpointModels {
  status: 'ok' | 'unavailable';
  reason?: 'unauthorized' | 'unreachable' | 'bad_response' | 'endpoint_refused';
  recognized: RecognizedModel[];
  others: ListedModel[];
}

/** What a custom endpoint lists, read with the user's key before it is saved. */
export async function listEndpointModels(
  body: { api_format: string; base_url: string; api_key: string },
  signal?: AbortSignal,
): Promise<EndpointModels> {
  const { data } = await api.post('/api/v1/providers/custom/models', body, { signal });
  return data as EndpointModels;
}
