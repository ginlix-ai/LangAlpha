/**
 * Model and BYOK API key endpoints.
 * Extracted from Dashboard/utils/api.ts for shared use.
 */
import { api } from '@/api/client';
import type { KeyCandidates, ProviderCatalogEntry, ReasoningDeclaration } from '@/components/model/types';
import type { GatewayIndex, ModelMetadataEntry } from '@/hooks/useFilteredModels';
import type { CompactionProfileCatalog, SearchProviderCatalog, SystemDefaults } from '@/hooks/useAllModels';
import { apiErrorStatus } from '@/api/errors';

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

/** Where this user's saved setup token stands. `off`: this server does not
 *  run turns on a saved token (their flag is off, or the deployment runs
 *  Claude Code only on the user's own computers). `checking`: a saved token Anthropic has not answered for
 *  yet. `unverified`: the check could not reach Anthropic, which a later read
 *  tries again; a turn runs meanwhile. `rejected`: Anthropic refused it.
 *  `policy_blocked`: an organization's Claude Code policy is installed where
 *  it would run, so Claude Code does not run there, whatever the token. */
export type ClaudeCodeState = 'off' | 'no_token' | 'checking' | 'unverified' | 'signed_in' | 'rejected' | 'policy_blocked';

/** `GET /api/v1/claude-code/status`, and the answer to a token saved. A
 *  server that predates it answers 404, which reads as `off`. */
export interface ClaudeCodeStatus {
  /** Claude Code models are on for the user, wherever they run. Absent from
   *  a server that predates it, where on means `state` is not `off`. */
  enabled?: boolean;
  state: ClaudeCodeState;
  /** The Claude account, known only for a token made through Sign in. */
  email: string | null;
  /** When the token stops working; for a pasted one, the latest it can last. */
  expires_at: string | null;
  /** A token is saved, whatever `state` says: one is still there to remove
   *  once Claude Code is off. Absent from a server that predates it. */
  saved?: boolean;
}

/** Where Claude Code stands on one of the user's own computers. `off`: their
 *  flag or the deployment does not run it. `offline`: the computer is not
 *  running or cannot be reached, so its token cannot be read. `needs_perl`:
 *  the computer has no perl, so no token can be used there.
 *  `computer_unsupported`: it runs musl, which no build of Claude Code runs
 *  on. The rest read as the account's (`ClaudeCodeState`), for the token
 *  that computer uses, `policy_blocked` for a policy installed on it. */
export type ClaudeCodeComputerState = 'off' | 'offline' | 'needs_perl' | 'computer_unsupported' | ClaudeCodeState;

/** One of the user's own computers (SSH, or a Mac through Remote control) as
 *  Claude Code sees it. */
export interface ClaudeCodeComputer {
  computer_id: string;
  name: string;
  kind: 'ssh' | 'helper';
  state: ClaudeCodeComputerState;
  /** The token its turns use: its own (`machine`), or the one saved in
   *  Settings (`saved`), which a self-hosted server uses on a computer with
   *  none of its own. Null with neither. */
  source: 'machine' | 'saved' | null;
  /** Whether its own token file is on the computer, one it refuses
   *  included, which can be removed while it runs on the saved token. */
  own_file?: boolean;
  email: string | null;
  expires_at: string | null;
}

/** A server from before per-computer tokens has no such route, and runs
 *  Claude Code on none of the user's computers. */
export async function getClaudeCodeComputers(): Promise<{ computers: ClaudeCodeComputer[] }> {
  try {
    const { data } = await api.get('/api/v1/claude-code/computers');
    return data as { computers: ClaudeCodeComputer[] };
  } catch (err) {
    if (apiErrorStatus(err) === 404) return { computers: [] };
    throw err;
  }
}

const computerPath = (computerId: string) => `/api/v1/claude-code/computers/${encodeURIComponent(computerId)}`;

/** Puts a setup token on the computer once Anthropic takes it; the refusals
 *  are `saveClaudeCodeToken`'s. */
export async function saveClaudeCodeComputerToken(computerId: string, token: string): Promise<ClaudeCodeComputer> {
  const { data } = await api.put(`${computerPath(computerId)}/token`, { token });
  return data as ClaudeCodeComputer;
}

export async function startClaudeCodeComputerSignIn(computerId: string): Promise<{ authorize_url: string; expires_at: string }> {
  const { data } = await api.post(`${computerPath(computerId)}/sign-in`);
  return data as { authorize_url: string; expires_at: string };
}

export async function submitClaudeCodeComputerSignInCode(computerId: string, code: string): Promise<ClaudeCodeComputer> {
  const { data } = await api.post(`${computerPath(computerId)}/sign-in/code`, { code });
  return data as ClaudeCodeComputer;
}

export async function removeClaudeCodeComputerToken(computerId: string): Promise<void> {
  await api.delete(`${computerPath(computerId)}/token`);
}

/** Where the user's Claude Code stands as a whole, the first that holds: a
 *  token works somewhere, one is being checked, one was refused or expired,
 *  somewhere runs it once a token is added, an own computer is out of reach,
 *  every own computer has something that stops it, or nothing the user has
 *  runs it. */
export type ClaudeCodeSetupState = 'connected' | 'checking' | 'refused' | 'not_connected' | 'offline' | 'unsupported' | 'no_computer';

/** `GET /api/v1/claude-code/setup`, read whether the user turned Claude Code
 *  on (`on`) or not. Until they do, an own computer reads `off` where it is
 *  reachable and `offline` where it is not. */
export interface ClaudeCodeSetup {
  state: ClaudeCodeSetupState;
  on: boolean;
  computers: ClaudeCodeComputer[];
  /** This server runs turns on a setup token added in Settings, so one can
   *  be added there whatever the user's computers. */
  saved_token_runs: boolean;
  /** This server runs Claude Code for workspaces on cloud computers too. */
  cloud: boolean;
}

/** Null where this server offers no Claude Code (404), or one from before
 *  this route, which shows none. */
export async function getClaudeCodeSetup(): Promise<ClaudeCodeSetup | null> {
  try {
    const { data } = await api.get('/api/v1/claude-code/setup');
    return data as ClaudeCodeSetup;
  } catch (err) {
    if (apiErrorStatus(err) === 404) return null;
    throw err;
  }
}

export async function getClaudeCodeStatus(): Promise<ClaudeCodeStatus> {
  const { data } = await api.get('/api/v1/claude-code/status');
  return data as ClaudeCodeStatus;
}

/** Whether Claude Code runs for the user on cloud computers: it runs only on
 *  their own computers (`local`), or on cloud computers too, with their
 *  Anthropic API key (`api_key`) or on the setup token they let the server
 *  store (`stored`). */
export type ClaudeCodeCustodyMode = 'local' | 'api_key' | 'stored';

/** `GET /api/v1/claude-code/custody`, and the answer to a choice made. */
export interface ClaudeCodeCustody {
  mode: ClaudeCodeCustodyMode;
  /** An Anthropic API key is saved under Model access, the one `api_key` runs on. */
  api_key_saved: boolean;
  /** What the server lets the user choose now. A mode chosen before its
   *  switch went off is kept, and paused. */
  offered: Exclude<ClaudeCodeCustodyMode, 'local'>[];
  /** An operator turned storing a token off for this user: `stored` is never
   *  offered, and the mode is `local`. */
  blocked: boolean;
  /** The server's verdict for a turn on a cloud computer: what it runs on,
   *  else the reason it is refused for (a `claude_code_off` refusal reason). */
  runs_on: 'api_key' | 'stored_token' | null;
  refusal: string | null;
  /** The version of the consent text storing a token needs accepted now. */
  disclosure: string | null;
}

/** Null where this server offers no choice for cloud computers (404): a
 *  self-hosted one, or one whose operator runs Claude Code only on the user's
 *  own computers. */
export async function getClaudeCodeCustody(): Promise<ClaudeCodeCustody | null> {
  try {
    const { data } = await api.get('/api/v1/claude-code/custody');
    return data as ClaudeCodeCustody;
  } catch (err) {
    if (apiErrorStatus(err) === 404) return null;
    throw err;
  }
}

/** A refusal carries the OAuth routes' `error` body (`readOAuthError`): a
 *  mode not offered now (`unavailable`), storing blocked for the user
 *  (`stored_blocked`), or `stored` without the current `disclosure`.
 *  Leaving `stored` deletes the stored token before it answers. */
export async function setClaudeCodeCustody(mode: ClaudeCodeCustodyMode, disclosure?: string): Promise<ClaudeCodeCustody> {
  const { data } = await api.put('/api/v1/claude-code/custody', disclosure ? { mode, disclosure } : { mode });
  return data as ClaudeCodeCustody;
}

/** Saves the user's setup token once Anthropic takes it, which one real
 *  request checks, so it answers in seconds. A refusal carries the OAuth
 *  routes' `error` body (`readOAuthError`) and saves nothing. */
export async function saveClaudeCodeToken(token: string): Promise<ClaudeCodeStatus> {
  const { data } = await api.put('/api/v1/claude-code/token', { token });
  return data as ClaudeCodeStatus;
}

/** Starts a Claude sign-in that ends in a setup token the server makes for
 *  the user: the page to approve it on, and when the sign-in lapses. */
export async function startClaudeCodeSignIn(): Promise<{ authorize_url: string; expires_at: string }> {
  const { data } = await api.post('/api/v1/claude-code/sign-in');
  return data as { authorize_url: string; expires_at: string };
}

/** Finishes that sign-in with the code Claude's page showed, which the server
 *  exchanges for a token and checks before saving, so it answers in seconds. */
export async function submitClaudeCodeSignInCode(code: string): Promise<ClaudeCodeStatus> {
  const { data } = await api.post('/api/v1/claude-code/sign-in/code', { code });
  return data as ClaudeCodeStatus;
}

export async function removeClaudeCodeToken(): Promise<void> {
  await api.delete('/api/v1/claude-code/token');
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
