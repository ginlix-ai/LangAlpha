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

/** A Claude plan's usage window, as Claude names it. `seven_day_overage_included`
 *  is the weekly Fable limit. */
export type ClaudeCodePlanWindow = 'five_hour' | 'seven_day' | 'seven_day_opus' | 'seven_day_sonnet' | 'seven_day_overage_included';

/** A window a Claude plan is held at, and until when: both null where
 *  Claude didn't say when it resets. */
export interface ClaudeCodePlanLimit {
  window: ClaudeCodePlanWindow | null;
  until: string | null;
}

/** A limit the plan holds now, and the model family it binds: null for one
 *  that binds every model. Where Claude didn't say when it resets
 *  (`reset_known` false), `until` only bounds the hold and is never shown,
 *  and `window` is null where Claude named none. */
export interface ClaudeCodePlanHeld {
  window: ClaudeCodePlanWindow | null;
  until: string;
  family: 'opus' | 'sonnet' | 'fable' | null;
  reset_known: boolean;
}

/** How much of the Claude plan behind a token its last request reported
 *  used: each window's share (0 to 1) and when it resets, every limit still
 *  ahead, in no particular order, and whether the account bills past the
 *  limit to usage credits (null until reported). A window that has passed is
 *  sent as null. */
export interface ClaudeCodePlan {
  five_hour: { used: number; resets_at: string } | null;
  seven_day: { used: number; resets_at: string } | null;
  limits: ClaudeCodePlanHeld[];
  credits: 'on' | 'off' | null;
  observed_at: string;
}

/** The Claude plan a Check now reads: the saved token's, or a computer's own token's. */
export type PlanCredential = { kind: 'saved' } | { kind: 'computer'; computerId: string };

/** A turn a Claude plan's limit ended: stopped partway (`plan_stopped`, with
 *  whether requests already under way were billed to usage credits), or
 *  refused before it started (`plan_limited`). */
export interface PlanRefusal extends ClaudeCodePlanLimit {
  reason: 'plan_stopped' | 'plan_limited';
  creditsBilled: boolean;
}

/** A reply whose last request was billed to usage credits: the window that
 *  ran out during it, and when it resets, both null where Claude didn't say. */
export type PlanBilled = ClaudeCodePlanLimit;

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
  /** The saved token's plan, null until a request on it reported one.
   *  Absent from a server that predates it. */
  plan?: ClaudeCodePlan | null;
  /** Whole seconds until Check now may run on the saved token again, null
   *  while it may run now. Counted on the server's clock, so it is read
   *  from when the answer arrived, never against a server time. Absent from
   *  a server that predates it. */
  check_in?: number | null;
  /** Whether a check would be billed to usage credits: under Stop the plan
   *  holds a limit on the model a check asks for, and usage credits are not
   *  known to be off. Check now is not offered then. */
  check_bills?: boolean;
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
  /** The plan of the token its turns use: the saved token's on `saved`.
   *  Absent from a server that predates it. */
  plan?: ClaudeCodePlan | null;
  /** Whole seconds until Check now may run on that token again, null while
   *  it may run now, as `ClaudeCodeStatus.check_in` counts them. */
  check_in?: number | null;
  /** Whether a check of that token would be billed (`ClaudeCodeStatus.check_bills`). */
  check_bills?: boolean;
  /** How many Claude Code agents it runs at once, subagents included.
   *  Absent from a server that predates the setting. */
  clis?: number;
}

/** The user's own computers, and each cloud computer of theirs that refuses
 *  a Claude Code turn whatever the credential, by computer id, with the
 *  reason (`needs_bigger_computer`: below the size Claude Code runs at). */
export type ClaudeCodeComputerRefusal = 'needs_bigger_computer';

/** Where a turn's Claude Code CLI runs: the workspace's own computer
 *  (`own`); for every cloud workspace and Home, the own computer the server
 *  picked where the user's cloud computers run none (`picked`); inside Home's
 *  cloud computer (`home_cloud`); or in this server's workers, on no computer
 *  of the user's (`server`). */
export type ClaudeCodeCliKind = 'own' | 'picked' | 'home_cloud' | 'server';

/** Why a turn's Claude Code CLI can run nowhere. */
export type ClaudeCodePlacementRefusal =
  | 'no_own_computer' | 'cli_computer_offline' | 'needs_own_computer' | 'needs_bigger_computer'
  | 'policy_blocked' | 'signed_out' | 'token_refused' | 'needs_perl' | 'computer_unsupported';

/** Where a turn's Claude Code CLI runs (`kind`), on the computer `computer_id`
 *  and `name` name, or the reason it can run nowhere (`refusal`), with
 *  `name` the computer that names (the offline one on
 *  `cli_computer_offline`). */
export interface ClaudeCodeCliPlacement {
  kind: ClaudeCodeCliKind;
  computer_id: string | null;
  name: string | null;
  refusal: ClaudeCodePlacementRefusal | null;
}

export interface ClaudeCodeChiefOfStaff extends ClaudeCodeCliPlacement {
  /** Whether the Chief of Staff runs the user's saved Claude Code Default:
   *  false where its CLI has nowhere to run, where the token there is known
   *  not to work, or where it would sign in with the setup token the user
   *  stored. */
  default_runs: boolean;
}

export interface ClaudeCodeComputers {
  computers: ClaudeCodeComputer[];
  /** Absent from a server that predates it. */
  cloud_refusals?: Record<string, ClaudeCodeComputerRefusal>;
  /** Where the user's cloud computers run no CLI themselves, the own
   *  computer, one of `computers`, that runs it for every cloud workspace
   *  and Home (`picked`), or `no_own_computer` or `cli_computer_offline`.
   *  Null where they do; absent from a server that predates it. */
  cli_computer?: ClaudeCodeCliPlacement | null;
  /** The most a computer may be set to run at once (`clis`). Absent from a
   *  server that predates the setting, which offers none. */
  clis_max?: number;
}

/** A server from before per-computer tokens has no such route, and runs
 *  Claude Code on none of the user's computers. */
export async function getClaudeCodeComputers(): Promise<ClaudeCodeComputers> {
  try {
    const { data } = await api.get('/api/v1/claude-code/computers');
    return data as ClaudeCodeComputers;
  } catch (err) {
    if (apiErrorStatus(err) === 404) return { computers: [] };
    throw err;
  }
}

const computerPath = (computerId: string) => `/api/v1/claude-code/computers/${encodeURIComponent(computerId)}`;

/** `POST /api/v1/claude-code/check` or `.../computers/{id}/check`: one
 *  minimal request on the credential's plan, run where its own check runs,
 *  so its windows are read now. Answers 204; refused with 429 `check_held`
 *  (and `check_in`) within 5 minutes of the last. */
export async function checkClaudeCodePlan(credential: PlanCredential): Promise<void> {
  const path = credential.kind === 'saved' ? '/api/v1/claude-code/check' : `${computerPath(credential.computerId)}/check`;
  await api.post(path);
}

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

/** `PUT .../computers/{id}/clis`: how many Claude Code agents the computer
 *  runs at once, from its next one on. Answers the computer's entry. */
export async function setClaudeCodeComputerClis(computerId: string, clis: number): Promise<ClaudeCodeComputer> {
  const { data } = await api.put(`${computerPath(computerId)}/clis`, { clis });
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
  /** A turn on a cloud computer runs Claude Code inside that computer, not
   *  on this server. Absent from a server that predates it. */
  cli_in_sandbox?: boolean;
  /** The user's cloud computers too small for Claude Code, in their order.
   *  Absent from a server that predates it. */
  too_small?: { computer_id: string; name: string }[];
  /** `state` for a subagent's Claude Code pin, from a server that still
   *  keeps pins off the setup token the user stored; a newer one omits it,
   *  since a pin there runs as the turn's own CLI does. */
  pin_state?: ClaudeCodeSetupState;
  /** Where the Chief of Staff's CLI runs, whenever Claude Code is on for the
   *  user. Null while it is off; absent from a server that predates it. */
  chief_of_staff?: ClaudeCodeChiefOfStaff | null;
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
 *  their own computers (`local`), or on cloud computers too, on the setup
 *  token they let the server store (`stored`). */
export type ClaudeCodeCustodyMode = 'local' | 'stored';

/** `GET /api/v1/claude-code/custody`, and the answer to a choice made. */
export interface ClaudeCodeCustody {
  mode: ClaudeCodeCustodyMode;
  /** What the server lets the user choose now: none while storing is
   *  blocked for them (`blocked`). */
  offered: Exclude<ClaudeCodeCustodyMode, 'local'>[];
  /** An operator turned storing a token off for this user: `stored` is never
   *  offered, and the mode is `local`. */
  blocked: boolean;
  /** The server's verdict for a turn on a cloud computer: what it runs on,
   *  else the reason it is refused for (a `claude_code_off` refusal reason). */
  runs_on: 'stored_token' | null;
  refusal: string | null;
  /** The version of the consent text storing a token needs accepted now. */
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
 *  (`stored_blocked`), or `stored` without the current `disclosure`, the version of the consent copy the dialog showed.
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
