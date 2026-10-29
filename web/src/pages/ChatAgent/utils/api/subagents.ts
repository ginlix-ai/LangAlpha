/**
 * Subagents API: the user's view of every subagent the agent can hand work
 * to, built-in bases and the user's own files alike.
 *
 * A base's row here is the base merged with the user's override; `base` keeps
 * the base's own values so the pane can show what a reset returns to. Writes
 * carry the view's `version`, and a stale one is refused with a 409 so two
 * writers (this pane, the agent editing the file) never overwrite each other.
 */
import { api, ApiError } from '@/api/client';
import { apiErrorCoded, apiErrorObject, apiErrorStatus } from '@/api/errors';

export type SubagentTool = 'web' | 'market_data' | 'code' | 'shell';

/**
 * Written to model, effort, search_provider or search_depth: follow the
 * thread (or the user's own setting) even where the base pins a value. A
 * null there clears the user's key instead, which lands back on the pin.
 */
export const SUBAGENT_INHERIT = 'inherit';
export type SubagentBrokerages = 'off' | 'on';
export type SubagentAudience = 'analyst' | 'chief_of_staff';

export type SubagentProblemCode =
  | 'model_unavailable'
  | 'skill_missing'
  | 'connector_missing'
  | 'search_not_on_plan'
  | 'search_unavailable'
  | 'depth_not_offered'
  | 'effort_not_offered'
  | 'description_matches'
  | 'base_removed'
  | 'name_collision';

export interface SubagentProblem {
  code: SubagentProblemCode;
  /** The missing name, the matching subagent, the provider... */
  detail?: string | null;
}

/** The fields an override may set, as the base itself sets them. */
export interface SubagentBaseValues {
  model: string | null;
  effort: string | null;
  tools: SubagentTool[];
  search_provider: string | null;
  search_depth: string | null;
  connectors: boolean;
  brokerages: SubagentBrokerages;
  memory: boolean;
  preferred_connectors: string[];
  preferred_skills: string[];
  enabled: boolean;
}

export interface SubagentPlugin {
  name: string;
  display_name: string;
  /** False on an override whose plugin is gone, and absent from a server
   *  that predates it. */
  installed?: boolean;
}

/** The server reads both from one key, so a plugin's comes with its plugin
 *  and no other source has one. */
type SubagentOrigin =
  | { source: 'plugin'; plugin: SubagentPlugin }
  | { source: 'builtin' | 'deployment' | 'custom'; plugin: null };

export type SubagentView = SubagentOrigin & {
  name: string;
  kind: 'base' | 'custom';
  /** A base with an override row. */
  edited: boolean;
  /** Keys the user's row sets. */
  user_fields: string[];
  description: string;
  /** Custom: the text. Base: the rendered, read-only body. */
  instructions: string;
  enabled: boolean;
  /** The saved value; null follows the thread. May be unusable, see problems. */
  model: string | null;
  effort: string | null;
  tools: SubagentTool[];
  search_provider: string | null;
  search_depth: string | null;
  connectors: boolean;
  brokerages: SubagentBrokerages;
  /** Reads the user's memory; it never writes it. */
  memory: boolean;
  preferred_connectors: string[];
  preferred_skills: string[];
  available_to: SubagentAudience[];
  /** The base's own values, null for a custom one. */
  base: SubagentBaseValues | null;
  problems: SubagentProblem[];
  updated_by: 'user' | 'agent' | null;
  updated_thread_id: string | null;
  updated_thread_title: string | null;
  updated_at: string | null;
  /** Null when the name cannot be a file (a deployment name off the pattern). */
  file: string | null;
  /** Send back on PUT and DELETE. */
  version: string;
};

export interface SubagentConnector {
  name: string;
  kind: 'builtin' | 'plugin' | 'user' | 'brokerage';
  label: string;
  /** Switched off in Connectors: still a valid name, but nothing to reach for. */
  enabled: boolean;
}

export interface SubagentsResponse {
  /** What the agent itself runs on, read-only here. */
  main: {
    model: string | null;
    effort: string | null;
    search_provider: string | null;
    search_depth: string | null;
  };
  subagents: SubagentView[];
  /** The names valid for preferred_connectors. */
  connectors: SubagentConnector[];
  /** The names valid for preferred_skills. */
  skills: string[];
  brokerages: { connected: boolean };
  limits: { max_custom: number; description_max: number; instructions_max_bytes: number };
}

/**
 * A partial write. A key set to null clears the user's value; the four
 * inheritable keys also take `SUBAGENT_INHERIT`.
 */
export type SubagentFields = Partial<{
  enabled: boolean | null;
  model: string | null;
  effort: string | null;
  tools: SubagentTool[] | null;
  search_provider: string | null;
  search_depth: string | null;
  connectors: boolean | null;
  brokerages: SubagentBrokerages | null;
  memory: boolean | null;
  preferred_connectors: string[] | null;
  preferred_skills: string[] | null;
  description: string;
  instructions: string;
  available_to: SubagentAudience[];
}>;

export interface SubagentWrite {
  /** Required for an existing subagent; null creates a custom one. */
  version: string | null;
  /** Create only: copy that subagent's effective values, orders excepted. */
  start_from?: string | null;
  fields: SubagentFields;
}

/** One refused field from a 422. */
export interface SubagentFieldError {
  field: string;
  message: string;
  options?: string[];
}

const NOT_READY_TRIES = 3;

/**
 * Every route answers 503 with a Retry-After while the server is still
 * loading the agent config, which is a matter of seconds after a restart.
 * Nothing was read or written, so asking again after the wait is safe for a
 * write too.
 */
async function whenReady<T>(call: () => Promise<T>): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await call();
    } catch (err) {
      if (apiErrorStatus(err) !== 503 || attempt >= NOT_READY_TRIES) throw err;
      const header = err instanceof ApiError ? err.response?.headers['retry-after'] : undefined;
      const seconds = Math.min(Number.parseInt(header ?? '', 10) || 2, 10);
      await new Promise((resolve) => setTimeout(resolve, seconds * 1000));
    }
  }
}

export async function getSubagents(): Promise<SubagentsResponse> {
  const { data } = await whenReady(() => api.get<SubagentsResponse>('/api/v1/subagents'));
  return data;
}

export async function putSubagent(name: string, body: SubagentWrite): Promise<SubagentView> {
  const { data } = await whenReady(() =>
    api.put<SubagentView>(`/api/v1/subagents/${encodeURIComponent(name)}`, body),
  );
  return data;
}

/**
 * Custom: deletes it. Base: drops the override, which is the reset. A name
 * already gone (the agent removed the file) is the outcome asked for.
 */
export async function deleteSubagent(name: string, version: string): Promise<void> {
  try {
    await whenReady(() =>
      api.delete(`/api/v1/subagents/${encodeURIComponent(name)}`, { params: { version } }),
    );
  } catch (err) {
    if (apiErrorStatus(err) !== 404) throw err;
  }
}

/** The write was against a version someone else has since replaced. */
export function isSubagentVersionConflict(err: unknown): boolean {
  return apiErrorCoded(err, 'version_conflict') !== null;
}

/** The refused fields of a 422, empty for any other error. */
export function subagentFieldErrors(err: unknown): SubagentFieldError[] {
  const errors = apiErrorObject(err)?.errors;
  if (!Array.isArray(errors)) return [];
  return errors.filter(
    (e): e is SubagentFieldError =>
      !!e && typeof e === 'object' && typeof (e as SubagentFieldError).field === 'string',
  );
}
