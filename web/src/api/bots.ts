/**
 * Bot management endpoints.
 *
 * A bot is the identity a turn answers as: it names the speaker and fixes the
 * model and approval surface for that thread's turns. It belongs to one
 * workspace, which stays the unit of sandbox, files and workspace memory, so
 * a bot never moves between workspaces. The Home bot (the user's Chief of
 * Staff) is derived and created lazily by the server, so this client reads it
 * from the list but never creates or deletes it.
 */
import { api } from '@/api/client';

/** A workspace analyst, or the derived Home bot. Only an analyst is created here. */
export type BotRole = 'analyst' | 'chief_of_staff';

/** A paused bot keeps its threads readable but refuses to answer. */
export type BotStatus = 'active' | 'paused';

/** The four approval levels the server accepts, least autonomous first. */
export type BotApproval = 'ask' | 'edits' | 'auto' | 'full';

/** One bot as the API returns it. */
export interface BotInfo {
  bot_id: string;
  user_id: string;
  workspace_id: string;
  name: string;
  role: BotRole;
  persona: string;
  avatar: Record<string, unknown>;
  provider: string | null;
  model: string | null;
  skills: string[];
  tools_allow: string[];
  tools_deny: string[];
  approval: BotApproval;
  memory_scopes: string[];
  status: BotStatus;
  primary_thread_id: string | null;
  created_at: string | null;
  updated_at: string | null;
}

/** `GET /api/v1/bots`: one answer for every workspace when none is named. */
export interface BotListResponse {
  bots: BotInfo[];
  /** The server's per-workspace cap, for the create dialog to state. */
  max_bots: number;
}

/** The body of `POST /api/v1/bots`. */
export interface BotCreateInput {
  workspace_id: string;
  name: string;
  /** Only an analyst can be made by hand; the Chief of Staff is derived. */
  role?: 'analyst';
  persona?: string;
  avatar?: Record<string, unknown>;
  provider?: string | null;
  model?: string | null;
  skills?: string[];
  tools_allow?: string[];
  tools_deny?: string[];
  approval?: BotApproval;
  memory_scopes?: string[];
}

/** The body of `PATCH /api/v1/bots/{bot_id}`; an absent field is left alone. */
export type BotUpdateInput = Partial<
  Pick<
    BotInfo,
    | 'name'
    | 'persona'
    | 'avatar'
    | 'provider'
    | 'model'
    | 'skills'
    | 'tools_allow'
    | 'tools_deny'
    | 'approval'
    | 'memory_scopes'
    | 'status'
  >
>;

/** `DELETE /api/v1/bots/{bot_id}`: a soft delete; the row stays auditable. */
export interface BotDeleteResponse {
  ok: boolean;
  bot_id: string;
}

/** Every live bot, or one workspace's when `workspaceId` is named. */
export async function getBots(workspaceId: string | null = null): Promise<BotListResponse> {
  const { data } = await api.get<BotListResponse>(
    '/api/v1/bots',
    workspaceId ? { params: { workspace_id: workspaceId } } : undefined,
  );
  return data;
}

export async function getBot(botId: string): Promise<BotInfo> {
  const { data } = await api.get<BotInfo>(`/api/v1/bots/${encodeURIComponent(botId)}`);
  return data;
}

export async function createBot(input: BotCreateInput): Promise<BotInfo> {
  const { data } = await api.post<BotInfo>('/api/v1/bots', input);
  return data;
}

export async function updateBot(botId: string, patch: BotUpdateInput): Promise<BotInfo> {
  const { data } = await api.patch<BotInfo>(`/api/v1/bots/${encodeURIComponent(botId)}`, patch);
  return data;
}

export async function deleteBot(botId: string): Promise<BotDeleteResponse> {
  const { data } = await api.delete<BotDeleteResponse>(`/api/v1/bots/${encodeURIComponent(botId)}`);
  return data;
}
