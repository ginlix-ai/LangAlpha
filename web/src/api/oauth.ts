/**
 * Subscription sign-ins: the device flow ChatGPT and Grok share, and the
 * Claude paste-back flow.
 *
 * Every refusal from these routes carries `{code, message, retryable}` in its
 * `error`, beside a plain `detail`; `readOAuthError` reads it, and the connect flows word each code
 * themselves rather than showing the server's sentence.
 */
import { api } from '@/api/client';

/**
 * Every subscription, in the order they are offered: the provider it is saved
 * under, the short name its query key and copy (`modelAccess.<key>`) go by,
 * the name a connection row carries, and its routes.
 */
export const SUBSCRIPTIONS = [
  { provider: 'codex-oauth', key: 'codex', name: 'ChatGPT', path: '/api/v1/oauth/codex' },
  { provider: 'claude-oauth', key: 'claude', name: 'Claude', path: '/api/v1/oauth/claude' },
  { provider: 'grok-oauth', key: 'grok', name: 'Grok', path: '/api/v1/oauth/grok' },
] as const;

export type Subscription = (typeof SUBSCRIPTIONS)[number];
export type SubscriptionProvider = Subscription['provider'];
export type SubscriptionKey = Subscription['key'];

function subscription(provider: SubscriptionProvider): Subscription {
  return SUBSCRIPTIONS.find((s) => s.provider === provider)!;
}

export function isSubscriptionProvider(provider: string): provider is SubscriptionProvider {
  return SUBSCRIPTIONS.some((s) => s.provider === provider);
}

export function subscriptionKey(provider: SubscriptionProvider): SubscriptionKey {
  return subscription(provider).key;
}

/** The subscriptions that sign in with a short code approved on the provider's page. */
export type DeviceProvider = 'codex-oauth' | 'grok-oauth';

export type ConnectionState = 'connected' | 'needs_reauth' | 'disconnected';

export interface OAuthStatus {
  /** A connection is stored, whether or not it still works. */
  connected: boolean;
  /** Whether it still works: `needs_reauth` once its refresh was refused. */
  state: ConnectionState;
  account_id: string | null;
  email: string | null;
  plan_type: string | null;
  expires_at: string | null;
  needs_reauth_at: string | null;
  last_error_code: string | null;
}

export interface DeviceCode {
  user_code: string;
  verification_url: string;
  /** The page arrives with the code already filled in, so the user only
   *  compares it rather than pasting it. */
  verification_url_has_code: boolean;
  /** Seconds between polls, as the provider asks. */
  interval: number;
  /** When the code stops working; null from a server that predates it. */
  expires_at: string | null;
}

export type DevicePollStatus = 'pending' | 'success' | 'denied' | 'expired';

export interface DevicePollResult {
  status: DevicePollStatus;
  /** Why a `denied` or `expired` ended, e.g. `no_pending_flow`. */
  code?: string;
  email?: string | null;
  plan_type?: string | null;
  account_id?: string | null;
}

export interface OAuthErrorDetail {
  code: string;
  message: string;
  /** Asking again (the same poll, the same paste) can succeed. */
  retryable: boolean;
  upstream_code?: string;
}

/** A status from an older server that sends only `connected`. */
function normalizeStatus(data: Partial<OAuthStatus> | null | undefined): OAuthStatus {
  const connected = data?.connected === true;
  return {
    connected,
    state: data?.state ?? (connected ? 'connected' : 'disconnected'),
    account_id: data?.account_id ?? null,
    email: data?.email ?? null,
    plan_type: data?.plan_type ?? null,
    expires_at: data?.expires_at ?? null,
    needs_reauth_at: data?.needs_reauth_at ?? null,
    last_error_code: data?.last_error_code ?? null,
  };
}

export async function getOAuthStatus(provider: SubscriptionProvider): Promise<OAuthStatus> {
  const { data } = await api.get(`${subscription(provider).path}/status`);
  return normalizeStatus(data as Partial<OAuthStatus>);
}

export async function disconnectOAuth(provider: SubscriptionProvider): Promise<void> {
  await api.delete(subscription(provider).path);
}

export async function initiateDevice(provider: DeviceProvider): Promise<DeviceCode> {
  const { data } = await api.post(`${subscription(provider).path}/device/initiate`);
  const d = data as Partial<DeviceCode>;
  return {
    user_code: String(d.user_code ?? ''),
    verification_url: String(d.verification_url ?? ''),
    verification_url_has_code: d.verification_url_has_code === true,
    interval: typeof d.interval === 'number' && d.interval > 0 ? d.interval : 5,
    expires_at: typeof d.expires_at === 'string' ? d.expires_at : null,
  };
}

export async function pollDevice(provider: DeviceProvider): Promise<DevicePollResult> {
  const { data } = await api.post(`${subscription(provider).path}/device/poll`);
  const d = (data ?? {}) as Record<string, unknown>;
  // `status` is the contract; `success`/`pending` are what older servers sent.
  const status: DevicePollStatus =
    d.status === 'pending' || d.status === 'success' || d.status === 'denied' || d.status === 'expired'
      ? d.status
      : d.success === true
        ? 'success'
        : 'pending';
  return {
    status,
    ...(typeof d.code === 'string' ? { code: d.code } : {}),
    email: (d.email as string | null | undefined) ?? null,
    plan_type: (d.plan_type as string | null | undefined) ?? null,
    account_id: (d.account_id as string | null | undefined) ?? null,
  };
}

export async function initiateClaudeOAuth(): Promise<{ authorize_url: string; expires_at: string | null }> {
  const { data } = await api.post(`${subscription('claude-oauth').path}/initiate`);
  const body = data as { authorize_url?: unknown; expires_at?: unknown };
  return {
    authorize_url: String(body.authorize_url ?? ''),
    expires_at: typeof body.expires_at === 'string' ? body.expires_at : null,
  };
}

export async function submitClaudeCallback(callbackInput: string): Promise<void> {
  await api.post(`${subscription('claude-oauth').path}/callback`, { callback_input: callbackInput });
}

/** The coded detail of a refusal from these routes, or null for anything else
 *  (a dropped connection, a proxy's HTML page). */
export function readOAuthError(err: unknown): OAuthErrorDetail | null {
  const d = (err as { response?: { data?: { error?: Record<string, unknown> | null } } })?.response
    ?.data?.error;
  if (typeof d?.code !== 'string') return null;
  return {
    code: d.code,
    message: typeof d.message === 'string' ? d.message : '',
    retryable: d.retryable === true,
    ...(typeof d.upstream_code === 'string' ? { upstream_code: d.upstream_code } : {}),
  };
}
