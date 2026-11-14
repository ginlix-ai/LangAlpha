import type { Credential } from "./credentials.js";
import { CliError, detailMessage, detailReason, Exit, exitFor, unreachable } from "./errors.js";

declare const __LA_VERSION__: string;
export const VERSION = typeof __LA_VERSION__ === "string" ? __LA_VERSION__ : "dev";

/** The command a request came from inside a computer, so the server can tell
 *  which conversation ran it. Set in every agent command's environment. */
const CALL_ENV = "LIVEFS_CALL_ID";

/** A path whose interpolated ids are encoded, once, here: callers hold raw ids. */
export function p(strings: TemplateStringsArray, ...ids: string[]): string {
  return strings.reduce((path, part, i) => path + part + (i < ids.length ? encodeURIComponent(ids[i]!) : ""), "");
}

export type Query = Record<string, string | number | boolean | undefined>;

export interface RequestOptions {
  query?: Query;
  body?: unknown;
  accept?: string;
  /** Send no token: asking how to sign in must not first try to refresh a
   *  sign-in that may be the reason for asking. */
  anonymous?: boolean;
}

/** Clock and pause the approval wait runs on, replaceable so a test need not sleep. */
export interface ApprovalClock {
  now(): number;
  sleep(ms: number): Promise<void>;
}

const realClock: ApprovalClock = { now: () => Date.now(), sleep: (ms) => new Promise((r) => setTimeout(r, ms)) };
/** A pending answer faster than this means the server is not holding the request. */
const FAST_PENDING_MS = 2_000;
const PENDING_PAUSE_MS = 1_000;

/** Minutes until the request expires, for the waiting line only. Measured
 *  from the reply's own `Date` when it has one, since the machine's clock may
 *  drift; minutes rather than a clock time, since the machine's timezone is
 *  rarely the user's. */
function minutesLeft(res: Response, expiresAt: unknown, localNow: number): string {
  const serverNow = Date.parse(res.headers.get("date") ?? "");
  const ms = Date.parse(String(expiresAt)) - (Number.isNaN(serverNow) ? localNow : serverNow);
  const minutes = Number.isNaN(ms) ? 1 : Math.max(1, Math.round(ms / 60_000));
  return minutes === 1 ? "1 minute" : `${minutes} minutes`;
}

export class Api {
  constructor(
    readonly credential: Credential,
    private readonly clock: ApprovalClock = realClock,
  ) {}

  get baseUrl(): string {
    return this.credential.baseUrl;
  }

  async json<T>(method: string, path: string, opts: RequestOptions = {}): Promise<T> {
    const text = await (await this.send(method, path, opts)).text();
    if (!text) throw new CliError(`${method} ${path} answered with no body`, Exit.error);
    return JSON.parse(text) as T;
  }

  /** A call whose answer carries nothing the caller reads. */
  async call(method: string, path: string, opts: RequestOptions = {}): Promise<void> {
    const res = await this.send(method, path, opts);
    await res.body?.cancel();
  }

  /** The raw response once it is a success; an error otherwise. A request the
   *  server holds for the user's approval comes back pending each time the
   *  server stops holding it, and is sent again, identically, naming the
   *  approval the last answer named, until the server answers it some other
   *  way. The server alone decides when it has expired. */
  async send(method: string, path: string, opts: RequestOptions = {}): Promise<Response> {
    let approval: string | undefined;
    for (;;) {
      const started = this.clock.now();
      const res = await this.once(method, path, opts, approval);
      if (res.ok) return res;
      const text = await res.text().catch(() => "");
      const body = parse(text);
      const pending = res.status === 403 ? pendingDetail(body) : undefined;
      if (!pending) throw httpError(res, text, body, this.credential);
      if (approval === undefined) {
        const left = minutesLeft(res, pending.expires_at, this.clock.now());
        process.stderr.write(`Waiting for your approval in the LangAlpha app (expires in ${left})…\n`);
      }
      approval = pending.approval_id;
      if (this.clock.now() - started < FAST_PENDING_MS) await this.clock.sleep(PENDING_PAUSE_MS);
    }
  }

  /** One send, and once more when a 401 meets a credential with a different
   *  token to offer. */
  private async once(method: string, path: string, opts: RequestOptions, approval?: string): Promise<Response> {
    const extra: Record<string, string> = approval ? { "x-langalpha-approval": approval } : {};
    const res = await this.attempt(method, path, opts, extra);
    if (res.status !== 401 || opts.anonymous || !(await this.credential.renew())) return res;
    await res.body?.cancel();
    return this.attempt(method, path, opts, extra);
  }

  private async attempt(
    method: string,
    path: string,
    opts: RequestOptions,
    extra: Record<string, string>,
  ): Promise<Response> {
    const url = new URL(`${this.baseUrl}${path}`);
    for (const [k, v] of Object.entries(opts.query ?? {})) {
      if (v !== undefined) url.searchParams.set(k, String(v));
    }
    const headers: Record<string, string> = {
      ...extra,
      accept: opts.accept ?? "application/json",
      "user-agent": `langalpha-cli/${VERSION}`,
    };
    const token = opts.anonymous ? undefined : await this.credential.token();
    if (token) headers.authorization = `Bearer ${token}`;
    const call = process.env[CALL_ENV];
    if (call) headers["x-langalpha-call"] = call;
    if (opts.body !== undefined) headers["content-type"] = "application/json";
    try {
      return await fetch(url, {
        method,
        headers,
        body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
      });
    } catch (e) {
      throw unreachable(this.baseUrl, e);
    }
  }
}

function parse(text: string): unknown {
  try {
    return text ? JSON.parse(text) : undefined;
  } catch {
    return undefined;
  }
}

/** The detail of a request still waiting on the user's answer. */
function pendingDetail(body: unknown): { approval_id: string; expires_at: unknown } | undefined {
  if (detailReason(body) !== "approval_pending") return undefined;
  const { approval_id, expires_at } = (body as { detail: { approval_id?: unknown; expires_at?: unknown } }).detail;
  return typeof approval_id === "string" ? { approval_id, expires_at } : undefined;
}

function httpError(res: Response, text: string, body: unknown, credential: Credential): CliError {
  let message = detailMessage(body, text.trim() || `HTTP ${res.status}`);
  if (res.status === 401 && credential.loginHint) message = `${message}. ${credential.loginHint}`;
  const retry = res.headers.get("retry-after");
  if (res.status === 429 && retry) message = `${message} (retry after ${retry}s)`;
  const reason = detailReason(body);
  return new CliError(message, exitFor(res.status, reason), { status: res.status, detail: body, reason });
}
