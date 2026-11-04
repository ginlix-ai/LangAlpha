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

export class Api {
  constructor(readonly credential: Credential) {}

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

  /** The raw response once it is a success; an error otherwise. A 401 is
   *  retried once when the credential has a different token to offer. */
  async send(method: string, path: string, opts: RequestOptions = {}): Promise<Response> {
    let res = await this.attempt(method, path, opts);
    if (res.status === 401 && !opts.anonymous && (await this.credential.renew())) {
      await res.body?.cancel();
      res = await this.attempt(method, path, opts);
    }
    if (!res.ok) throw await httpError(res, this.credential);
    return res;
  }

  private async attempt(method: string, path: string, opts: RequestOptions): Promise<Response> {
    const url = new URL(`${this.baseUrl}${path}`);
    for (const [k, v] of Object.entries(opts.query ?? {})) {
      if (v !== undefined) url.searchParams.set(k, String(v));
    }
    const headers: Record<string, string> = {
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

async function httpError(res: Response, credential: Credential): Promise<CliError> {
  const text = await res.text().catch(() => "");
  let body: unknown;
  try {
    body = text ? JSON.parse(text) : undefined;
  } catch {
    body = undefined;
  }
  let message = detailMessage(body, text.trim() || `HTTP ${res.status}`);
  if (res.status === 401 && credential.loginHint) message = `${message}. ${credential.loginHint}`;
  const retry = res.headers.get("retry-after");
  if (res.status === 429 && retry) message = `${message} (retry after ${retry}s)`;
  return new CliError(message, exitFor(res.status), { status: res.status, detail: body, reason: detailReason(body) });
}
