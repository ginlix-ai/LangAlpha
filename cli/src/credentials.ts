import { readFileSync } from "node:fs";
import { CliError, Exit, unreachable } from "./errors.js";
import { type OAuthLogin, putLogin, readStore, withStoreLock } from "./store.js";
import { CLI_NAME } from "./name.js";

export const DEFAULT_SERVER = "http://localhost:8000";
// Refresh this long before the access token runs out, so a call in flight
// never carries one that lapses on the way.
const REFRESH_MARGIN_MS = 30_000;
// Well inside the store lock's staleness, which a refresh holds across this.
const EXCHANGE_TIMEOUT_MS = 15_000;
const SIGN_IN_AGAIN = `Run \`${CLI_NAME} login\` again.`;

/** Where the token a request carries comes from. */
export interface Credential {
  baseUrl: string;
  /** What to tell the user when a 401 stands after ``renew``, if signing in would help. */
  loginHint?: string;
  token(): Promise<string | undefined>;
  /** After a 401: whether a different token is now on hand to retry with. */
  renew(): Promise<boolean>;
}

export interface CredentialOptions {
  server?: string;
  token?: string;
  env?: NodeJS.ProcessEnv;
}

/** ``--token`` > ``LANGALPHA_TOKEN`` > ``LANGALPHA_TOKEN_FILE`` (what a
 *  computer's agent gets) > a stored login > none, which an oss server takes. */
export function resolveCredential(opts: CredentialOptions = {}): Credential {
  const env = opts.env ?? process.env;
  const given = opts.token || env.LANGALPHA_TOKEN || undefined;
  const filePath = given ? undefined : env.LANGALPHA_TOKEN_FILE;
  // An empty value, as `LANGALPHA_URL=` in a .env file, names no server.
  const server = opts.server || env.LANGALPHA_URL || undefined;
  if (filePath) return tokenFile(server, filePath);
  const store = readStore();
  const baseUrl = trimUrl(server ?? store.current ?? DEFAULT_SERVER);
  if (given) return fixed(baseUrl, given);
  const login = store.servers[baseUrl];
  if (login?.kind === "pat") return { ...fixed(baseUrl, login.token), loginHint: SIGN_IN_AGAIN };
  if (login?.kind === "oauth") return oauth(baseUrl, login);
  return {
    baseUrl,
    loginHint: `Sign in with \`${CLI_NAME} login\`, or set LANGALPHA_TOKEN.`,
    token: async () => undefined,
    renew: async () => false,
  };
}

function origin(url: string): string {
  try {
    return new URL(url).origin;
  } catch {
    return url;
  }
}

function trimUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function fixed(baseUrl: string, token: string): Credential {
  return { baseUrl, token: async () => token, renew: async () => false };
}

interface TokenFile {
  token: string;
  base_url?: string;
}

function readTokenFile(path: string): TokenFile | undefined {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as Partial<TokenFile>;
    if (typeof parsed.token !== "string" || !parsed.token) return undefined;
    return { token: parsed.token, base_url: typeof parsed.base_url === "string" ? parsed.base_url : undefined };
  } catch {
    return undefined;
  }
}

/** The file the server keeps fresh in a computer: read per request, since it
 *  is rewritten under a running command, and again after a 401. Where it is
 *  set, nothing else is used: a computer's command falling back to the
 *  person's own sign-in on that machine would act with all of their rights,
 *  deletes included, and one sending the computer's token to the server
 *  that sign-in names would hand it to a server it was never meant for. */
function tokenFile(server: string | undefined, path: string): Credential {
  const first = readTokenFile(path);
  if (!first) {
    throw new CliError(`LANGALPHA_TOKEN_FILE is set, but ${path} holds no token. Unset it to use your own sign-in.`, Exit.auth);
  }
  // The file names the server its token was issued for; sending the token
  // anywhere else would hand a computer's credential to an unrelated host.
  if (server && first.base_url && origin(server) !== origin(first.base_url)) {
    throw new CliError(
      `${path} holds a token for ${trimUrl(first.base_url)}, not ${trimUrl(server)}. Drop --server and LANGALPHA_URL, or unset LANGALPHA_TOKEN_FILE.`,
      Exit.auth,
    );
  }
  const baseUrl = server ?? first.base_url;
  if (!baseUrl) {
    throw new CliError(`LANGALPHA_TOKEN_FILE is set, but ${path} names no server. Pass --server or set LANGALPHA_URL.`, Exit.auth);
  }
  let used = first.token;
  return {
    baseUrl: trimUrl(baseUrl),
    token: async () => {
      used = readTokenFile(path)?.token ?? used;
      return used;
    },
    renew: async () => {
      const now = readTokenFile(path)?.token;
      return now !== undefined && now !== used;
    },
  };
}

function oauth(baseUrl: string, login: OAuthLogin): Credential {
  let current = login;
  const refresh = async (): Promise<boolean> => {
    const next = await refreshLogin(baseUrl, current);
    if (next) current = next;
    return next !== undefined;
  };
  return {
    baseUrl,
    loginHint: SIGN_IN_AGAIN,
    token: async () => {
      if (expiring(current)) await refresh();
      return current.access_token;
    },
    renew: refresh,
  };
}

function expiring(login: OAuthLogin): boolean {
  return login.expires_at - Date.now() < REFRESH_MARGIN_MS;
}

/** Refresh tokens rotate, and agents run `la` side by side: a process
 *  refreshing from the copy it read at start would spend a token another
 *  process already spent. So a refresh runs under the store's lock, from the
 *  stored login, and a login another process saved meanwhile is used as is
 *  while it lasts. None when there is nothing to refresh with, or the
 *  sign-in was removed or replaced since, before the exchange or during it:
 *  what it bought is saved only over the sign-in it spent. */
async function refreshLogin(baseUrl: string, held: OAuthLogin): Promise<OAuthLogin | undefined> {
  return withStoreLock(async () => {
    const stored = readStore().servers[baseUrl];
    if (stored?.kind !== "oauth") return undefined;
    if (stored.access_token !== held.access_token && !expiring(stored)) return stored;
    if (!stored.refresh_token) return undefined;
    let next: OAuthLogin;
    try {
      next = await exchange(stored.token_endpoint, stored.client_id, {
        grant_type: "refresh_token",
        refresh_token: stored.refresh_token,
      });
    } catch (e) {
      // Only the refresh token itself refused ends the sign-in; an issuer
      // that failed or throttled leaves it as it is, to try again.
      if (e instanceof CliError && e.reason === "invalid_grant") {
        throw new CliError(`Your sign-in has ended. ${SIGN_IN_AGAIN}`, Exit.auth, { status: e.status });
      }
      throw e;
    }
    // The lock outlasts any exchange, but one that slept past its staleness
    // was taken over, and the store may hold another sign-in now, or none.
    const now = readStore().servers[baseUrl];
    if (now?.kind !== "oauth") return undefined;
    if (now.refresh_token !== stored.refresh_token) return expiring(now) ? undefined : now;
    putLogin(baseUrl, next);
    return next;
  });
}

/** One grant at the issuer's token endpoint, for a sign-in or a refresh. */
export async function exchange(
  tokenEndpoint: string,
  clientId: string,
  grant: Record<string, string>,
): Promise<OAuthLogin> {
  let res: Response;
  try {
    res = await fetch(tokenEndpoint, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ ...grant, client_id: clientId }),
      signal: AbortSignal.timeout(EXCHANGE_TIMEOUT_MS),
    });
  } catch (e) {
    throw unreachable(new URL(tokenEndpoint).origin, e);
  }
  const body = (await res.json().catch(() => ({}))) as Record<string, unknown>;
  if (!res.ok || typeof body.access_token !== "string") {
    const why = String(body.error_description ?? body.error ?? res.status);
    const reason = typeof body.error === "string" ? body.error : undefined;
    throw new CliError(`Sign-in failed: ${why}`, Exit.auth, { status: res.status, reason });
  }
  const expiresIn = typeof body.expires_in === "number" ? body.expires_in : 300;
  return {
    kind: "oauth",
    access_token: body.access_token,
    // A refresh rotates the refresh token; keep the old one only if none came back.
    refresh_token: typeof body.refresh_token === "string" ? body.refresh_token : grant.refresh_token,
    expires_at: Date.now() + expiresIn * 1000,
    token_endpoint: tokenEndpoint,
    client_id: clientId,
  };
}
