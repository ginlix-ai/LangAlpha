import { createHash, randomBytes } from "node:crypto";
import { spawn } from "node:child_process";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { exchange } from "./credentials.js";
import { CliError, Exit } from "./errors.js";
import type { Api } from "./http.js";
import { printable } from "./output.js";
import { saveLogin } from "./store.js";

export interface CliAuth {
  auth_required: boolean;
  issuer: string | null;
  client_id: string | null;
  /** API scopes to ask for besides identity, where the token's own claim is
   *  what it may do; none where the server's platform grants them. */
  scopes?: string[];
  personal_access_tokens: boolean;
}

export interface Issuer {
  issuer: string;
  clientId: string;
  scopes?: string[];
}

// Who the user is, and a refresh token; the server adds what the token may do
// there when it reads that from the token.
const OIDC_SCOPES = ["openid", "profile", "email", "offline_access"];
const LOGIN_TIMEOUT_MS = 5 * 60_000;
// An issuer that does not answer reads as one with no settings there.
const METADATA_TIMEOUT_MS = 15_000;

export async function discover(api: Api): Promise<CliAuth> {
  try {
    return await api.json<CliAuth>("GET", "/api/v1/auth/cli", { anonymous: true });
  } catch (e) {
    if (e instanceof CliError && e.status === 404) {
      throw new CliError(`${api.baseUrl} does not offer a CLI sign-in`, Exit.error, { status: 404 });
    }
    throw e;
  }
}

export async function readSecret(prompt: string): Promise<string> {
  if (!process.stdin.isTTY) {
    let data = "";
    for await (const chunk of process.stdin) data += chunk;
    return data.trim();
  }
  // At a terminal the token is typed with echo off, so it stays out of
  // scrollback and session recordings.
  process.stderr.write(prompt);
  const stdin = process.stdin;
  stdin.setRawMode(true);
  stdin.resume();
  stdin.setEncoding("utf8");
  try {
    return await new Promise<string>((resolve, reject) => {
      let typed = "";
      const onData = (chunk: string) => {
        for (const ch of chunk) {
          if (ch === "\r" || ch === "\n" || ch === "\u0004") {
            stdin.off("data", onData);
            return resolve(typed.trim());
          }
          if (ch === "\u0003") {
            stdin.off("data", onData);
            return reject(new CliError("Cancelled", Exit.error));
          }
          typed = ch === "\u007f" || ch === "\b" ? typed.slice(0, -1) : typed + ch;
        }
      };
      stdin.on("data", onData);
    });
  } finally {
    stdin.setRawMode(false);
    stdin.pause();
    process.stderr.write("\n");
  }
}

interface Endpoints {
  issuer: string;
  authorization_endpoint: string;
  token_endpoint: string;
}

/** The issuer's endpoints, from a metadata document that names the issuer
 *  the server named (RFC 8414 section 3.3): one served at that address for
 *  another issuer would send the sign-in, and its code, somewhere else. */
async function endpoints(issuer: string): Promise<Endpoints> {
  const base = issuer.replace(/\/+$/, "");
  if (!URL.canParse(base)) throw new CliError(`The server names an issuer that is not a URL: ${printable(base)}`, Exit.error);
  const { origin, pathname } = new URL(base);
  const path = pathname.replace(/\/+$/, "");
  // OAuth puts the well-known segment before the issuer's path (RFC 8414
  // section 3), OpenID Connect after it; the two agree for an issuer with none.
  const docs = [`${origin}/.well-known/oauth-authorization-server${path}`, `${base}/.well-known/openid-configuration`];
  for (const doc of docs) {
    const res = await fetch(doc, { signal: AbortSignal.timeout(METADATA_TIMEOUT_MS) }).catch(() => undefined);
    if (!res?.ok) continue;
    const meta = (await res.json().catch(() => ({}))) as Partial<Endpoints>;
    if (!meta.authorization_endpoint || !meta.token_endpoint) continue;
    if (typeof meta.issuer !== "string" || meta.issuer.replace(/\/+$/, "") !== base) {
      throw new CliError(`The sign-in settings at ${base} are for another issuer (${String(meta.issuer)})`, Exit.auth);
    }
    // The sign-in page goes to the system's opener, which would run a file: or
    // an app's own scheme as readily as a web page, and the code and its
    // verifier go to the token endpoint: never in the clear from an issuer
    // reached over TLS.
    const tls = new URL(base).protocol === "https:";
    const offWeb = [meta.authorization_endpoint, meta.token_endpoint].some((u) => {
      const scheme = URL.canParse(u) ? new URL(u).protocol : "";
      return tls ? scheme !== "https:" : scheme !== "https:" && scheme !== "http:";
    });
    if (offWeb) throw new CliError(`The sign-in settings at ${base} name an endpoint that is not ${tls ? "https" : "a web page"}`, Exit.auth);
    return meta as Endpoints;
  }
  throw new CliError(`Cannot read the sign-in settings of ${base}`, Exit.error);
}

/** Sign in through the browser: authorization code with PKCE, answered on a
 *  loopback port the issuer accepts at any port. */
export async function browserLogin(baseUrl: string, { issuer, clientId, scopes = [] }: Issuer, openBrowser: boolean): Promise<void> {
  const { authorization_endpoint, token_endpoint } = await endpoints(issuer);
  const verifier = randomBytes(32).toString("base64url");
  const state = randomBytes(16).toString("base64url");

  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  let code: string;
  const redirectUri = `http://127.0.0.1:${(server.address() as AddressInfo).port}/callback`;
  try {
    const url = new URL(authorization_endpoint);
    url.search = new URLSearchParams({
      response_type: "code",
      client_id: clientId,
      redirect_uri: redirectUri,
      scope: [...new Set([...OIDC_SCOPES, ...scopes])].join(" "),
      state,
      code_challenge: createHash("sha256").update(verifier).digest("base64url"),
      code_challenge_method: "S256",
    }).toString();
    process.stderr.write(`Open this page to sign in:\n\n  ${printable(url.toString())}\n\n`);
    if (openBrowser) open(url.toString());
    code = await callback(server, state);
  } finally {
    server.close();
  }

  const login = await exchange(token_endpoint, clientId, {
    grant_type: "authorization_code",
    code,
    redirect_uri: redirectUri,
    code_verifier: verifier,
  });
  await saveLogin(baseUrl, login, { select: true });
}

function callback(server: Server, state: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new CliError("Sign-in timed out", Exit.auth)), LOGIN_TIMEOUT_MS);
    server.on("request", (req, res) => {
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      if (url.pathname !== "/callback") {
        res.writeHead(404).end();
        return;
      }
      // A request without this sign-in's state is not the issuer answering
      // it (another process, a stale tab): refused, and the wait goes on.
      if (url.searchParams.get("state") !== state) {
        res.writeHead(400, { "content-type": "text/plain; charset=utf-8" }).end("Not this sign-in.");
        return;
      }
      const error = url.searchParams.get("error");
      const why = url.searchParams.get("error_description");
      const code = url.searchParams.get("code");
      const ok = !error && code;
      res.writeHead(ok ? 200 : 400, { "content-type": "text/plain; charset=utf-8" });
      res.end(ok ? "Signed in to LangAlpha. You can close this tab." : "Sign-in failed. Return to the terminal.");
      clearTimeout(timer);
      if (ok) resolve(code);
      else reject(new CliError(`Sign-in failed: ${error ? (why ? `${error}: ${why}` : error) : "no code"}`, Exit.auth));
    });
  });
}

// Never through cmd.exe on Windows: it reads the URL's `&` as a command
// separator, and the URL comes from the issuer's metadata.
function open(url: string): void {
  const [cmd, args] =
    process.platform === "darwin"
      ? ["open", [url]]
      : process.platform === "win32"
        ? ["rundll32", ["url.dll,FileProtocolHandler", url]]
        : ["xdg-open", [url]];
  try {
    spawn(cmd, args as string[], { stdio: "ignore", detached: true }).on("error", () => {}).unref();
  } catch {
    // The URL is printed; opening it is a convenience.
  }
}
