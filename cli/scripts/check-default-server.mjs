// The server a release defaults to, asked what `langalpha login` will ask it:
// that it offers a browser sign-in from an https issuer, and that the issuer
// publishes settings for itself with https endpoints. A release whose default
// fails either would ship a CLI whose first command cannot sign anyone in, and
// only a person would notice.
// Run against the release's own address before the package is published.
import { readFileSync } from "node:fs";

const server = (process.env.LANGALPHA_CLI_DEFAULT_SERVER ?? "").replace(/\/+$/, "");
const { version } = JSON.parse(readFileSync(new URL("../package.json", import.meta.url)));
// The server is asked with the CLI's own headers, so an edge that treats
// clients differently answers this check as it will answer the CLI; the
// issuer's documents are fetched bare, as `login` fetches them.
const CLI_HEADERS = { accept: "application/json", "user-agent": `langalpha-cli/${version}` };

class Downgrade extends Error {}

/** A GET whose every redirect stays on https: a hop through http would let
 *  anyone on that path answer in the server's or the issuer's place. */
async function get(url, headers = {}) {
  for (let hop = 0; hop <= 5; hop++) {
    if (new URL(url).protocol !== "https:") throw new Downgrade(`${url} is not https`);
    const res = await fetch(url, { headers, redirect: "manual", signal: AbortSignal.timeout(20_000) });
    const next = res.status >= 300 && res.status < 400 && res.headers.get("location");
    if (next) {
      url = new URL(next, url).href;
      continue;
    }
    if (!res.ok) throw new Error(`${url} answered ${res.status}`);
    return res.json();
  }
  throw new Error(`${url} redirects too many times`);
}

async function issuerSettings(issuer) {
  const base = issuer.replace(/\/+$/, "");
  const { origin, pathname } = new URL(base);
  const path = pathname.replace(/\/+$/, "");
  // The two documents `login` reads, in its order (RFC 8414, then OIDC).
  for (const doc of [`${origin}/.well-known/oauth-authorization-server${path}`, `${base}/.well-known/openid-configuration`]) {
    const meta = await get(doc).catch((e) => {
      if (e instanceof Downgrade) throw e;
      return undefined;
    });
    if (!meta?.authorization_endpoint || !meta?.token_endpoint) continue;
    return { base, meta };
  }
  throw new Error(`no sign-in settings at ${base}`);
}

const problems = [];
try {
  const cli = await get(`${server}/api/v1/auth/cli`, CLI_HEADERS);
  if (cli.auth_required !== true) problems.push("it asks no sign-in, so it is not the hosted service");
  // `login` adds these to the scopes it asks for; anything but a list of
  // strings, null included, fails the sign-in there.
  if (cli.scopes !== undefined && !(Array.isArray(cli.scopes) && cli.scopes.every((s) => typeof s === "string"))) {
    problems.push(`its scopes ${JSON.stringify(cli.scopes)} are not a list of strings`);
  }
  if (typeof cli.issuer !== "string" || !cli.issuer || typeof cli.client_id !== "string" || !cli.client_id) {
    problems.push("it names no issuer and client, so `login` would fall back to pasting a token");
  } else if (!URL.canParse(cli.issuer) || new URL(cli.issuer).protocol !== "https:") {
    // Its settings are fetched from it, so over http anyone on the path could
    // name their own sign-in and token endpoints.
    problems.push(`its issuer ${cli.issuer} is not https`);
  } else {
    const { base, meta } = await issuerSettings(cli.issuer);
    if (typeof meta.issuer !== "string" || meta.issuer.replace(/\/+$/, "") !== base) {
      problems.push(`the settings at ${base} are for another issuer (${meta.issuer})`);
    }
    for (const endpoint of [meta.authorization_endpoint, meta.token_endpoint]) {
      if (!URL.canParse(endpoint) || new URL(endpoint).protocol !== "https:") problems.push(`${endpoint} is not https`);
    }
  }
} catch (e) {
  problems.push(e instanceof Error ? e.message : String(e));
}

if (problems.length) {
  console.error(`The default server ${server} cannot sign a CLI user in:\n- ${problems.join("\n- ")}`);
  process.exit(1);
}
console.log(`${server} offers the CLI sign-in`);
