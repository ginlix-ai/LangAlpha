import { spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { build } from "esbuild";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resolveCredential } from "../src/credentials.js";
import { forgetLogin, type OAuthLogin, readStore, saveLogin, selectServer } from "../src/store.js";

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "la-"));
  process.env.XDG_CONFIG_HOME = dir;
});
afterEach(() => {
  vi.unstubAllGlobals();
});

describe("resolveCredential", () => {
  it("prefers the flag, then the env token, then the computer's file", async () => {
    const file = join(dir, "t.json");
    writeFileSync(file, JSON.stringify({ token: "lac1.file", base_url: "http://box:8000/" }));
    const env = { LANGALPHA_TOKEN_FILE: file };
    expect(await resolveCredential({ token: "flag", env }).token()).toBe("flag");
    expect(await resolveCredential({ env: { ...env, LANGALPHA_TOKEN: "envtok" } }).token()).toBe("envtok");
    const fromFile = resolveCredential({ env });
    expect(fromFile.loginHint).toBeUndefined();
    expect(fromFile.baseUrl).toBe("http://box:8000");
    expect(await fromFile.token()).toBe("lac1.file");
  });

  it("offers the rewritten token after a 401, and only a different one", async () => {
    const file = join(dir, "t.json");
    writeFileSync(file, JSON.stringify({ token: "old", base_url: "http://box:8000" }));
    const cred = resolveCredential({ env: { LANGALPHA_TOKEN_FILE: file } });
    await cred.token();
    expect(await cred.renew()).toBe(false);
    writeFileSync(file, JSON.stringify({ token: "new", base_url: "http://box:8000" }));
    expect(await cred.renew()).toBe(true);
    expect(await cred.token()).toBe("new");
  });

  it("keeps a token file's token on the server it was issued for", async () => {
    const file = join(dir, "t.json");
    writeFileSync(file, JSON.stringify({ token: "lac1.file", base_url: "http://box:8000" }));
    const env = { LANGALPHA_TOKEN_FILE: file };
    expect(() => resolveCredential({ server: "https://elsewhere.test", env })).toThrow(/not https:\/\/elsewhere.test/);
    expect(() => resolveCredential({ env: { ...env, LANGALPHA_URL: "http://box:9000" } })).toThrow(/holds a token for/);
    expect(resolveCredential({ server: "http://box:8000/", env }).baseUrl).toBe("http://box:8000");
    // An empty LANGALPHA_URL, as a .env line with no value, names no server.
    expect(resolveCredential({ env: { ...env, LANGALPHA_URL: "" } }).baseUrl).toBe("http://box:8000");
    expect(resolveCredential({ env: { LANGALPHA_URL: "" } }).baseUrl).toBe("http://localhost:8000");
  });

  it("never falls back to the person's sign-in where the token file is set", async () => {
    await saveLogin("http://mine.test", { kind: "pat", token: "la_pat_mine" }, { select: true });
    expect(() => resolveCredential({ env: { LANGALPHA_TOKEN_FILE: join(dir, "missing.json") } })).toThrow(/holds no token/);
    const nameless = join(dir, "t.json");
    writeFileSync(nameless, JSON.stringify({ token: "lac1.file" }));
    // Nor sends the computer's token to the server that sign-in names.
    expect(() => resolveCredential({ env: { LANGALPHA_TOKEN_FILE: nameless } })).toThrow(/names no server/);
    const named = resolveCredential({ server: "http://box:8000", env: { LANGALPHA_TOKEN_FILE: nameless } });
    expect(named.baseUrl).toBe("http://box:8000");
    expect(await named.token()).toBe("lac1.file");
  });
});

describe("a server that asks no sign-in", () => {
  it("is selected without the sign-in kept for it before", async () => {
    await saveLogin("http://open.test", { kind: "pat", token: "la_pat_old" }, { select: true });
    await selectServer("http://open.test");
    expect(readStore()).toEqual({ current: "http://open.test", servers: {} });
  });
});

describe("an unreadable store", () => {
  it("is none to a reader but never written over", async () => {
    const path = join(dir, "langalpha", "credentials.json");
    mkdirSync(join(dir, "langalpha"), { recursive: true });
    writeFileSync(path, '{"servers": {"http://a.test": {"kind": "pat", "token": "la_pat_a"}}');
    expect(resolveCredential({ token: "flag", env: {} }).baseUrl).toBe("http://localhost:8000");
    await expect(saveLogin("http://b.test", { kind: "pat", token: "la_pat_b" }, { select: true })).rejects.toThrow(/cannot be read/);
    expect(readFileSync(path, "utf8")).toContain("la_pat_a");
  });

  it.each([
    ["whose servers are a list, where a sign-in saved would vanish", '{"servers": []}'],
    ["whose current server is not an address", '{"current": 1, "servers": {}}'],
  ])("includes one %s", async (_, text) => {
    const path = join(dir, "langalpha", "credentials.json");
    mkdirSync(join(dir, "langalpha"), { recursive: true });
    writeFileSync(path, text);
    expect(resolveCredential({ token: "flag", env: {} }).baseUrl).toBe("http://localhost:8000");
    await expect(saveLogin("http://b.test", { kind: "pat", token: "la_pat_b" }, { select: true })).rejects.toThrow(/cannot be read/);
  });
});

describe("refreshing a stored sign-in", () => {
  const SERVER = "http://la.test";
  const expired = (): OAuthLogin => ({
    kind: "oauth",
    access_token: "a0",
    refresh_token: "r0",
    expires_at: Date.now() - 1000,
    token_endpoint: "http://issuer.test/token",
    client_id: "cli",
  });

  /** An issuer whose refresh tokens rotate: each works once. */
  function rotatingIssuer() {
    let n = 0;
    const spent = new Set<string>();
    const fetch = vi.fn(async (_url: string, init: { body: URLSearchParams }) => {
      const token = init.body.get("refresh_token")!;
      if (spent.has(token)) return new Response(JSON.stringify({ error: "invalid_grant" }), { status: 400 });
      spent.add(token);
      await new Promise((r) => setTimeout(r, 30));
      n += 1;
      return Response.json({ access_token: `a${n}`, refresh_token: `r${n}`, expires_in: 3600 });
    });
    vi.stubGlobal("fetch", fetch);
    return fetch;
  }

  it("refreshes once when processes race, and each takes the token the winner saved", async () => {
    await saveLogin(SERVER, expired(), { select: true });
    const fetch = rotatingIssuer();
    // Each credential holds its own copy of the store, as a separate `la` would.
    const racers = [resolveCredential({ env: {} }), resolveCredential({ env: {} }), resolveCredential({ env: {} })];
    const tokens = await Promise.all(racers.map((c) => c.token()));
    expect(tokens).toEqual(["a1", "a1", "a1"]);
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(readStore().servers[SERVER]).toMatchObject({ access_token: "a1", refresh_token: "r1" });
    expect(existsSync(join(dir, "langalpha", "credentials.lock"))).toBe(false);
  });

  it("takes over a lock its holder left behind", async () => {
    await saveLogin(SERVER, expired(), { select: true });
    rotatingIssuer();
    const lock = join(dir, "langalpha", "credentials.lock");
    mkdirSync(lock, { recursive: true });
    writeFileSync(join(lock, `${Date.now() - 60_000}.99999.dead`), "");
    expect(await resolveCredential({ env: {} }).token()).toBe("a1");
    expect(existsSync(lock)).toBe(false);
  });

  it("clears a lock file an earlier build left behind", async () => {
    await saveLogin(SERVER, expired(), { select: true });
    rotatingIssuer();
    const lock = join(dir, "langalpha", "credentials.lock");
    writeFileSync(lock, "");
    const past = (Date.now() - 60_000) / 1000;
    utimesSync(lock, past, past);
    expect(await resolveCredential({ env: {} }).token()).toBe("a1");
    expect(existsSync(lock)).toBe(false);
  });

  it("does not write a sign-in back that `la logout` removed during the refresh", async () => {
    await saveLogin(SERVER, expired(), { select: true });
    const fetch = rotatingIssuer();
    const refreshing = resolveCredential({ env: {} }).token();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalled());
    // The logout waits for the refresh's lock, then removes what it saved.
    expect(await forgetLogin(SERVER)).toBe(true);
    expect(await refreshing).toBe("a1");
    expect(readStore().servers[SERVER]).toBeUndefined();
  });

  it("saves a refresh only over the sign-in it spent", async () => {
    await saveLogin(SERVER, expired(), { select: true });
    const replaced: OAuthLogin = { ...expired(), access_token: "new", refresh_token: "rn", expires_at: Date.now() + 3_600_000 };
    // As a writer that took the lock over while this refresh slept past its
    // staleness would: the store changes under the exchange.
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        writeFileSync(join(dir, "langalpha", "credentials.json"), JSON.stringify({ current: SERVER, servers: { [SERVER]: replaced } }));
        return Response.json({ access_token: "a1", refresh_token: "r1", expires_in: 3600 });
      }),
    );
    expect(await resolveCredential({ env: {} }).token()).toBe("new");
    expect(readStore().servers[SERVER]).toEqual(replaced);
  });
});

describe("the store's lock", () => {
  it("lets one process take over a stale lock when several race for it", async () => {
    // Real processes, since the race is between them: each waits on the
    // same stale lock, then logs entering and leaving while it holds it.
    const lock = join(dir, "langalpha", "credentials.lock");
    mkdirSync(lock, { recursive: true });
    writeFileSync(join(lock, `${Date.now() - 60_000}.99999.dead`), "");
    const script = join(dir, "racer.mjs");
    await build({
      stdin: {
        contents: `
          import { appendFileSync } from "node:fs";
          import { withStoreLock } from ${JSON.stringify(resolve("src/store.ts"))};
          const log = process.argv[2];
          const start = Number(process.argv[3]);
          while (Date.now() < start) {}
          for (let i = 0; i < 5; i++) {
            await withStoreLock(async () => {
              appendFileSync(log, \`in \${process.pid}\\n\`);
              await new Promise((r) => setTimeout(r, 2));
              appendFileSync(log, \`out \${process.pid}\\n\`);
            });
          }
        `,
        resolveDir: resolve("."),
        loader: "ts",
      },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: script,
      logLevel: "silent",
    });
    const log = join(dir, "log");
    writeFileSync(log, "");
    const start = Date.now() + 500;
    const env = { ...process.env, XDG_CONFIG_HOME: dir };
    const racers = Array.from({ length: 8 }, () => {
      const child = spawn(process.execPath, [script, log, String(start)], { env, stdio: "inherit" });
      return new Promise<number | null>((done) => child.on("exit", done));
    });
    expect(await Promise.all(racers)).toEqual(Array(8).fill(0));
    const lines = readFileSync(log, "utf8").trim().split("\n");
    expect(lines).toHaveLength(8 * 5 * 2);
    for (let i = 0; i < lines.length; i += 2) {
      expect(lines[i]).toMatch(/^in /);
      expect(lines[i + 1]).toBe(lines[i]!.replace("in", "out"));
    }
    expect(existsSync(lock)).toBe(false);
  }, 20_000);
});
