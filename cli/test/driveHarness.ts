// What the Drive command tests share: a stubbed `fetch` that answers by route, the
// CLI's own selection and binding to run a command, and a scratch directory.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, vi } from "vitest";
import { bind, type Command, type Ctx, parseFor, select } from "../src/commands/command.js";
import { COMMANDS } from "../src/commands/index.js";
import type { Credential } from "../src/credentials.js";
import { Api } from "../src/http.js";

export const credential: Credential = { baseUrl: "http://server.test", token: async () => "tok", renew: async () => false };
export const entry = { id: "e1", path: "_cli/a.txt", name: "a.txt", kind: "file", size: 3, sha256: "abc", is_text: true, version: 1 };

export const fetchMock = vi.fn();
/** What the command under test wrote to stdout. */
export const out: string[] = [];
const scratch: string[] = [];

/** Call once at the top of a test file: a fresh stub and output for each test. */
export function useDriveHarness(): void {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
    out.length = 0;
    vi.spyOn(process.stdout, "write").mockImplementation((t) => {
      out.push(typeof t === "string" ? t : Buffer.from(t).toString());
      return true;
    });
  });
  afterEach(() => {
    for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });
}

/** A new empty directory for a test's files, removed after it. */
export function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "drive-"));
  scratch.push(dir);
  return dir;
}

export function localFile(name: string, content: Buffer | string): string {
  const file = join(tempDir(), name);
  writeFileSync(file, content);
  return file;
}

export const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

/** Run ``la drive ...`` words through the same selection and binding the CLI uses. */
export async function run(argv: string[]): Promise<number | void> {
  const selection = select(COMMANDS, argv);
  const command = selection.command as Command;
  const parsed = parseFor(command, selection.rest);
  const args = bind(command, parsed);
  return command.run({ api: new Api(credential), args, json: parsed.values.has("json") } as Ctx);
}

export type Handler = (url: URL, init: RequestInit & { headers: Record<string, string>; body?: unknown }) => Response | Promise<Response>;

/** A request's path below the space: "/uploads", or "/spaces" for the listing that has no space. */
export const drivePath = (u: URL) => u.pathname.replace(/^\/api\/v1\/drive(\/(?!spaces$)[^/]+)?/, "") || "/";

/** Answer each request by "METHOD /path-after-/api/v1/drive"; anything unlisted is a failure of the test. */
export function serve(routes: Record<string, Handler>): void {
  fetchMock.mockImplementation(async (url: URL, init) => {
    const u = new URL(String(url));
    const key = `${init?.method ?? "GET"} ${drivePath(u)}`;
    const handler = routes[key];
    if (!handler) throw new Error(`unexpected request ${key}`);
    return handler(u, init);
  });
}

export const usage = (limit: number) => () => json(200, { spaces: [{ id: "s1", kind: "team", usage: { max_upload_bytes: limit } }] });
export const sent = (key: string) => fetchMock.mock.calls.find(([u, i]) => `${i?.method ?? "GET"} ${drivePath(new URL(String(u)))}` === key);
