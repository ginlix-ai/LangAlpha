import { afterEach, describe, expect, it, vi } from "vitest";
import { Args } from "../src/args.js";
import { Exit } from "../src/errors.js";
import type { Command, Ctx } from "../src/commands/command.js";
import { threads } from "../src/commands/threads.js";
import { workspaces } from "../src/commands/workspaces.js";
import type { Api } from "../src/http.js";

/** An api answering each path with its body, or failing with ``error``. */
function api(bodies: Record<string, unknown>): Api {
  return {
    json: vi.fn(async (_method: string, path: string) => {
      const body = bodies[path];
      if (body instanceof Error) throw body;
      return body;
    }),
  } as unknown as Api;
}

async function printed(command: Command, a: Api, json = false, args = new Args(new Map(), new Map())): Promise<string> {
  const out = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
  const ctx: Ctx = { api: a, args, json };
  await command.run(ctx);
  return out.mock.calls.map(([text]) => String(text)).join("");
}

const list = (commands: Command[]) => commands.find((c) => c.words[1] === "list")!;

afterEach(() => {
  vi.restoreAllMocks();
});

describe("threads list", () => {
  it("names the model each thread's latest turn ran on", async () => {
    const rows = [
      { thread_id: "t1", title: "NVDA", current_status: "completed", latest_run_model: "claude-opus-5-5", llm_model: null },
      { thread_id: "t2", title: "New", current_status: "idle", latest_run_model: null, llm_model: "gpt-6" },
    ];
    const text = await printed(list(threads), api({ "/api/v1/threads": { threads: rows } }));
    const [header, first, second] = text.trimEnd().split("\n");
    expect(header).toMatch(/^THREAD_ID\s+TITLE\s+CURRENT_STATUS\s+MODEL\s/);
    expect(first).toContain("claude-opus-5-5");
    // A thread that has not run says nothing ran it, not the model it holds.
    expect(second).not.toContain("gpt-6");
  });

  it("prints the server's rows as they are under --json", async () => {
    const rows = [{ thread_id: "t1", latest_run_model: "claude-opus-5-5" }];
    expect(JSON.parse(await printed(list(threads), api({ "/api/v1/threads": { threads: rows } }), true))).toEqual(rows);
  });

  it("says a page is not every thread, and where the next one starts", async () => {
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const rows = [{ thread_id: "t21" }, { thread_id: "t22" }];
    const a = api({ "/api/v1/threads": { threads: rows, total: 134 } });
    const args = new Args(new Map<string, string | number | true>([["offset", 20], ["limit", 2]]), new Map());

    await printed(list(threads), a, true, args);

    expect(a.json).toHaveBeenCalledWith("GET", "/api/v1/threads", {
      query: { workspace_id: undefined, archived: undefined, limit: 2, offset: 20 },
    });
    expect(err.mock.calls.map(([t]) => String(t)).join("")).toBe(
      "Showing threads 21-22 of 134, newest first; pass --offset 22 for the next ones.\n",
    );
  });

  it("says nothing more once the page reaches the last thread", async () => {
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await printed(list(threads), api({ "/api/v1/threads": { threads: [{ thread_id: "t1" }], total: 1 } }));
    expect(err).not.toHaveBeenCalled();
  });
});

describe("workspaces list", () => {
  const rows = [
    { workspace_id: "w1", name: "NVDA", dir_name: "NVDA", computer_id: "c1", computer_name: "Cloud", status: "running" },
    { workspace_id: "w2", name: "Lab", dir_name: "Lab", computer_id: "c2", computer_name: "My Mac", status: "stopped" },
  ];

  it("names each workspace's folder and computer", async () => {
    const text = await printed(list(workspaces), api({ "/api/v1/workspaces": { workspaces: rows } }));
    const [header, first, second] = text.trimEnd().split("\n");
    expect(header).toMatch(/^WORKSPACE_ID\s+NAME\s+FOLDER\s+COMPUTER\s+STATUS$/);
    expect(first).toMatch(/^w1\s+NVDA\s+NVDA\s+Cloud\s+running$/);
    expect(second).toMatch(/^w2\s+Lab\s+Lab\s+My Mac\s+stopped$/);
  });

  it("prints the server's rows as they are under --json", async () => {
    expect(JSON.parse(await printed(list(workspaces), api({ "/api/v1/workspaces": { workspaces: rows } }), true))).toEqual(rows);
  });

  it("lists every page, since a workspace left off reads as one that does not exist", async () => {
    const all = Array.from({ length: 150 }, (_, i) => ({ workspace_id: `w${i}`, name: `W${i}` }));
    const json = vi.fn(async (_m: string, _p: string, opts: { query: { limit: number; offset: number } }) => ({
      workspaces: all.slice(opts.query.offset, opts.query.offset + opts.query.limit),
    }));
    const listed = JSON.parse(await printed(list(workspaces), { json } as unknown as Api, true));
    expect(listed).toHaveLength(150);
    expect(json.mock.calls.map(([, , o]) => o.query.offset)).toEqual([0, 100]);
  });

  it("reads one page of at most --limit", async () => {
    const json = vi.fn(async () => ({ workspaces: rows }));
    await printed(list(workspaces), { json } as unknown as Api, true, new Args(new Map([["limit", 2]]), new Map()));
    expect(json).toHaveBeenCalledTimes(1);
    expect(json.mock.calls[0]).toEqual(["GET", "/api/v1/workspaces", { query: { include_home: undefined, limit: 2 } }]);
  });
});

describe("threads cancel", () => {
  const cancel = threads.find((c) => c.words[1] === "cancel")!;

  it("stops only the run --run names, so a later turn is never ended by it", async () => {
    const json = vi.fn(async () => ({ cancelled: true }));
    const args = new Args(new Map([["run", "r-1"]]), new Map([["id", "t-1"]]));
    await printed(cancel, { json } as unknown as Api, true, args);
    expect(json).toHaveBeenCalledWith("POST", "/api/v1/threads/t-1/cancel", { query: { run_id: "r-1" } });
  });

  it("refuses an empty --run, which the server would read as none and stop whatever runs", async () => {
    const json = vi.fn(async () => ({ cancelled: true }));
    const args = new Args(new Map([["run", ""]]), new Map([["id", "t-1"]]));
    await expect(printed(cancel, { json } as unknown as Api, false, args)).rejects.toMatchObject({ code: Exit.usage });
    expect(json).not.toHaveBeenCalled();
  });

  it("says a stop found nothing to stop in the server's words, with no report promised", async () => {
    const json = vi.fn(async () => ({
      cancelled: false, state: "already_finished", message: "Run already finished; nothing to cancel.",
    }));
    const args = new Args(new Map([["run", "r-1"]]), new Map([["id", "t-1"]]));
    const out = await printed(cancel, { json } as unknown as Api, false, args);
    expect(out).toContain("Run already finished; nothing to cancel.");
    expect(out).not.toContain("Cancel requested");
  });
});
