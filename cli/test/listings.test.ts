import { afterEach, describe, expect, it, vi } from "vitest";
import { Args } from "../src/args.js";
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

async function printed(command: Command, a: Api, json = false): Promise<string> {
  const out = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
  const ctx: Ctx = { api: a, args: new Args(new Map(), new Map()), json };
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
});
