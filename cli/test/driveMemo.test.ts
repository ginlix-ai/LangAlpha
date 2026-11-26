import { describe, expect, it, vi } from "vitest";
import { CliError, Exit } from "../src/errors.js";
import { entry, fetchMock, json, localFile, out, run, sent, serve, usage, useDriveHarness } from "./driveHarness.js";

useDriveHarness();

describe("drive memos", () => {
  const body = (key: string) => JSON.parse(sent(key)![1].body);
  const marked = (over = {}) => ({ ...entry, version: 4, memo: { marked: true, status: null, tickers: [], final_seq: null, ...over } });

  it("marks with status, tickers and a note, and says what was set", async () => {
    serve({ "POST /memo": () => json(200, marked({ status: "draft", tickers: ["AAPL", "MSFT"] })) });
    await run(["drive", "memo", "mark", "_cli/a.txt", "--status", "draft", "--tickers", "aapl, msft", "--note", "start"]);
    expect(body("POST /memo")).toEqual({ path: "_cli/a.txt", memo: true, status: "draft", tickers: ["aapl", "msft"], note: "start" });
    expect(out.join("")).toBe("Marked _cli/a.txt as a memo (status draft, tickers AAPL,MSFT).\n");
  });

  it("marks with nothing else and leaves the fields out of the request", async () => {
    serve({ "POST /memo": () => json(200, marked()) });
    await run(["drive", "memo", "mark", "_cli/a.txt"]);
    expect(body("POST /memo")).toEqual({ path: "_cli/a.txt", memo: true });
    expect(out.join("")).toBe("Marked _cli/a.txt as a memo.\n");
  });

  it("unmarks, and prints the answer raw with --json", async () => {
    serve({ "POST /memo": () => json(200, marked({ marked: false })) });
    await run(["drive", "memo", "unmark", "_cli/a.txt"]);
    expect(body("POST /memo")).toEqual({ path: "_cli/a.txt", memo: false });
    expect(out.join("")).toBe("Unmarked _cli/a.txt; its versions are kept.\n");
    out.length = 0;
    await run(["drive", "memo", "unmark", "_cli/a.txt", "--json"]);
    expect(JSON.parse(out.join("")).memo.marked).toBe(false);
  });

  it("sets final, none and cleared tickers, each as the request says", async () => {
    serve({ "POST /memo": () => json(200, marked({ status: "final", final_seq: 4 })) });
    await run(["drive", "memo", "set", "_cli/a.txt", "--status", "final"]);
    expect(out.join("")).toBe("Status final, pinned at version 4.\n");
    fetchMock.mockClear();
    await run(["drive", "memo", "set", "_cli/a.txt", "--status", "none", "--tickers", ""]);
    expect(body("POST /memo")).toEqual({ path: "_cli/a.txt", status: null, tickers: [] });
    await expect(run(["drive", "memo", "set", "_cli/a.txt"])).rejects.toThrow(/--status or --tickers/);
    await expect(run(["drive", "memo", "set", "_cli/a.txt", "--status", "done"])).rejects.toThrow(/--status takes/);
  });

  it("names memo mark when the file is not a memo", async () => {
    serve({ "POST /memo": () => json(409, { detail: { type: "not_memo", message: "nope" } }) });
    const err = (await run(["drive", "memo", "set", "_cli/a.txt", "--status", "final"]).catch((e) => e)) as CliError;
    expect(err.message).toBe("_cli/a.txt is not a memo. Mark it first: la drive memo mark _cli/a.txt");
    expect(err.code).toBe(Exit.conflict);
  });

  it("lists memos with the pin, a star when revised since, and a note when more remain", async () => {
    const err: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((t) => (err.push(String(t)), true));
    const at = { modified_at: "2026-11-25T10:02:00Z", modified_by: { kind: "agent", agent: "main", thread_id: "t1" } };
    serve({
      "GET /memos": () =>
        json(200, {
          memos: [
            { entry: { ...marked({ status: "final", final_seq: 6, tickers: ["NVDA"] }), ...at, path: "a.md", version: 7 }, versions: 7 },
            { entry: { ...marked({ status: "draft" }), ...at, path: "b.md" }, versions: 2 },
          ],
          next: "c",
        }),
    });
    await run(["drive", "memos", "--status", "final", "--sort", "name", "--limit", "2"]);
    const q = new URL(String(fetchMock.mock.calls[0]![0])).searchParams;
    expect([q.get("status"), q.get("sort"), q.get("order"), q.get("limit")]).toEqual(["final", "name", "asc", "2"]);
    const text = out.join("");
    expect(text).toMatch(/a\.md\s+final v6\*\s+NVDA\s+2026-11-25 10:02\s+agent main\s+t1\s+7/);
    expect(text).toMatch(/b\.md\s+draft\s+-/);
    expect(err.join("")).toContain("More memos than these 2 exist");
  });

  it("reads on past one answer of the server's, a page at a time, up to --limit", async () => {
    const err: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((t) => (err.push(String(t)), true));
    const memo = (path: string) => ({ entry: { ...marked({ status: "draft" }), path, modified_at: "2026-11-25T10:02:00Z", modified_by: null }, versions: 1 });
    serve({
      "GET /memos": (url: URL) =>
        url.searchParams.get("cursor") === "c" ? json(200, { memos: [memo("c.md")], next: null }) : json(200, { memos: [memo("a.md"), memo("b.md")], next: "c" }),
    });
    await run(["drive", "memos", "--limit", "600"]);
    const asked = fetchMock.mock.calls.map((call) => new URL(String(call[0])).searchParams);
    expect(asked.map((q) => [q.get("limit"), q.get("cursor")])).toEqual([["500", null], ["500", "c"]]);
    expect(out.join("")).toMatch(/a\.md[\s\S]*b\.md[\s\S]*c\.md/);
    expect(err.join("")).toBe("");
  });

  it("shows versions and events in one record, and says when older items remain", async () => {
    const err: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((t) => (err.push(String(t)), true));
    const person = { kind: "person", thread_id: null };
    serve({
      "GET /record": () =>
        json(200, {
          next: "x",
          items: [
            { type: "event", kind: "update", created_at: "2026-11-25T10:05:00Z", actor: person, note: "ready", head_seq: 7, status_before: "draft", status_after: "final", tickers_before: [], tickers_after: [], final_seq: 7, cleared: null },
            { type: "version", seq: 7, created_at: "2026-11-25T10:02:00Z", actor: { kind: "agent", agent: "main", thread_id: "t9" }, note: "cut risks", restored_from: null, added: 12, removed: 3 },
            { type: "version", seq: 1, created_at: "2026-11-24T10:02:00Z", actor: person, note: null, restored_from: null, added: null, removed: null },
          ],
        }),
    });
    await run(["drive", "record", "_cli/a.md", "--limit", "3"]);
    const text = out.join("");
    expect(text).toMatch(/--\s+2026-11-25 10:05\s+person\s+-\s+status draft -> final \(pins v7\)\s+ready/);
    expect(text).toMatch(/v7\s+2026-11-25 10:02\s+agent main\s+t9\s+\+12 -3\s+cut risks/);
    expect(err.join("")).toContain("Older items remain");
  });

  it("checks a memo out with a reason, the words after the path", async () => {
    const answer = { change_id: "c1", path: "_cli/a.md", reason: "cut risks", continued: false, versions: 0, first_seq: null, last_seq: null, head_seq: 4, adopted_seq: null };
    serve({ "POST /memo/checkout": () => json(200, answer) });
    await run(["drive", "checkout", "_cli/a.md", "cut", "risks"]);
    expect(body("POST /memo/checkout")).toEqual({ path: "_cli/a.md", reason: "cut risks" });
    expect(out.join("")).toBe('Checked out. Edit the memo where it is; each edit is saved to it under this reason. Later edits that are part of this change need no new check-out, and a different change to the memo gets its own. The check-out ends once this conversation has been quiet for an hour; la drive checkout _cli/a.md --continue then carries the same change on.\n');
    fetchMock.mockClear();
    await run(["drive", "checkout", "_cli/a.md", "--", "- tightened valuation"]);
    expect(body("POST /memo/checkout").reason).toBe("- tightened valuation");
    await expect(run(["drive", "checkout", "_cli/a.md", "cut", "--fast"])).rejects.toThrow(/Unknown option --fast/);
  });

  it("says which save a check-out recorded for", async () => {
    serve({ "POST /memo/checkout": () => json(200, { change_id: "c1", path: "_cli/a.md", reason: "r", continued: false, versions: 1, first_seq: 5, last_seq: 5, head_seq: 5, adopted_seq: 5 }) });
    await run(["drive", "checkout", "_cli/a.md", "r"]);
    expect(out.join("").split("\n")[0]).toBe("v5, the save already made, now has this reason.");
    out.length = 0;
    await run(["drive", "checkout", "_cli/a.md", "r", "--json"]);
    expect(JSON.parse(out.join(""))).toMatchObject({ change_id: "c1", adopted_seq: 5 });
  });

  it("carries on with the earlier change with --continue", async () => {
    serve({ "POST /memo/checkout": () => json(200, { change_id: "c1", path: "_cli/a.md", reason: "cut risks", continued: true, versions: 2, first_seq: 3, last_seq: 4, head_seq: 4, adopted_seq: null }) });
    await run(["drive", "checkout", "_cli/a.md", "--continue"]);
    expect(body("POST /memo/checkout")).toEqual({ path: "_cli/a.md", continue: true });
    expect(out.join("")).toBe('Carrying on: "cut risks". Edits to the memo are saved under it.\n');
  });

  it("says which save --continue recorded for", async () => {
    serve({ "POST /memo/checkout": () => json(200, { change_id: "c1", path: "_cli/a.md", reason: "cut risks", continued: true, versions: 3, first_seq: 3, last_seq: 5, head_seq: 5, adopted_seq: 5 }) });
    await run(["drive", "checkout", "_cli/a.md", "--continue"]);
    expect(out.join("")).toBe('v5, the save already made, now has this reason.\nCarrying on: "cut risks". Edits to the memo are saved under it.\n');
  });

  it("refuses a check-out with both a reason and --continue, or neither, before asking the server", async () => {
    const lead = ["Pass a reason or --continue, not both: ", "Say what you are changing and why, or pass --continue: "];
    for (const [n, argv] of [["drive", "checkout", "_cli/a.md", "r", "--continue"], ["drive", "checkout", "_cli/a.md"]].entries()) {
      const err = (await run(argv!).catch((e) => e)) as CliError;
      expect(err.code).toBe(Exit.usage);
      expect(err.message.startsWith(lead[n]!)).toBe(true);
      expect(err.message).toContain("la drive checkout _cli/a.md '<what you are changing and why>'");
      expect(err.message).toContain("la drive checkout _cli/a.md --continue");
    }
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("quotes a path that a shell would split in the command it suggests", async () => {
    const err = (await run(["drive", "checkout", "notes/q3 $(x)'s.md"]).catch((e) => e)) as CliError;
    expect(err.message).toContain(`la drive checkout 'notes/q3 $(x)'\\''s.md' --continue`);
  });

  it("checks in with and without a reason, and says when no change was made", async () => {
    const change = (first: number | null, last: number | null) => ({ change_id: "c1", path: "_cli/a.md", reason: "r", continued: false, versions: first ? 3 : 0, first_seq: first, last_seq: last, head_seq: last, adopted_seq: null });
    serve({ "POST /memo/checkin": () => json(200, change(5, 7)) });
    await run(["drive", "checkin", "_cli/a.md", "tightened", "the", "case"]);
    expect(body("POST /memo/checkin")).toEqual({ path: "_cli/a.md", reason: "tightened the case" });
    expect(out.join("")).toBe("Checked in: _cli/a.md v5 to v7.\n");
    fetchMock.mockClear();
    await run(["drive", "checkin", "_cli/a.md"]);
    expect(body("POST /memo/checkin")).toEqual({ path: "_cli/a.md" });
    out.length = 0;
    serve({ "POST /memo/checkin": () => json(200, change(2, 2)) });
    await run(["drive", "checkin", "_cli/a.md"]);
    expect(out.join("")).toBe("Checked in: _cli/a.md v2.\n");
    out.length = 0;
    serve({ "POST /memo/checkin": () => json(200, change(null, null)) });
    await run(["drive", "checkin", "_cli/a.md"]);
    expect(out.join("")).toBe("Checked in: _cli/a.md, no change made.\n");
  });

  it("shows the server's sentence when a check-out is refused, with the exit code of its status", async () => {
    serve({ "POST /memo/checkout": () => json(404, { detail: { type: "not_found", message: "You have no earlier change to _cli/a.md." } }) });
    const err = (await run(["drive", "checkout", "_cli/a.md", "--continue"]).catch((e) => e)) as CliError;
    expect([err.message, err.code]).toEqual(["You have no earlier change to _cli/a.md.", Exit.notFound]);
  });

  it("shows a change of three versions as one entry between two ungrouped versions", async () => {
    const who = { kind: "agent", agent: "main", thread_id: "t9" };
    const v = (seq: number, at: string, over = {}) => ({ type: "version", seq, created_at: at, actor: who, note: null, restored_from: null, added: 1, removed: 0, change_id: null, ...over });
    const c = { change_id: "c1", note: "cut risks" };
    serve({
      "GET /record": () => json(200, { items: [v(9, "2026-11-26T09:00:00Z"), v(8, "2026-11-26T08:00:00Z", c), v(7, "2026-11-25T23:00:00Z", { ...c, added: 5, removed: 2 }), v(6, "2026-11-25T22:00:00Z", c), v(5, "2026-11-25T10:00:00Z", { note: "old" })] }),
    });
    await run(["drive", "record", "_cli/a.md"]);
    const lines = out.join("").split("\n");
    expect(lines.filter((l) => l.includes("cut risks"))).toHaveLength(1);
    const group = lines.findIndex((l) => l.includes("cut risks"));
    expect(lines[group]).toMatch(/v6 to v8\s+2026-11-25 22:00 to 2026-11-26 08:00\s+agent main\s+t9\s+\+7 -2\s+cut risks/);
    expect(lines.slice(group + 1, group + 4).map((l) => l.match(/v\d/)![0])).toEqual(["v8", "v7", "v6"]);
    expect(lines[group + 1]).toMatch(/^\s+v8/);
    expect(lines.findIndex((l) => /^v9\s/.test(l))).toBeLessThan(group);
    expect(lines.findIndex((l) => /^v5\s/.test(l))).toBeGreaterThan(group + 3);
    out.length = 0;
    await run(["drive", "record", "_cli/a.md", "--json"]);
    expect(JSON.parse(out.join("")).items).toHaveLength(5);
  });

  describe("a page cut inside a change", () => {
    const who = { kind: "agent", agent: "main", thread_id: "t9" };
    const v = (seq: number, change_id: string | null, over = {}) => ({ type: "version", seq, created_at: "2026-11-26T08:00:00Z", actor: who, note: "cut risks", restored_from: null, added: 2, removed: 1, change_id, ...over });
    const asked = () => fetchMock.mock.calls.map((call) => new URL(String(call[0])).searchParams).map((q) => [q.get("limit"), q.get("before")]);
    const stderr = () => {
      const err: string[] = [];
      vi.spyOn(process.stderr, "write").mockImplementation((t) => (err.push(String(t)), true));
      return err;
    };

    it("reads on to the change's first version and totals all of it", async () => {
      const err = stderr();
      serve({ "GET /record": (url: URL) => (url.searchParams.get("before") === "a" ? json(200, { items: [v(6, "c1", { added: 5 }), v(5, null)], next: "b" }) : json(200, { items: [v(8, "c1"), v(7, "c1")], next: "a" })) });
      await run(["drive", "record", "_cli/a.md", "--limit", "2"]);
      expect(asked()).toEqual([["2", null], ["200", "a"]]);
      const lines = out.join("").split("\n");
      expect(lines.find((l) => l.includes("cut risks"))).toMatch(/v6 to v8\s.*\+9 -3\s+cut risks/);
      expect(lines.filter((l) => /^\s+v\d/.test(l))).toHaveLength(3);
      expect(lines.some((l) => /^v5\s/.test(l))).toBe(false);
      expect(err.join("")).toContain("Older items remain");
    });

    it("says nothing older remains when the change reaches the record's start", async () => {
      const err = stderr();
      serve({ "GET /record": (url: URL) => (url.searchParams.get("before") === "a" ? json(200, { items: [v(6, "c1")], next: null }) : json(200, { items: [v(8, "c1"), v(7, "c1")], next: "a" })) });
      await run(["drive", "record", "_cli/a.md", "--limit", "2"]);
      expect(out.join("")).toMatch(/v6 to v8\s.*\+6 -3/);
      expect(err.join("")).toBe("");
    });

    it("shows nothing of the next page when it starts another change", async () => {
      const err = stderr();
      serve({ "GET /record": (url: URL) => (url.searchParams.get("before") === "a" ? json(200, { items: [v(6, "c0"), v(5, "c0")], next: null }) : json(200, { items: [v(8, "c2"), v(7, "c1")], next: "a" })) });
      await run(["drive", "record", "_cli/a.md", "--limit", "2"]);
      expect(asked()).toEqual([["2", null], ["200", "a"]]);
      const text = out.join("");
      expect(text).toMatch(/^v7\s/m);
      expect(text).not.toMatch(/v[56]\b/);
      expect(err.join("")).toContain("Older items remain");
    });

    it("gives no total for a change it stopped reading inside", async () => {
      const err = stderr();
      // Every older page is 200 more of the same change with another behind it, so the read stops at its bound.
      let seq = 5000;
      serve({ "GET /record": (url: URL) => (url.searchParams.get("before") ? json(200, { items: Array.from({ length: 200 }, () => v(seq--, "c1")), next: "more" }) : json(200, { items: [v(5002, "c1"), v(5001, "c1")], next: "a" })) });
      await run(["drive", "record", "_cli/a.md", "--limit", "2"]);
      expect(asked()).toHaveLength(6);
      const lines = out.join("").split("\n");
      expect(lines.find((l) => l.includes("v4001 to v5002"))).toMatch(/t9\s+-\s+cut risks/);
      expect(lines.find((l) => /^\s+v5002/.test(l))).toMatch(/\+2 -1/);
      expect(err.join("")).toContain("Older items remain");
    });

    it("reads nothing more when the page ends on a version outside any change, or an event", async () => {
      const ev = { type: "event", kind: "update", created_at: "2026-11-26T08:00:00Z", actor: who, note: null, status_before: null, status_after: "draft", tickers_before: [], tickers_after: [], final_seq: null, cleared: null };
      for (const last of [v(7, null), ev]) {
        fetchMock.mockClear();
        serve({ "GET /record": () => json(200, { items: [v(8, "c1"), last], next: "a" }) });
        await run(["drive", "record", "_cli/a.md", "--limit", "2"]);
        expect(asked()).toEqual([["2", null]]);
      }
    });

    it("leaves --json as the server's items up to --limit, with its cursor", async () => {
      serve({ "GET /record": () => json(200, { items: [v(8, "c1"), v(7, "c1")], next: "a" }) });
      await run(["drive", "record", "_cli/a.md", "--limit", "2", "--json"]);
      expect(asked()).toEqual([["2", null]]);
      expect(JSON.parse(out.join(""))).toMatchObject({ next: "a", items: [{ seq: 8 }, { seq: 7 }] });
    });
  });

  it("totals a change only when every version under it has its counts", async () => {
    const who = { kind: "agent", agent: "main", thread_id: "t9" };
    const v = (seq: number, over = {}) => ({ type: "version", seq, created_at: "2026-11-26T08:00:00Z", actor: who, note: "cut risks", restored_from: null, added: 4, removed: 1, change_id: "c1", ...over });
    serve({ "GET /record": () => json(200, { items: [v(8), v(7, { added: null, removed: null }), v(6)] }) });
    await run(["drive", "record", "_cli/a.md"]);
    const lines = out.join("").split("\n");
    expect(lines.find((l) => l.includes("v6 to v8"))).toMatch(/t9\s+-\s+cut risks/);
    expect(lines.find((l) => /^\s+v8/.test(l))).toMatch(/\+4 -1/);
  });

  it("says a restore on the version that is one, under the change's reason", async () => {
    const who = { kind: "agent", agent: "main", thread_id: "t9" };
    const v = (seq: number, over = {}) => ({ type: "version", seq, created_at: "2026-11-26T08:00:00Z", actor: who, note: "cut risks", restored_from: null, added: 4, removed: 1, change_id: "c1", ...over });
    serve({ "GET /record": () => json(200, { items: [v(8, { restored_from: 3 }), v(7, { restored_from: 2 }), v(6)] }) });
    await run(["drive", "record", "_cli/a.md"]);
    const lines = out.join("").split("\n");
    expect(lines.find((l) => l.includes("v6 to v8"))).toMatch(/cut risks\s*$/);
    expect(lines.find((l) => l.includes("v6 to v8"))).not.toMatch(/restored/);
    expect(lines.find((l) => /^\s+v8/.test(l))).toMatch(/restored from v3\s*$/);
    expect(lines.find((l) => /^\s+v7/.test(l))).toMatch(/restored from v2\s*$/);
    expect(lines.find((l) => /^\s+v6/.test(l))).not.toMatch(/restored|cut risks/);
  });

  it("names the pinned version when a memo already Final is set Final again", async () => {
    const who = { kind: "agent", agent: "main", thread_id: "t9" };
    const e = { type: "event", id: "e1", kind: "update", created_at: "2026-11-26T08:00:00Z", actor: who, note: null, status_before: "final", status_after: "final", tickers_before: [], tickers_after: [], final_seq: 3 };
    serve({ "GET /record": () => json(200, { items: [e] }) });
    await run(["drive", "record", "_cli/a.md"]);
    expect(out.join("")).toMatch(/pins v3/);
    expect(out.join("")).not.toMatch(/->/);
  });

  it("shows a child version's agent and thread only where they differ from the change's", async () => {
    const main = { kind: "agent", agent: "main", thread_id: "t9" };
    const sub = { kind: "agent", agent: "research", thread_id: "t7" };
    const v = (seq: number, actor: object) => ({ type: "version", seq, created_at: "2026-11-26T08:00:00Z", actor, note: "cut risks", restored_from: null, added: 1, removed: 0, change_id: "c1" });
    serve({ "GET /record": () => json(200, { items: [v(8, main), v(7, sub), v(6, main)] }) });
    await run(["drive", "record", "_cli/a.md"]);
    const lines = out.join("").split("\n");
    expect(lines.find((l) => /^\s+v7/.test(l))).toMatch(/agent research\s+t7/);
    expect(lines.find((l) => /^\s+v8/.test(l))).not.toMatch(/agent|t9/);
    expect(lines.find((l) => /^\s+v6/.test(l))).not.toMatch(/agent|t9/);
  });

  it("words the server's refusal to unmark a file that is not a memo, with the refusal exit code", async () => {
    serve({ "POST /memo": () => json(409, { detail: { code: "not_memo", message: "x" } }) });
    const err = (await run(["drive", "memo", "unmark", "_cli/a.txt"]).catch((e) => e)) as CliError;
    expect(err.message).toBe("_cli/a.txt is not a memo.");
    expect(err.code).toBe(Exit.conflict);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("cuts a note to 500 characters and says so on stderr, for checkout and memo mark", async () => {
    const err: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((t) => (err.push(String(t)), true));
    const long = "x".repeat(600);
    serve({ "POST /memo/checkout": () => json(200, { reason: "x", adopted_seq: null }), "POST /memo": () => json(200, marked()) });
    await run(["drive", "checkout", "_cli/a.md", long]);
    expect(body("POST /memo/checkout").reason).toBe("x".repeat(500));
    await run(["drive", "memo", "mark", "_cli/a.md", "--note", long]);
    expect(body("POST /memo").note).toBe("x".repeat(500));
    expect(err.filter((l) => l.includes("cut to fit"))).toHaveLength(2);
    err.length = 0;
    await run(["drive", "checkout", "_cli/a.md", "short"]);
    expect(err).toEqual([]);
  });

  it("undoes by path and prints the new version", async () => {
    serve({ "POST /undo": () => json(200, { ...entry, version: 5 }) });
    await run(["drive", "undo", "_cli/a.txt", "--note", "wrong"]);
    expect(body("POST /undo")).toEqual({ path: "_cli/a.txt", note: "wrong" });
    expect(out.join("")).toBe("Put back _cli/a.txt as it was before its last save (now version 5).\n");
  });

  it("prints the server's checkout_required message as it comes on put, restore and undo", async () => {
    const line = '_cli/a.txt was not saved: it is a memo, which you check out before changing. Run la drive checkout _cli/a.txt \'<what you are changing and why>\', then make this call again.';
    const refuse = () => json(409, { detail: { type: "checkout_required", message: line } });
    const file = localFile("a.txt", "hi");
    serve({ "GET /spaces": usage(1000), "GET /spaces/": usage(1000), "POST /uploads": refuse, "GET /entry": () => json(200, entry), "POST /versions/restore": refuse, "POST /undo": refuse });
    for (const argv of [["drive", "put", file, "_cli/a.txt"], ["drive", "restore", "_cli/a.txt", "--rev", "1"], ["drive", "undo", "_cli/a.txt"]]) {
      const err = (await run(argv).catch((e) => e)) as CliError;
      expect(err.message).toBe(line);
      expect(err.code).toBe(Exit.conflict);
    }
  });

  it("lists every version of a memo in history and points to the record, with no clear hint", async () => {
    const who = { kind: "person", thread_id: null };
    const v = (seq: number) => ({ seq, size: 2048, created_at: "2026-11-20T10:00:00Z", actor: who, note: null, restored_from: null, expires_at: null });
    serve({
      "GET /versions": () => json(200, { memo: marked().memo, versions: [v(3), v(2), v(1)], history_bytes: 4096 }),
    });
    await run(["drive", "history", "_cli/m.md"]);
    const text = out.join("");
    expect(text.split("\n").filter((l) => /^[123]\s/.test(l))).toHaveLength(3);
    expect(text).toContain("Who changed what and why: la drive record _cli/m.md");
    expect(text).not.toContain("keeps one earlier copy");
    expect(text).not.toContain("history clear");
  });

  it("adds a memo column to ls only when a row is a memo", async () => {
    serve({ "GET /entries": () => json(200, { entries: [{ ...entry, modified_at: "2026-11-25T10:02:00Z", modified_by: null, name: "a.txt" }] }) });
    await run(["drive", "ls"]);
    expect(out.join("")).not.toMatch(/MEMO/);
    out.length = 0;
    serve({ "GET /entries": () => json(200, { entries: [{ ...entry, modified_at: "2026-11-25T10:02:00Z", modified_by: null, name: "a.txt", memo: { marked: true, status: "final", tickers: [], final_seq: 1 } }] }) });
    await run(["drive", "ls"]);
    expect(out.join("")).toMatch(/MEMO[\s\S]*final/);
  });

  describe("a path in the file mount's form", () => {
    const answer = { change_id: "c1", path: "notes/a.md", reason: "r", continued: false, versions: 0, first_seq: null, last_seq: null, head_seq: 1, adopted_seq: null };
    const sentTo = async (argv: string[]) => {
      fetchMock.mockClear();
      serve({ "POST /memo/checkout": () => json(200, answer) });
      await run(argv);
      return { url: String(sent("POST /memo/checkout")![0]), body: body("POST /memo/checkout") };
    };

    it("resolves relative and absolute to the path in that space", async () => {
      for (const typed of [".agents/drive/personal/notes/a.md", "/home/x/ws/.agents/drive/personal/notes/a.md"]) {
        const got = await sentTo(["drive", "checkout", typed, "r"]);
        expect(got.body.path).toBe("notes/a.md");
        expect(got.url).toContain("/drive/personal/memo/checkout");
      }
      expect((await sentTo(["drive", "checkout", ".agents/drive/team/x.md", "r"])).url).toContain("/drive/team/memo/checkout");
    });

    it("resolves the second operand of put and both of mv", async () => {
      serve({ "GET /spaces": usage(1000), "GET /spaces/": usage(1000), "POST /uploads": () => json(200, { mode: "held", entry: { ...entry, path: "notes/a.md", version: 2 } }) });
      await run(["drive", "put", localFile("a.md", "hi"), ".agents/drive/personal/notes/a.md"]);
      expect(String(sent("POST /uploads")![0])).toContain("/drive/personal/");
      expect(body("POST /uploads").path).toBe("notes/a.md");
      fetchMock.mockClear();
      serve({ "POST /move": () => json(200, { ...entry, path: "b/c.md" }) });
      await run(["drive", "mv", "/w/.agents/drive/team/a.md", ".agents/drive/team/b/c.md"]);
      expect(body("POST /move")).toEqual({ from: "a.md", to: "b/c.md" });
      expect(String(sent("POST /move")![0])).toContain("/drive/team/");
    });

    it("reads a space's folder in the mount, with its slash, as the space's root", async () => {
      fetchMock.mockClear();
      serve({ "GET /entries": () => json(200, { entries: [] }) });
      await run(["drive", "ls", "/home/x/ws/.agents/drive/personal/"]);
      const url = new URL(String(sent("GET /entries")![0]));
      expect([url.pathname.includes("/drive/personal/"), url.searchParams.get("path")]).toEqual([true, ""]);
    });

    it("resolves a path that starts as a filesystem one, and leaves a Drive path that holds the mount's folders as typed", async () => {
      for (const typed of ["./.agents/drive/personal/notes/a.md", "./ws/.agents/drive/personal/notes/a.md", "../ws/.agents/drive/personal/notes/a.md", "~/ws/.agents/drive/personal/notes/a.md"]) {
        const got = await sentTo(["drive", "checkout", typed, "r"]);
        expect([typed, got.body.path]).toEqual([typed, "notes/a.md"]);
        expect(got.url).toContain("/drive/personal/memo/checkout");
      }
      // A folder in the Drive can hold the same names; the command acts on the file named, in the space given.
      const kept = "backups/.agents/drive/team/report.md";
      const got = await sentTo(["drive", "checkout", kept, "r"]);
      expect(got.body.path).toBe(kept);
      expect(got.url).not.toContain("/drive/team/");
      fetchMock.mockClear();
      serve({ "POST /trash": () => json(200, { trash_id: "t1", entries: 1 }) });
      await run(["drive", "rm", kept]);
      expect(body("POST /trash").path).toBe(kept);
      expect(String(sent("POST /trash")![0])).not.toContain("/drive/team/");
    });

    it("leaves an ordinary path, a folder named .agents, and a disagreeing --space as typed", async () => {
      expect((await sentTo(["drive", "checkout", "notes/a.md", "r"])).body.path).toBe("notes/a.md");
      expect((await sentTo(["drive", "checkout", ".agents/notes/a.md", "r"])).body.path).toBe(".agents/notes/a.md");
      expect((await sentTo(["drive", "checkout", ".agents/drive/a.md", "r"])).body.path).toBe(".agents/drive/a.md");
      const got = await sentTo(["drive", "checkout", ".agents/drive/personal/notes/a.md", "r", "--space", "s1"]);
      expect([got.body.path, got.url]).toEqual([".agents/drive/personal/notes/a.md", expect.stringContaining("/drive/s1/")]);
    });
  });
});
