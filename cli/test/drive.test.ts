import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { Args } from "../src/args.js";
import { drive } from "../src/commands/drive.js";
import { buffersFor, getFile, pool, writeAll } from "../src/commands/driveTransfer.js";
import { bind, command, parseFor, select } from "../src/commands/command.js";
import { Api } from "../src/http.js";
import { COMMANDS } from "../src/commands/index.js";
import { CliError, Exit } from "../src/errors.js";
import { credential, drivePath, entry, fetchMock, type Handler, json, localFile, out, run, sent, serve, tempDir, usage, useDriveHarness } from "./driveHarness.js";

// Bytes go by node:http in the CLI; here they are answered by the same stub as the rest.
vi.mock("../src/rawHttp.js", () => ({
  rawResponse: (url: URL, init: { method: string; headers: Record<string, string>; body?: Uint8Array }) =>
    globalThis.fetch(url, { method: init.method, headers: init.headers, body: init.body as BodyInit | undefined }),
  rawRequest: () => {
    throw new Error("a signed download is not part of these tests");
  },
}));

useDriveHarness();

const find = (...words: string[]) => drive.find((c) => c.words.join(" ") === words.join(" "))!;


describe("drive arguments", () => {
  it("takes ls with or without a path, and trash restore ahead of trash", () => {
    expect(select(COMMANDS, ["drive", "ls"]).command).toBe(find("drive", "ls"));
    expect(select(COMMANDS, ["drive", "trash", "restore", "x"]).command).toBe(find("drive", "trash", "restore"));
    expect(select(COMMANDS, ["drive", "trash"]).command).toBe(find("drive", "trash"));
    const ls = find("drive", "ls");
    expect(bind(ls, parseFor(ls, [])).opt("path")).toBeUndefined();
    expect(() => bind(ls, parseFor(ls, ["a", "b"]))).toThrow(/Unexpected argument/);
  });

  it("reads --rev as the file's version and leaves --version to the CLI", async () => {
    fetchMock
      .mockResolvedValueOnce(json(200, entry))
      .mockResolvedValueOnce(json(200, { versions: [] }))
      .mockResolvedValueOnce(new Response("old", { status: 200 }));
    await run(["drive", "cat", "_cli/a.txt", "--rev", "2"]);
    expect(out.join("")).toBe("old");
    const url = new URL(String(fetchMock.mock.calls[2]![0]));
    expect(url.pathname).toBe("/api/v1/drive/personal/content");
    expect(url.searchParams.get("seq")).toBe("2");
    // Ahead of the words, or on a command, --version is still the global flag.
    const ls = find("drive", "ls");
    expect(parseFor(ls, ["--version"]).values.get("version")).toBe(true);
    expect(select(COMMANDS, ["--version", "drive", "ls"]).command).toBe(ls);
  });
});


describe("drive put", () => {
  it("sends a small file in one request, with the note and the preconditions", async () => {
    const file = localFile("x.bin", Buffer.from([0, 1, 2, 255]));
    serve({
      "GET /spaces": usage(1e9),
      "POST /uploads": () => json(200, { mode: "single", max_bytes: 50e6 }),
      "PUT /content": () => json(200, { ...entry, size: 4 }),
    });
    await run(["drive", "put", file, "_cli/x.bin", "--note", "why", "--if-match", "abc", "--space", "s1"]);
    const begin = JSON.parse(sent("POST /uploads")![1].body);
    expect(begin).toMatchObject({ path: "_cli/x.bin", size: 4, note: "why", if_match: "abc" });
    expect(begin.sha256).toMatch(/^[0-9a-f]{64}$/);
    const [url, init] = sent("PUT /content")!;
    const u = new URL(String(url));
    expect(u.searchParams.get("path")).toBe("_cli/x.bin");
    expect(u.searchParams.get("note")).toBe("why");
    expect(init.headers["content-type"]).toBe("application/octet-stream");
    expect(init.headers["if-match"]).toBe('"abc"');
    expect([...(init.body as Uint8Array)]).toEqual([0, 1, 2, 255]);
  });

  it("asks for If-None-Match for --new, and refuses it beside --if-match", async () => {
    const file = localFile("x.txt", "hi");
    serve({
      "GET /spaces": usage(1e9),
      "POST /uploads": () => json(200, { mode: "single" }),
      "PUT /content": () => json(200, entry),
    });
    await run(["drive", "put", file, "x.txt", "--new"]);
    expect(JSON.parse(sent("POST /uploads")![1].body).if_none_match).toBe(true);
    expect(sent("PUT /content")![1].headers["if-none-match"]).toBe("*");
    await expect(run(["drive", "put", file, "x.txt", "--new", "--if-match", "abc"])).rejects.toMatchObject({ code: Exit.usage });
  });

  it("answers bytes the Drive already holds without sending any", async () => {
    const file = localFile("x.txt", "hi");
    serve({
      "GET /spaces": usage(1e9),
      "POST /uploads": () => json(200, { mode: "held", entry }),
    });
    await run(["drive", "put", file, "x.txt"]);
    expect(out.join("")).toContain("Saved _cli/a.txt");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("explains a changed file, with the conflict exit code", async () => {
    const file = localFile("x.txt", "hi");
    serve({
      "GET /spaces": usage(1e9),
      "POST /uploads": () => json(412, { detail: { type: "changed", message: "server words" } }),
    });
    const err = (await run(["drive", "put", file, "x.txt", "--if-match", "old"]).catch((e) => e)) as CliError;
    expect(err.code).toBe(Exit.conflict);
    expect(err.message).toContain("la drive history");
  });

  it("refuses a file over the allowance before reading or sending a byte", async () => {
    const file = localFile("x.bin", Buffer.alloc(2048));
    serve({ "GET /spaces": usage(1024) });
    const err = (await run(["drive", "put", file, "x.bin", "--space", "s1"]).catch((e) => e)) as CliError;
    expect(err.message).toContain("1.0 KB");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

describe("drive put in parts", () => {
  const PART = 16 * 1024 * 1024;
  const begun = (mode: "relay" | "direct", held: number[]) => () =>
    json(200, {
      mode,
      upload_id: "u1",
      part_size: PART,
      part_count: 3,
      parts_held: held.map((part_number) => ({ part_number })),
      resumed: held.length > 0,
    });
  const big = () => localFile("big.bin", Buffer.alloc(2 * PART + 5, 7));

  it("sends only the parts the store lacks, each with its MD5, then waits for the check", async () => {
    const parts: Record<string, string> = {};
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": begun("relay", [2]),
      "PUT /uploads/u1/parts/1": (_u, i) => ((parts["1"] = i.headers["content-md5"]!), json(200, {})),
      "PUT /uploads/u1/parts/3": (_u, i) => ((parts["3"] = i.headers["content-md5"]!), json(200, {})),
      "POST /uploads/u1/complete": () => json(202, { state: "verifying" }),
      "GET /uploads/u1": () => json(200, { state: "done", entry }),
    });
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await run(["drive", "put", big(), "_cli/big.bin"]);
    expect(Object.keys(parts).sort()).toEqual(["1", "3"]);
    expect(parts["3"]).toBe(createHash("md5").update(Buffer.alloc(5, 7)).digest("base64"));
    expect(err.mock.calls.join("")).toContain("1 of 3 parts are already in the Drive");
  });

  it("sends a part through the relay when the store cannot be reached, without printing the address", async () => {
    const relayed: string[] = [];
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": begun("direct", []),
      "POST /uploads/u1/parts": (_u, i) =>
        json(200, {
          parts: JSON.parse(String(i.body)).parts.map((p: { part_number: number }) => ({
            part_number: p.part_number,
            url: "https://store.test/secret-signature",
            headers: {},
          })),
        }),
      "PUT /uploads/u1/parts/1": () => (relayed.push("1"), json(200, {})),
      "PUT /uploads/u1/parts/2": () => (relayed.push("2"), json(200, {})),
      "PUT /uploads/u1/parts/3": () => (relayed.push("3"), json(200, {})),
      "POST /uploads/u1/complete": () => json(202, { state: "verifying" }),
      "GET /uploads/u1": () => json(200, { state: "done", entry }),
    });
    const real = fetchMock.getMockImplementation()!;
    fetchMock.mockImplementation(async (url: URL, init) => {
      if (String(url).startsWith("https://store.test")) throw new TypeError("fetch failed");
      return real(url, init);
    });
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await run(["drive", "put", big(), "_cli/big.bin"]);
    expect(relayed.sort()).toEqual(["1", "2", "3"]);
    expect(out.join("") + err.mock.calls.join("")).not.toContain("secret-signature");
  });

  it("sends parts the server reports missing to the store first, retrying a busy relay", async () => {
    const signedPuts: string[] = [];
    let completes = 0;
    let relayTries = 0;
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": begun("direct", [1, 3]),
      "POST /uploads/u1/parts": (_u, i) =>
        json(200, {
          parts: JSON.parse(String(i.body)).parts.map((p: { part_number: number }) => ({
            part_number: p.part_number,
            url: `https://store.test/part-${p.part_number}`,
            headers: {},
          })),
        }),
      "PUT /uploads/u1/parts/2": () => (++relayTries === 1 ? json(503, {}) : json(200, {})),
      "POST /uploads/u1/complete": () =>
        ++completes === 1
          ? json(409, { detail: { type: "parts_missing", message: "x", missing_parts: [2] } })
          : json(202, { state: "verifying" }),
      "GET /uploads/u1": () => json(200, { state: "done", entry }),
    });
    const real = fetchMock.getMockImplementation()!;
    fetchMock.mockImplementation(async (url: URL, init) => {
      const at = String(url);
      if (!at.startsWith("https://store.test")) return real(url, init);
      signedPuts.push(at.slice(-6));
      // The store takes part 2 the first time and refuses it when it is sent again.
      return new Response(null, { status: signedPuts.length === 1 ? 200 : 500 });
    });
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await run(["drive", "put", big(), "_cli/big.bin"]);
    // Once on the first pass, then again for the server's missing part, before the relay.
    expect(signedPuts.filter((p) => p === "part-2").length).toBeGreaterThanOrEqual(2);
    expect(relayTries).toBe(2);
    expect(completes).toBe(2);
  });

  /** A direct upload of ``count`` small parts whose store answers by ``store``; signatures carry the round they were made in. */
  function directSmall(count: number, store: (part: number, round: number) => number, routes: Record<string, Handler> = {}) {
    const size = 1024;
    const signed: number[][] = [];
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": () =>
        json(200, { mode: "direct", upload_id: "u1", part_size: size, part_count: count, parts_held: [], resumed: false }),
      "POST /uploads/u1/parts": (_u, i) => {
        const asked = JSON.parse(String(i.body)).parts.map((p: { part_number: number }) => p.part_number) as number[];
        signed.push(asked);
        const round = signed.length;
        return json(200, { parts: asked.map((n) => ({ part_number: n, url: `https://store.test/p${n}/r${round}`, headers: {} })) });
      },
      "GET /uploads/u1": () => json(200, { state: "done", entry }),
      ...routes,
    });
    const real = fetchMock.getMockImplementation()!;
    const puts: Array<[number, number]> = [];
    fetchMock.mockImplementation(async (url: URL, init) => {
      const m = /p(\d+)\/r(\d+)$/.exec(String(url));
      if (!m) return real(url, init);
      puts.push([Number(m[1]), Number(m[2])]);
      return new Response(null, { status: store(Number(m[1]), Number(m[2])) });
    });
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    return { file: localFile("parts.bin", Buffer.alloc((count - 1) * size + 5, 3)), signed, puts };
  }

  it("signs each part once, in batches, ahead of the parts still to go", async () => {
    const { file, signed } = directSmall(20, () => 200, {
      "POST /uploads/u1/complete": () => json(202, { state: "verifying" }),
    });
    await run(["drive", "put", file, "_cli/parts.bin"]);
    const all = signed.flat();
    expect(all.sort((a, b) => a - b)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
    expect(signed.length).toBeLessThanOrEqual(4);
  });

  it("signs a part the server reports missing afresh, rather than trying an old address first", async () => {
    let completes = 0;
    let signedBeforeComplete = 0;
    const { file, signed, puts } = directSmall(12, () => 200, {
      "POST /uploads/u1/complete": () => {
        if (++completes > 1) return json(202, { state: "verifying" });
        signedBeforeComplete = signed.length;
        return json(409, { detail: { type: "parts_missing", message: "x", missing_parts: [2] } });
      },
    });
    await run(["drive", "put", file, "_cli/parts.bin"]);
    const again = puts.filter(([n], i) => n === 2 && i >= 12);
    // The missing part went once, on an address signed after the server asked for it.
    expect(again).toHaveLength(1);
    expect(again[0]![1]).toBeGreaterThan(signedBeforeComplete);
    expect(signed.at(-1)).toEqual([2]);
  });

  it("sends what is missing and completes again when a poll finds the upload reopened", async () => {
    let completes = 0;
    let polls = 0;
    const relayed: number[] = [];
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": begun("relay", [1, 3]),
      "PUT /uploads/u1/parts/2": () => (relayed.push(2), json(200, {})),
      "POST /uploads/u1/complete": () => (completes++, json(202, { state: "verifying" })),
      // Another sender's complete found a part missing and reopened it, before this one's check.
      "GET /uploads/u1": () =>
        ++polls === 1
          ? json(200, { state: "uploading", parts_held: [{ part_number: 1 }, { part_number: 3 }] })
          : json(200, { state: "done", entry }),
    });
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await run(["drive", "put", big(), "_cli/big.bin"]);
    expect(relayed).toEqual([2, 2]);
    expect(completes).toBe(2);
  });

  it("follows the upload to its end when another sender completed it first", async () => {
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": begun("relay", [1, 2]),
      "PUT /uploads/u1/parts/3": () => json(409, { detail: { type: "upload_ended", message: "x" } }),
      "GET /uploads/u1": () => json(200, { state: "done", entry }),
    });
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await run(["drive", "put", big(), "_cli/big.bin"]);
    expect(out.join("")).toContain("Saved _cli/a.txt");
  });

  it("sends nothing for an earlier upload of the same file already being checked, and waits for it", async () => {
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": () =>
        // As a status reads while it is checked: no parts listed.
        json(200, { mode: "relay", state: "verifying", upload_id: "u1", part_size: PART, part_count: 3, resumed: true }),
      "GET /uploads/u1": () => json(200, { state: "done", entry }),
    });
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await run(["drive", "put", big(), "_cli/big.bin"]);
    expect(out.join("")).toContain("Saved _cli/a.txt");
    expect(fetchMock.mock.calls.map(([u, i]) => `${i?.method ?? "GET"} ${drivePath(new URL(String(u)))}`)).toEqual([
      "GET /spaces", "POST /uploads", "GET /uploads/u1",
    ]);
    expect(err.mock.calls.join("")).toContain("already being checked");
  });

  it("names a failed check in plain words", async () => {
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": begun("relay", [1, 2, 3]),
      "POST /uploads/u1/complete": () => json(202, { state: "verifying" }),
      "GET /uploads/u1": () => json(200, { state: "failed", error: { type: "mismatch", message: "x", status: 422 } }),
    });
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const err = (await run(["drive", "put", big(), "_cli/big.bin"]).catch((e) => e)) as CliError;
    expect(err.message).toContain("did not match the file's hash");
  });

  it.each([
    ["checkout_required", 409, Exit.conflict],
    ["changed", 412, Exit.conflict],
    ["over_quota", 507, Exit.error],
  ])("exits for an upload that failed with %s as for that refusal answered at once", async (type, status, code) => {
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": begun("relay", [1, 2, 3]),
      "POST /uploads/u1/complete": () => json(202, { state: "verifying" }),
      "GET /uploads/u1": () => json(200, { state: "failed", error: { type, message: "The server's sentence", status } }),
    });
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const err = (await run(["drive", "put", big(), "_cli/big.bin"]).catch((e) => e)) as CliError;
    expect(err.code).toBe(code);
    expect(err.status).toBe(status);
  });
});

describe("the part buffers of one upload", () => {
  const MIB = 1024 * 1024;

  it("go four at once at the usual part size and fewer as the part grows", () => {
    expect(buffersFor(16 * MIB, 100)).toBe(4);
    expect(buffersFor(64 * MIB, 100)).toBe(4);
    // A 1 TiB file's part, which four buffers would hold 420 MiB of.
    expect(buffersFor(105 * MIB, 100)).toBe(2);
    expect(buffersFor(512 * MIB, 100)).toBe(1);
  });

  it("are never more than the parts left to send, and never none", () => {
    expect(buffersFor(16 * MIB, 2)).toBe(2);
    expect(buffersFor(16 * MIB, 0)).toBe(1);
  });
});

describe("drive get and cat", () => {
  const bytes = Buffer.from([9, 8, 7]);
  const link = (extra = {}) => ({
    url: "/api/v1/drive/personal/content?id=e1&seq=1&download=true",
    signed: false,
    size: 3,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    name: "a.bin",
    ...extra,
  });

  it("downloads by range to a temporary file, verifies it, and refuses to overwrite without --force", async () => {
    const dest = join(tempDir(), "out.bin");
    serve({
      "GET /entry": () => json(200, { ...entry, is_text: false }),
      "GET /content/link": () => json(200, link()),
      "GET /content": (_u, i) => {
        expect(i.headers.range).toBe("bytes=0-2");
        return new Response(bytes, { status: 206 });
      },
    });
    await run(["drive", "get", "_cli/a.bin", "-o", dest]);
    expect([...readFileSync(dest)]).toEqual([9, 8, 7]);
    expect(existsSync(`${dirname(dest)}/.out.bin.la-part`)).toBe(false);
    const err = (await run(["drive", "get", "_cli/a.bin", "-o", dest]).catch((e) => e)) as CliError;
    expect(err.code).toBe(Exit.conflict);
    await run(["drive", "get", "_cli/a.bin", "-o", dest, "--force"]);
  });

  it("keeps nothing when the bytes do not match the hash the link names", async () => {
    const dest = join(tempDir(), "out.bin");
    serve({
      "GET /entry": () => json(200, entry),
      "GET /content/link": () => json(200, link({ sha256: "0".repeat(64) })),
      "GET /content": () => new Response(bytes, { status: 206 }),
    });
    const err = (await run(["drive", "get", "_cli/a.bin", "-o", dest]).catch((e) => e)) as CliError;
    expect(err.message).toContain("did not match");
    expect(existsSync(dest)).toBe(false);
    expect(existsSync(`${dirname(dest)}/.out.bin.la-part`)).toBe(false);
  });

  it("refuses a binary file and points at get", async () => {
    fetchMock.mockResolvedValueOnce(json(200, { ...entry, is_text: false }));
    const err = (await run(["drive", "cat", "_cli/a.bin"]).catch((e) => e)) as CliError;
    expect(err.message).toContain("la drive get _cli/a.txt");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

describe("a file edited while it is being put", () => {
  /** Rewrites the file, same size, as an editor would between the hash and what the server answers. */
  const edit = (file: string) => () => {
    writeFileSync(file, Buffer.alloc(readFileSync(file).length, 0x5a));
    const later = new Date(Date.now() + 5000);
    utimesSync(file, later, later);
  };
  const changed = /changed while it was being sent; run the command again/;

  it("is not recorded as the old bytes when the Drive already holds them", async () => {
    const file = localFile("x.txt", "hello");
    serve({
      "GET /spaces": usage(1e9),
      "POST /uploads": () => (edit(file)(), json(200, { mode: "held", entry })),
    });
    const err = (await run(["drive", "put", file, "x.txt"]).catch((e) => e)) as CliError;
    expect(err.message).toMatch(changed);
  });

  it("is not sent in one request once it differs from what was announced", async () => {
    const file = localFile("x.txt", "hello");
    serve({
      "GET /spaces": usage(1e9),
      "POST /uploads": () => (edit(file)(), json(200, { mode: "single" })),
      "PUT /content": () => json(200, entry),
    });
    const err = (await run(["drive", "put", file, "x.txt"]).catch((e) => e)) as CliError;
    expect(err.message).toMatch(changed);
    expect(sent("PUT /content")).toBeUndefined();
  });

  it("sends no part of it, and says so, when it differs from what was announced", async () => {
    const PART = 16 * 1024 * 1024;
    const file = localFile("big.bin", Buffer.alloc(2 * PART + 5, 7));
    serve({
      "GET /spaces": usage(1e12),
      "POST /uploads": () => (
        edit(file)(),
        json(200, { mode: "relay", upload_id: "u1", part_size: PART, part_count: 3, parts_held: [], resumed: false })
      ),
      "PUT /uploads/u1/parts/1": () => json(200, {}),
    });
    const err = (await run(["drive", "put", file, "_cli/big.bin"]).catch((e) => e)) as CliError;
    expect(err.message).toMatch(changed);
    expect(sent("PUT /uploads/u1/parts/1")).toBeUndefined();
  });

  it("still saves a file that does not change", async () => {
    const file = localFile("x.txt", "hello");
    serve({
      "GET /spaces": usage(1e9),
      "POST /uploads": () => json(200, { mode: "held", entry }),
    });
    await run(["drive", "put", file, "x.txt"]);
    expect(out.join("")).toContain("Saved _cli/a.txt");
  });
});

describe("drive cat of an older version", () => {
  it("is decided by the version's own is_text when the history carries it", async () => {
    serve({
      "GET /entry": () => json(200, { ...entry, is_text: false }),
      "GET /versions": () => json(200, { versions: [{ seq: 2, is_text: false }, { seq: 1, is_text: true }] }),
      "GET /content": (u) => {
        expect(u.searchParams.get("seq")).toBe("1");
        return new Response("old text");
      },
    });
    await run(["drive", "cat", "_cli/a.txt", "--rev", "1"]);
    expect(out.join("")).toBe("old text");
    serve({
      "GET /entry": () => json(200, entry),
      "GET /versions": () => json(200, { versions: [{ seq: 1, is_text: false }] }),
    });
    const err = (await run(["drive", "cat", "_cli/a.txt", "--rev", "1"]).catch((e) => e)) as CliError;
    expect(err.message).toContain("la drive get _cli/a.txt");
  });

  it("falls back to the bytes when the history does not say", async () => {
    serve({
      "GET /entry": () => json(200, { ...entry, is_text: false }),
      "GET /versions": () => json(200, { versions: [{ seq: 1 }] }),
      "GET /content": () => new Response("old text"),
    });
    await run(["drive", "cat", "_cli/a.txt", "--rev", "1"]);
    expect(out.join("")).toBe("old text");
    serve({
      "GET /entry": () => json(200, entry),
      "GET /versions": () => json(200, { versions: [{ seq: 1 }] }),
      "GET /content": () => new Response(new Uint8Array([1, 0, 2])),
    });
    const err = (await run(["drive", "cat", "_cli/a.txt", "--rev", "1"]).catch((e) => e)) as CliError;
    expect(err.message).toContain("binary file");
  });
});

describe("a download's temporary file", () => {
  const bytes = Buffer.from([4, 5, 6]);
  const route = () =>
    serve({
      "GET /content/link": () =>
        json(200, {
          url: "/api/v1/drive/personal/content?id=e1&seq=1&download=true",
          signed: false,
          size: 3,
          sha256: createHash("sha256").update(bytes).digest("hex"),
          name: "a.bin",
        }),
      "GET /content": () => new Response(bytes, { status: 206 }),
    });
  const get = (dest: string) => getFile({ api: new Api(credential), base: "/api/v1/drive/personal", path: "a.bin", force: false, quiet: true, out: dest });

  it("belongs to one download at a time, and is released when it ends", async () => {
    const dest = join(tempDir(), "out.bin");
    const lock = join(dirname(dest), ".out.bin.la-part.lock");
    writeFileSync(lock, String(process.pid));
    route();
    const err = (await get(dest).catch((e) => e)) as CliError;
    expect(err.code).toBe(Exit.conflict);
    expect(existsSync(dest)).toBe(false);
    expect(readFileSync(lock, "utf8")).toBe(String(process.pid));
    rmSync(lock);
    await get(dest);
    expect(existsSync(lock)).toBe(false);
  });

  it("is taken over from a download that was killed", async () => {
    const dest = join(tempDir(), "out.bin");
    writeFileSync(join(dirname(dest), ".out.bin.la-part.lock"), String(spawnSync(process.execPath, ["-e", ""]).pid));
    route();
    await get(dest);
    expect([...readFileSync(dest)]).toEqual([4, 5, 6]);
  });
});

describe("transfer helpers", () => {
  it("writes the whole piece when a write takes only part of it", async () => {
    const calls: [number, number, number][] = [];
    const fh = {
      write: async (_b: Uint8Array, offset: number, length: number, position: number) => {
        calls.push([offset, length, position]);
        return { bytesWritten: Math.min(length, 3) };
      },
    };
    await writeAll(fh as never, new Uint8Array(8), 100);
    expect(calls).toEqual([[0, 8, 100], [3, 5, 103], [6, 2, 106]]);
    await expect(writeAll({ write: async () => ({ bytesWritten: 0 }) } as never, new Uint8Array(1), 0)).rejects.toThrow(/no more bytes/);
  });

  it("settles every started worker before it throws the first failure", async () => {
    const events: string[] = [];
    const slow = pool([1, 2], 2, async (n) => {
      if (n === 1) throw new Error("first");
      await new Promise((r) => setTimeout(r, 30));
      events.push("second settled");
    });
    await expect(slow).rejects.toThrow("first");
    expect(events).toEqual(["second settled"]);
  });

  it("starts nothing new after a failure", async () => {
    const started: number[] = [];
    await expect(
      pool([1, 2, 3, 4], 1, async (n) => {
        started.push(n);
        if (n === 2) throw new Error("stop");
      }),
    ).rejects.toThrow("stop");
    expect(started).toEqual([1, 2]);
  });
});

describe("drive rm and restore", () => {
  it("trashes by POST, not DELETE", async () => {
    fetchMock.mockResolvedValueOnce(json(200, { trash_id: "t1" }));
    await run(["drive", "rm", "_cli/a.txt"]);
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(init.method).toBe("POST");
    expect(new URL(String(url)).pathname).toBe("/api/v1/drive/personal/trash");
    expect(JSON.parse(init.body)).toEqual({ path: "_cli/a.txt" });
    expect(out.join("")).toContain("la drive trash restore t1");
  });

  it("lists the trash by the largest page and says when more remain", async () => {
    const item = { trash_id: "t1", entry, entries: 1, deleted_at: "2026-01-02T03:04:05Z" };
    const err: string[] = [];
    vi.spyOn(process.stderr, "write").mockImplementation((t) => {
      err.push(String(t));
      return true;
    });
    fetchMock.mockResolvedValueOnce(json(200, { items: [item], more: true }));
    await run(["drive", "trash"]);
    expect(new URL(String(fetchMock.mock.calls[0]![0])).searchParams.get("limit")).toBe("1000");
    expect(out.join("")).toContain("t1");
    expect(err.join("")).toBe("The trash holds more than these 1 items; older ones are not listed.\n");

    err.length = 0;
    fetchMock.mockResolvedValueOnce(json(200, { items: [item], more: false }));
    await run(["drive", "trash"]);
    expect(err).toEqual([]);
  });

  it("restores a version by the entry's id", async () => {
    fetchMock.mockResolvedValueOnce(json(200, entry)).mockResolvedValueOnce(json(200, { ...entry, version: 3 }));
    await run(["drive", "restore", "_cli/a.txt", "--rev", "1"]);
    expect(JSON.parse(fetchMock.mock.calls[1]![1].body)).toEqual({ id: "e1", seq: 1 });
  });
});

describe("drive history and usage", () => {
  const who = { kind: "person", thread_id: null };
  const v = (seq: number, expires_at: string | null) => ({ seq, size: 2048, created_at: "2026-11-20T10:00:00Z", actor: who, note: null, restored_from: null, expires_at });

  it("shows a file's earlier copies, each with when it goes, and how to keep every version", async () => {
    serve({
      "GET /versions": () =>
        json(200, { versions: [v(4, null), v(3, "2026-12-19T10:00:00Z"), v(2, "2026-12-20T10:00:00Z")], memo: null, history_bytes: 2048 }),
    });
    await run(["drive", "history", "_cli/m.bin"]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const text = out.join("");
    expect(text).toContain("current");
    expect(text).toContain("2026-12-20 10:00");
    expect(text).toContain("2026-12-19 10:00");
    expect(text).toContain("_cli/m.bin keeps one earlier copy, for 7 days after a save. To keep every version, mark it as a memo: la drive memo mark _cli/m.bin");
  });

  it("says a file too large for history keeps none", async () => {
    serve({
      "GET /versions": () => json(200, { versions: [v(1, null)], memo: null, history_bytes: 0, too_large_for_history: true }),
    });
    await run(["drive", "history", "_cli/big.bin"]);
    expect(out.join("")).toContain("_cli/big.bin is too large to keep earlier versions: saving over it replaces it.");
  });

  it("clears by path ahead of reading clear as a file, and prints what it freed", async () => {
    expect(select(COMMANDS, ["drive", "history", "clear", "x"]).command).toBe(find("drive", "history", "clear"));
    fetchMock.mockResolvedValueOnce(json(200, { dropped: 2, freed_bytes: 3 * 1024 * 1024 }));
    await run(["drive", "history", "clear", "_cli/m.bin", "--before", "3"]);
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(new URL(String(url)).pathname).toBe("/api/v1/drive/personal/versions/clear");
    expect(JSON.parse(init.body)).toEqual({ path: "_cli/m.bin", before: 3 });
    expect(out.join("")).toContain("Dropped 2 earlier versions of _cli/m.bin, freeing 3.0 MB.");
  });

  it("says the server's refusal when a computer's token may not clear history", async () => {
    const sentence = "Only you can clear a file's history, from the app.";
    serve({ "POST /versions/clear": () => json(403, { detail: { type: "forbidden", message: sentence } }) });
    const err = (await run(["drive", "history", "clear", "_cli/a.txt"]).catch((e) => e)) as CliError;
    expect(err).toBeInstanceOf(CliError);
    expect(err.message).toBe(sentence);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("shows the trash apart from the counted space", async () => {
    const usage = { bytes: 1024 * 1024, trash_bytes: 5 * 1024 * 1024, entries: 4, max_bytes: 1024 ** 3, max_entries: 100 };
    fetchMock.mockResolvedValueOnce(json(200, { spaces: [{ id: "s1", kind: "personal", usage, trash_days: 14 }] }));
    await run(["drive", "usage"]);
    const text = out.join("");
    expect(text).toContain("1.0 MB of 1.0 GB used.");
    expect(text).toContain("The trash holds 5.0 MB, which does not count toward the space");
  });
});

describe("Args", () => {
  it("reads an optional operand that was left out as undefined", () => {
    expect(new Args(new Map(), new Map()).opt("path")).toBeUndefined();
  });
});

describe("command definitions", () => {
  it("refuse an optional operand that is not last", () => {
    const run = async () => {};
    expect(() => command({ words: ["x"], summary: "", operands: ["a?", "b"], run })).toThrow(/must be last/);
    expect(() => command({ words: ["x"], summary: "", operands: ["a", "b?"], run })).not.toThrow();
  });
});

describe("operand types", () => {
  it("keeps an optional operand from being read as always there", () => {
    command({
      words: ["x"],
      summary: "",
      operands: ["a", "b?"],
      async run({ args }) {
        const a: string = args.op("a");
        const b: string | undefined = args.opt("b");
        // @ts-expect-error an optional operand is read with opt, which may answer undefined
        args.op("b");
        // @ts-expect-error a required operand is never optional
        args.opt("a");
        void [a, b];
      },
    });
  });
});
