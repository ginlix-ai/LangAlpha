import { afterEach, describe, expect, it, vi } from "vitest";
import { Args } from "../src/args.js";
import { CliError, Exit } from "../src/errors.js";
import type { Ctx } from "../src/commands/command.js";
import { skills } from "../src/commands/skills.js";
import type { Api } from "../src/http.js";

const save = skills.find((c) => c.words.join(" ") === "skills save")!;

function api(answer: unknown) {
  const json = vi.fn(async () => answer);
  return { api: { json } as unknown as Api, json };
}

function ctx(a: Api, flags: [string, string][] = []): Ctx {
  return { api: a, args: new Args(new Map(flags), new Map([["name", "earnings-recap"]])), json: false };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("skills save", () => {
  it("saves the folder of the workspace the command runs in when none is named", async () => {
    const out = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const { api: a, json } = api({ name: "earnings-recap", saved: true, changed: true, reason: null });

    await save.run(ctx(a));

    expect(json).toHaveBeenCalledWith("POST", "/api/v1/skills/earnings-recap/save", { query: { workspace_id: undefined } });
    expect(String(out.mock.calls[0]![0])).toBe("Saved earnings-recap\n");
  });

  it("says so when the folder was already saved as it stands", async () => {
    const out = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const { api: a } = api({ name: "earnings-recap", saved: true, changed: false, reason: null });

    await save.run(ctx(a));

    expect(String(out.mock.calls[0]![0])).toBe("earnings-recap is already saved as it stands\n");
  });

  it("fails with the server's reason when the folder is refused", async () => {
    const { api: a } = api({ name: "earnings-recap", saved: false, changed: false, reason: "frontmatter must declare a description" });

    const refused = await save.run(ctx(a)).catch((e: unknown) => e);

    expect(refused).toBeInstanceOf(CliError);
    expect((refused as CliError).message).toBe("Not saved: frontmatter must declare a description");
    expect((refused as CliError).code).toBe(Exit.failed);
  });

  it("saves into the workspace it is given by id", async () => {
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const id = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21";
    const { api: a, json } = api({ name: "earnings-recap", saved: true, changed: true, reason: null });

    await save.run(ctx(a, [["workspace", id]]));

    expect(json).toHaveBeenCalledWith("POST", "/api/v1/skills/earnings-recap/save", { query: { workspace_id: id } });
  });
});
