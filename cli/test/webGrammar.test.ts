import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { WEB_GRAMMAR, webGrammar } from "../scripts/web-grammar.js";

describe("the web app's copy of the command table", () => {
  it("is what the CLI's commands generate", () => {
    const path = fileURLToPath(new URL(`../${WEB_GRAMMAR}`, import.meta.url));
    // On a difference, run `pnpm gen:web-grammar` and commit the file.
    expect(readFileSync(path, "utf8")).toBe(webGrammar());
  });
});
