import { describe, expect, it } from "vitest";
import { Exit, unreachable } from "../src/errors.js";

function refused(cause: Error): Error {
  return new TypeError("fetch failed", { cause });
}

describe("unreachable", () => {
  it("names the code of a message-less AggregateError instead of printing empty parentheses", () => {
    const cause = Object.assign(new AggregateError([], ""), { code: "ECONNREFUSED" });
    const err = unreachable("http://localhost:8000", refused(cause));
    expect(err.message).toBe("Cannot reach http://localhost:8000 (ECONNREFUSED)");
    expect(err.code).toBe(Exit.error);
  });

  it("falls back to the inner errors, each reason once", () => {
    const inner = (address: string) => Object.assign(new Error(`connect ECONNREFUSED ${address}`), { code: "ECONNREFUSED" });
    const cause = new AggregateError([inner("::1:8000"), inner("::1:8000"), inner("127.0.0.1:8000")], "");
    expect(unreachable("http://localhost:8000", refused(cause)).message).toBe(
      "Cannot reach http://localhost:8000 (connect ECONNREFUSED ::1:8000; connect ECONNREFUSED 127.0.0.1:8000)",
    );
  });

  it("keeps a plain cause's message, and never leaves the parentheses empty", () => {
    expect(unreachable("http://x", refused(new Error("getaddrinfo ENOTFOUND x"))).message).toBe(
      "Cannot reach http://x (getaddrinfo ENOTFOUND x)",
    );
    expect(unreachable("http://x", refused(new AggregateError([], ""))).message).toBe(
      "Cannot reach http://x (TypeError: fetch failed)",
    );
    expect(unreachable("http://x", new Error("boom")).message).toBe("Cannot reach http://x (Error: boom)");
  });
});
