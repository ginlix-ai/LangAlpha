import { afterEach, describe, expect, it, vi } from "vitest";
import { Args } from "../src/args.js";
import type { Ctx } from "../src/commands/command.js";
import { billsTo } from "../src/commands/models.js";
import { threads } from "../src/commands/threads.js";
import { detailReason, Exit } from "../src/errors.js";
import type { Api } from "../src/http.js";
import { printable } from "../src/output.js";
import { answerText, describeInterrupt, turn } from "../src/turn.js";

/** An api whose every call answers with these SSE frames. */
function sseApi(...frames: [string, object][]): Api {
  const body = frames.map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join("");
  return { send: vi.fn(async () => new Response(body)) } as unknown as Api;
}

function ctx(api: Api, flags: Record<string, string | true> = {}, operands: Record<string, string> = {}): Ctx {
  return { api, args: new Args(new Map(Object.entries(flags)), new Map(Object.entries(operands))), json: false };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("turn", () => {
  it("fails on the server's retry frame, and says how to retry", async () => {
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const api = sseApi(
      ["metadata", { thread_id: "t1", run_id: "r1" }],
      ["retry", { message: "Temporary error occurred", thread_id: "t1", recovery: "retry", run_id: "r1" }],
    );
    expect(await turn(ctx(api, { wait: true }), { method: "POST", path: "/x", thread: "t1" })).toBe(Exit.failed);
    const said = err.mock.calls.map(([text]) => String(text)).join("");
    expect(said).toContain("Turn failed: Temporary error occurred");
    expect(said).toContain("langalpha threads retry t1");
  });

  it("takes the run's own end as its verdict", async () => {
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const ended = (outcome: string) =>
      sseApi(["metadata", { thread_id: "t1", run_id: "r1" }], ["message_chunk", { content: "par" }], ["run_end", { outcome }]);
    const wait = (api: Api) => turn(ctx(api, { wait: true }), { method: "POST", path: "/x", thread: "t1" });
    expect(await wait(ended("error"))).toBe(Exit.failed);
    expect(await wait(ended("cancelled"))).toBe(Exit.failed);
    expect(err.mock.calls.map(([text]) => String(text)).join("")).toContain("Turn cancelled.");
    expect(await wait(ended("completed"))).toBe(Exit.ok);
  });

  it("never reads a stream that closed before the turn ended as done", async () => {
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const torn = sseApi(["metadata", { thread_id: "t1", run_id: "r1" }], ["message_chunk", { content: "par" }]);
    expect(await turn(ctx(torn, { wait: true }), { method: "POST", path: "/x" })).toBe(Exit.error);
    expect(err.mock.calls.map(([text]) => String(text)).join("")).toContain("threads follow t1");
    // Detached, the run once named is all that was asked for.
    expect(await turn(ctx(torn, { detach: true }), { method: "POST", path: "/x" })).toBe(Exit.ok);
  });

  it("strips terminal escapes from what the server says", async () => {
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const out = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const api = sseApi(
      ["message_chunk", { content: "hi\u001b]0;pwned\u0007 there\n" }],
      ["error", { message: "bad\u001b[2J", recovery: "resend" }],
    );
    expect(await turn(ctx(api), { method: "POST", path: "/x" })).toBe(Exit.failed);
    const all = [...out.mock.calls, ...err.mock.calls].map(([text]) => String(text)).join("");
    expect(all).not.toContain("\u001b");
    expect(all).toContain("hi]0;pwned there");
    expect(all).toContain("send the message again");
  });
});

describe("answering an order", () => {
  it("refuses locally, since an answer of the usual shape would turn it down", async () => {
    const order = { name: "place_order", description: "Buy 10 AAPL", attempt_id: "a1", order: {} };
    const api = sseApi(["interrupt", { interrupt_id: "i1", kind: "order_approval", action_requests: [order] }]);
    for (const verdict of ["approve", "reject"]) {
      const answer = threads.find((c) => c.words[1] === verdict)!;
      await expect(answer.run(ctx(api, {}, { id: "t1" }))).rejects.toThrow(/only the LangAlpha app/);
    }
    // Only the replay was read; nothing was answered.
    expect(vi.mocked(api.send).mock.calls.every(([method]) => method === "GET")).toBe(true);
  });

  it("points at the app", () => {
    const text = describeInterrupt("t1", {
      interrupt_id: "i",
      kind: "order_approval",
      action_requests: [{ name: "place_order", description: "Buy 10 AAPL" }],
    });
    expect(text).toContain("Buy 10 AAPL");
    expect(text).toContain("in the LangAlpha app");
    expect(text).not.toContain("threads approve");
  });
});

describe("server text", () => {
  it("reads a structured refusal's type, else its code", () => {
    expect(detailReason({ detail: { message: "m", type: "order_decision_forbidden" } })).toBe("order_decision_forbidden");
    expect(detailReason({ detail: { message: "m", code: "auth_failed" } })).toBe("auth_failed");
    expect(detailReason({ detail: "plain" })).toBeUndefined();
  });

  it("keeps newlines and tabs and drops other controls", () => {
    expect(printable("a\tb\nc\r\u001b[31md\u009b\u202e")).toBe("a\tb\nc[31md");
  });
});

describe("answerText", () => {
  it("keeps the reply and drops reasoning, which shares the event", () => {
    const chunk = (content_type: string | undefined, content = "x") => ({
      event: "message_chunk",
      data: content_type === undefined ? { content } : { content, content_type },
    });
    expect(answerText(chunk("text", "hi"))).toBe("hi");
    expect(answerText(chunk(undefined, "hi"))).toBe("hi");
    expect(answerText(chunk("reasoning"))).toBe("");
    expect(answerText(chunk("reasoning_signal", "start"))).toBe("");
    expect(answerText({ event: "tool_calls", data: { content: "x" } })).toBe("");
  });
});

describe("describeInterrupt", () => {
  it("shows a question with its options", () => {
    const text = describeInterrupt("t1", {
      interrupt_id: "i",
      action_requests: [{ type: "ask_user_question", question: "Which color?", options: ["red", "blue"] }],
    });
    expect(text).toContain("The agent asks: Which color?");
    expect(text).toContain("options: red | blue");
    expect(text).toContain('langalpha threads approve t1 --message "<answer>"');
  });

  it("lists what a tool approval would do", () => {
    const text = describeInterrupt("t1", {
      interrupt_id: "i",
      action_requests: [{ name: "Bash", args: {}, description: "Run rm -rf build" }],
    });
    expect(text).toContain("  Run rm -rf build\n");
  });
});

describe("billsTo", () => {
  const row = (fields: object) => ({ name: "m", provider: "p", custom: false, payer: null, ...fields }) as Parameters<typeof billsTo>[0];
  it("names who a turn is billed to", () => {
    expect(billsTo(row({ payer: "credits" }))).toBe("LangAlpha credits");
    expect(billsTo(row({ payer: "own_key" }))).toBe("your API key");
    expect(billsTo(row({ payer: "subscription", plan: "Claude", signed_in: true }))).toBe("your Claude subscription");
    expect(billsTo(row({ payer: "subscription", plan: "GLM Coding Plan", signed_in: true }))).toBe("your GLM Coding Plan");
    expect(billsTo(row({ payer: "subscription", plan: "ChatGPT", signed_in: false }))).toBe(
      "your ChatGPT subscription (not signed in)",
    );
  });
  it("says what a model nothing pays for needs", () => {
    expect(billsTo(row({ needs: "plan" }))).toBe("not on your plan");
    expect(billsTo(row({ needs: "key" }))).toBe("needs your API key");
  });
});
