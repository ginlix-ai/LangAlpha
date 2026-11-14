import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Credential } from "../src/credentials.js";
import { CliError, Exit } from "../src/errors.js";
import { Api } from "../src/http.js";

const credential: Credential = {
  baseUrl: "http://server.test",
  token: async () => "tok",
  renew: async () => false,
};

const NOW = Date.parse("2026-11-14T12:00:00Z");
const later = (ms: number) => new Date(NOW + ms).toISOString();

const reply = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
const pending = (id: string, expires = later(30 * 60_000), headers: Record<string, string> = {}) =>
  reply(403, { detail: { type: "approval_pending", approval_id: id, expires_at: expires, message: "waiting" } }, headers);

let time: number;
let sleeps: number[];
let stderr: string[];
let fetchMock: ReturnType<typeof vi.fn>;

function api(cred: Credential = credential): Api {
  return new Api(cred, {
    now: () => time,
    sleep: async (ms) => {
      sleeps.push(ms);
    },
  });
}

async function failure(p: Promise<unknown>): Promise<CliError> {
  try {
    await p;
  } catch (e) {
    return e as CliError;
  }
  throw new Error("expected a failure");
}

const approvalHeaders = () =>
  fetchMock.mock.calls.map(([, init]) => (init.headers as Record<string, string>)["x-langalpha-approval"]);

beforeEach(() => {
  time = NOW;
  sleeps = [];
  stderr = [];
  fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
  vi.spyOn(process.stderr, "write").mockImplementation((chunk) => {
    stderr.push(String(chunk));
    return true;
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("approval wait", () => {
  it("sends the identical request again, naming the approval, and returns the success", async () => {
    fetchMock
      .mockResolvedValueOnce(pending("a1"))
      .mockResolvedValueOnce(pending("a1"))
      .mockResolvedValueOnce(reply(200, { ok: true }));
    const out = await api().json("DELETE", "/api/v1/threads/t1", { query: { force: true }, body: { x: 1 } });
    expect(out).toEqual({ ok: true });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    for (const [url, init] of fetchMock.mock.calls) {
      expect(String(url)).toBe("http://server.test/api/v1/threads/t1?force=true");
      expect(init.method).toBe("DELETE");
      expect(init.body).toBe(JSON.stringify({ x: 1 }));
    }
    expect(approvalHeaders()).toEqual([undefined, "a1", "a1"]);
    const notices = stderr.filter((s) => s.startsWith("Waiting for your approval"));
    expect(notices).toEqual(["Waiting for your approval in the LangAlpha app (expires in 30 minutes)…\n"]);
  });

  it("pauses between instant pending answers but not after a held one", async () => {
    fetchMock.mockImplementation(async () => {
      if (fetchMock.mock.calls.length === 1) return pending("a1");
      time += 50_000;
      return fetchMock.mock.calls.length === 2 ? pending("a1") : reply(200, {});
    });
    await api().send("DELETE", "/x");
    expect(sleeps).toEqual([1000]);
  });

  it("adopts the approval each pending answer names", async () => {
    fetchMock
      .mockResolvedValueOnce(pending("a1"))
      .mockResolvedValueOnce(pending("a2"))
      .mockResolvedValueOnce(reply(200, {}));
    await api().send("DELETE", "/x");
    expect(approvalHeaders()).toEqual([undefined, "a1", "a2"]);
    expect(stderr.filter((s) => s.startsWith("Waiting"))).toHaveLength(1);
  });

  it("keeps waiting past the expiry the machine's clock reads; only the server ends it", async () => {
    fetchMock.mockImplementation(async () => {
      time += 40 * 60_000;
      return fetchMock.mock.calls.length < 3 ? pending("a1", later(-1000)) : reply(200, {});
    });
    await api().send("DELETE", "/x");
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("measures the minutes left from the reply's own date", async () => {
    time = NOW + 2 * 60 * 60_000;
    const date = new Date(NOW).toUTCString();
    fetchMock.mockResolvedValueOnce(pending("a1", later(10 * 60_000), { date })).mockResolvedValueOnce(reply(200, {}));
    await api().send("DELETE", "/x");
    expect(stderr).toContain("Waiting for your approval in the LangAlpha app (expires in 10 minutes)…\n");
  });

  it("stops with an error, nothing done, once the server says the request no longer waits", async () => {
    const message = "This request is no longer waiting in the app; nothing was done. Run the command again to ask again.";
    fetchMock
      .mockResolvedValueOnce(pending("a1"))
      .mockResolvedValueOnce(reply(403, { detail: { type: "approval_expired", message } }));
    const err = await failure(api().send("DELETE", "/x"));
    expect(err.message).toBe(message);
    expect(err.code).toBe(Exit.error);
    expect(err.reason).toBe("approval_expired");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("turns a rejection into an auth error with the server's message", async () => {
    const message = "The user declined this in the app; nothing was done";
    fetchMock
      .mockResolvedValueOnce(pending("a1"))
      .mockResolvedValueOnce(reply(403, { detail: { type: "approval_rejected", message } }));
    const err = await failure(api().send("DELETE", "/x"));
    expect(err.message).toBe(message);
    expect(err.code).toBe(Exit.auth);
    expect(err.status).toBe(403);
    expect(err.reason).toBe("approval_rejected");
  });

  it("answers approval_limit as an error with the server's message", async () => {
    fetchMock.mockResolvedValueOnce(reply(429, { detail: { type: "approval_limit", message: "Too many waiting" } }));
    const err = await failure(api().send("DELETE", "/x"));
    expect(err.message).toBe("Too many waiting");
    expect(err.code).toBe(Exit.error);
    expect(err.reason).toBe("approval_limit");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("answers approval_unavailable as an error with the server's message, sending nothing again", async () => {
    const message = "Can't ask for approval right now; nothing was done. Try again shortly.";
    fetchMock.mockResolvedValueOnce(reply(503, { detail: { type: "approval_unavailable", message } }));
    const err = await failure(api().send("DELETE", "/x"));
    expect(err.message).toBe(message);
    expect(err.code).toBe(Exit.error);
    expect(err.reason).toBe("approval_unavailable");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("renews a token that lapses mid-wait and keeps waiting on the same approval", async () => {
    let token = "old";
    const renewing: Credential = {
      baseUrl: "http://server.test",
      token: async () => token,
      renew: async () => {
        token = "new";
        return true;
      },
    };
    fetchMock
      .mockResolvedValueOnce(pending("a1"))
      .mockResolvedValueOnce(reply(401, { detail: "expired token" }))
      .mockResolvedValueOnce(pending("a1"))
      .mockResolvedValueOnce(reply(200, {}));
    await api(renewing).send("DELETE", "/x");
    expect(approvalHeaders()).toEqual([undefined, "a1", "a1", "a1"]);
    const auth = fetchMock.mock.calls.map(([, init]) => (init.headers as Record<string, string>).authorization);
    expect(auth).toEqual(["Bearer old", "Bearer old", "Bearer new", "Bearer new"]);
  });

  it("lets an unreachable re-send propagate", async () => {
    fetchMock.mockResolvedValueOnce(pending("a1")).mockRejectedValueOnce(new TypeError("fetch failed"));
    const err = await failure(api().send("DELETE", "/x"));
    expect(err.reason).toBe("unreachable");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("leaves an ordinary 403 as it was", async () => {
    fetchMock.mockResolvedValueOnce(reply(403, { detail: "Forbidden for this token" }));
    const err = await failure(api().send("DELETE", "/x"));
    expect(err.message).toBe("Forbidden for this token");
    expect(err.code).toBe(Exit.auth);
    expect(stderr).toEqual([]);
  });
});
