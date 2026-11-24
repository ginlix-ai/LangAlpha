import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { rawRequest, rawResponse } from "../src/rawHttp.js";

let server: http.Server | undefined;

afterEach(() => {
  server?.closeAllConnections();
  server?.close();
});

async function listen(handler: http.RequestListener): Promise<URL> {
  server = http.createServer(handler);
  await new Promise<void>((r) => server!.listen(0, "127.0.0.1", r));
  return new URL(`http://127.0.0.1:${(server.address() as AddressInfo).port}/p`);
}

describe("raw requests", () => {
  it("send the buffer as it is, with its length, and answer as a Response", async () => {
    let seen: { length: string | undefined; bytes: Buffer } | undefined;
    const url = await listen((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        seen = { length: req.headers["content-length"], bytes: Buffer.concat(chunks) };
        res.writeHead(200, { "content-type": "application/json" }).end('{"ok":true}');
      });
    });
    const res = await rawResponse(url, { method: "PUT", headers: { "content-md5": "x" }, body: Buffer.from([1, 2, 3]) });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
    expect(seen).toEqual({ length: "3", bytes: Buffer.from([1, 2, 3]) });
  });

  it("answer a status that has no body", async () => {
    const url = await listen((_req, res) => res.writeHead(204).end());
    const res = await rawResponse(url, { method: "PUT", headers: {}, body: Buffer.alloc(1) });
    expect(res.status).toBe(204);
  });

  it("give up on a connection that never completes, before the idle timer could start", async () => {
    // 10.255.255.1 is not routed: the connect hangs where the network drops it, and fails at once where it is refused.
    const started = Date.now();
    await expect(rawRequest(new URL("http://10.255.255.1:81/"), { method: "GET", headers: {}, connectMs: 150, idleMs: 60_000 })).rejects.toThrow();
    expect(Date.now() - started).toBeLessThan(3000);
  });

  it("hand a range back unread, for a download written straight to disk", async () => {
    const url = await listen((req, res) => res.writeHead(206).end(`range ${req.headers.range}`));
    const res = await rawRequest(url, { method: "GET", headers: { range: "bytes=0-4" } });
    expect(res.statusCode).toBe(206);
    let text = "";
    for await (const piece of res) text += piece;
    expect(text).toBe("range bytes=0-4");
  });

  it("give up on a store that takes the request and then says nothing", async () => {
    const url = await listen(() => {});
    const started = Date.now();
    await expect(rawResponse(url, { method: "PUT", headers: {}, body: Buffer.alloc(1), idleMs: 200 })).rejects.toThrow(
      /No bytes moved/,
    );
    expect(Date.now() - started).toBeLessThan(5000);
  });

  it("give up on a body that stops arriving", async () => {
    const url = await listen((_req, res) => {
      res.writeHead(206);
      res.write("abc");
    });
    const res = await rawResponse(url, { method: "GET", headers: {}, idleMs: 200 });
    expect(res.status).toBe(206);
    await expect(res.arrayBuffer()).rejects.toThrow();
  });
});
