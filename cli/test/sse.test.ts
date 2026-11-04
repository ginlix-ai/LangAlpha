import { describe, expect, it } from "vitest";
import { frames } from "../src/sse.js";

function stream(...chunks: string[]): ReadableStream<Uint8Array> {
  const enc = new TextEncoder();
  return new ReadableStream({
    start(c) {
      for (const chunk of chunks) c.enqueue(enc.encode(chunk));
      c.close();
    },
  });
}

async function all(body: ReadableStream<Uint8Array>) {
  const out = [];
  for await (const f of frames(body)) out.push(f);
  return out;
}

describe("frames", () => {
  it("reads frames split across chunks", async () => {
    const got = await all(stream('id: 3\nevent: message_chunk\ndata: {"con', 'tent":"hi"}\n\n', "event: finish\ndata: {}\n\n"));
    expect(got).toEqual([
      { event: "message_chunk", data: { content: "hi" } },
      { event: "finish", data: {} },
    ]);
  });

  it("skips comments and frames that are not JSON objects", async () => {
    const got = await all(stream(": ping\n\nevent: x\ndata: [1]\n\nevent: y\r\ndata: {\"a\":1}\r\n\r\n"));
    expect(got).toEqual([{ event: "y", data: { a: 1 } }]);
  });
});
