/**
 * A socket that died with no close leaves `reader.read()` pending, so these
 * read from a body that never ends: only the silence watch can let them go.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/api/client', () => ({ api: { defaults: { baseURL: '' } } }));
vi.mock('@/lib/authToken', () => ({ getAuthHeaders: async () => ({}), bearerTokenOf: () => null, refreshAccessToken: async () => null }));

import { PING_STREAM_BEAT_MS } from '@/lib/streamSilence';
import { openThreadMuxStream } from '../messages';
import { streamFetch } from '../transport';

const BEAT_MS = 15_000;
const SILENCE_MS = BEAT_MS * 3;
const encoder = new TextEncoder();

/** A response whose body stays open, with a way to send it more. */
function openResponse() {
  type Read = { done: boolean; value?: Uint8Array };
  const queued: Read[] = [];
  let waiting: ((read: Read) => void) | null = null;
  const deliver = (read: Read) => {
    if (waiting) {
      waiting(read);
      waiting = null;
    } else {
      queued.push(read);
    }
  };
  const reader = {
    read: () => new Promise<Read>((resolve) => {
      const next = queued.shift();
      if (next) resolve(next);
      else waiting = resolve;
    }),
    // As the spec has it: a cancel ends the read that is pending.
    cancel: async () => deliver({ done: true }),
  };
  const response = {
    ok: true,
    status: 200,
    headers: { get: () => null },
    body: { getReader: () => reader },
  } as unknown as Response;
  return { response, send: (text: string) => deliver({ done: false, value: encoder.encode(text) }) };
}

const settle = () => vi.advanceTimersByTimeAsync(0);

describe('a stream gone silent', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('comes back as a drop the retry loop takes, with what arrived before it delivered', async () => {
    const { response, send } = openResponse();
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(response);
    const events: unknown[] = [];

    const stream = streamFetch('/s', {}, (event) => events.push(event), undefined, { beatMs: BEAT_MS });
    await settle();
    send('data: {"n":1}\n\n');
    await settle();

    await vi.advanceTimersByTimeAsync(SILENCE_MS);
    await expect(stream).resolves.toMatchObject({ disconnected: true, aborted: false, silentOnReturn: false });
    expect(events).toEqual([{ n: 1 }]);
  });

  it('counts a keepalive as the stream being alive', async () => {
    const { response, send } = openResponse();
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(response);
    const settled = vi.fn();

    void streamFetch('/s', {}, () => undefined, undefined, { beatMs: BEAT_MS }).then(settled);
    await settle();
    for (let beat = 0; beat < 10; beat += 1) {
      await vi.advanceTimersByTimeAsync(15_000);
      send(':keepalive\n\n');
      await settle();
    }
    expect(settled).not.toHaveBeenCalled();
  });

  // The server beats from the headers on, so a setup that goes quiet is a dead
  // socket like any other.
  it('watches from the headers, through frames of the setup', async () => {
    const { response, send } = openResponse();
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(response);

    const stream = streamFetch('/s', {}, () => undefined, undefined, { beatMs: BEAT_MS });
    await settle();
    send('id: 0\nevent: workspace_status\ndata: {"status":"starting"}\n\n');
    await settle();

    await vi.advanceTimersByTimeAsync(SILENCE_MS);
    await expect(stream).resolves.toMatchObject({ disconnected: true, silentOnReturn: false });
  });

  it('is not looked for on a stream that asked for no watch', async () => {
    const { response } = openResponse();
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(response);
    const settled = vi.fn();

    void streamFetch('/replay', {}, () => undefined).then(settled);
    await vi.advanceTimersByTimeAsync(SILENCE_MS * 10);
    expect(settled).not.toHaveBeenCalled();
  });

  it('fails the mux read so its loop reconnects from the cursors it holds', async () => {
    const { response, send } = openResponse();
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(response);
    const lines: string[] = [];

    const stream = openThreadMuxStream('th-1', null, (line) => lines.push(line), new AbortController().signal);
    const failed = expect(stream).rejects.toThrow('went silent');
    await settle();
    send('id: run:r#1-0\n');
    await settle();

    await vi.advanceTimersByTimeAsync(PING_STREAM_BEAT_MS * 3);
    await failed;
    expect(lines).toEqual(['id: run:r#1-0']);
  });

  it('ends the mux read as a close when the page comes back to find it quiet', async () => {
    const { response } = openResponse();
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(response);
    const hidden = vi.spyOn(document, 'hidden', 'get').mockReturnValue(true);

    const stream = openThreadMuxStream('th-1', null, () => undefined, new AbortController().signal);
    await settle();
    document.dispatchEvent(new Event('visibilitychange'));
    await vi.advanceTimersByTimeAsync(PING_STREAM_BEAT_MS);
    hidden.mockReturnValue(false);
    document.dispatchEvent(new Event('visibilitychange'));
    await vi.advanceTimersByTimeAsync(3_000);

    await expect(stream).resolves.toBeUndefined();
    hidden.mockRestore();
  });
});
