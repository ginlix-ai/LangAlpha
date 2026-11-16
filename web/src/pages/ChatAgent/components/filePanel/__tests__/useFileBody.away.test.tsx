/**
 * A file its computer could not serve, with no backup to stand in, is asked
 * for again until the machine answers. A poll that fails on the way (the
 * network, the edge) says nothing about the machine, so it does not end that.
 */
import { describe, it, expect, vi, afterEach } from 'vitest';
import type { ReactNode } from 'react';
import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useFileBody, useFileBodyCache } from '../useFileBody';
import type { BodyReaders } from '../fileBody';

function httpError(status: number, headers: Record<string, string> = {}) {
  return Object.assign(new Error(`HTTP ${status}`), { response: { status, headers } });
}

const away = () => httpError(503, { 'x-computer-unreachable': 'not_connected' });

function setup(readFile: BodyReaders['readFile']) {
  const readers = { readFile } as unknown as BodyReaders;
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return renderHook(() => {
    const cache = useFileBodyCache({ scope: 'ws-1', workspaceId: 'ws-1', readers });
    return useFileBody({ cache, path: 'notes.md' });
  }, { wrapper });
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

describe('useFileBody while the computer is away', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    ['a headerless 502', () => httpError(502)],
    ['a network error', () => Object.assign(new Error('Network Error'), { code: 'ERR_NETWORK' })],
  ])('keeps asking past %s', async (_label, blip) => {
    vi.useFakeTimers();
    const readFile = vi.fn()
      .mockRejectedValueOnce(away())
      .mockRejectedValueOnce(blip())
      .mockResolvedValue({ content: 'back', mime: 'text/markdown', from: { kind: 'live' } });
    const { result } = setup(readFile);

    await tick(0);
    await tick(15_000);
    expect(readFile).toHaveBeenCalledTimes(2);
    await tick(15_000);
    expect(readFile).toHaveBeenCalledTimes(3);
    await tick(1_000);
    expect(result.current.body?.content).toBe('back');
  });

  it('stops once the machine answers, even with a refusal', async () => {
    vi.useFakeTimers();
    const readFile = vi.fn()
      .mockRejectedValueOnce(away())
      .mockRejectedValue(httpError(404));
    setup(readFile);

    await tick(0);
    await tick(15_000);
    await tick(60_000);
    expect(readFile).toHaveBeenCalledTimes(2);
  });
});
