/**
 * A re-read of a ranged PDF answers with a new loader every time. The cache
 * keeps the body it has while the version is the same, since a new body
 * reloads the document on screen, and takes the new one once the version
 * moves or a read found the file changed.
 */
import { describe, it, expect, vi } from 'vitest';
import type { ReactNode } from 'react';
import { renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { markStale, type RangedFile } from '@/lib/rangedFile';
import { useFileBodyCache } from '../useFileBody';
import type { BodyReaders, FileBody } from '../fileBody';

function ranged(etag: string, length = 6_000_000): RangedFile {
  return { length, etag, initial: new Uint8Array(8), read: vi.fn() };
}

function setup(versions: RangedFile[]) {
  const openRanged = vi.fn(async () => versions.shift()!);
  const readers = { openRanged } as unknown as BodyReaders;
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  const { result } = renderHook(() => useFileBodyCache({ scope: 'ws-1', workspaceId: 'ws-1', readers }), { wrapper });
  // What a viewer reads: the cached body, after the cache's sharing, which
  // the resolved fetch itself does not go through.
  const cached = () => client.getQueryData<FileBody>(result.current.keyFor('big.pdf'))!;
  const reread = async () => {
    result.current.invalidate('big.pdf');
    await result.current.fetchBody('big.pdf');
    return cached();
  };
  return { cache: result, cached, reread, openRanged };
}

describe('useFileBodyCache with a ranged PDF', () => {
  it('keeps the body a re-read finds at the same version', async () => {
    const { cache, cached, reread, openRanged } = setup([ranged('"v1"'), ranged('"v1"')]);
    await cache.current.fetchBody('big.pdf');
    const first = cached();
    expect(await reread()).toBe(first);
    expect(openRanged).toHaveBeenCalledTimes(2);
  });

  it('takes the new body once the version moves', async () => {
    const { cache, reread } = setup([ranged('"v1"'), ranged('"v2"')]);
    const first = await cache.current.fetchBody('big.pdf');
    const second = await reread();
    expect(second).not.toBe(first);
    expect(second.ranged?.etag).toBe('"v2"');
  });

  it('takes the new body when the length moved under the same tag', async () => {
    const { cache, reread } = setup([ranged('"v1"'), ranged('"v1"', 7_000_000)]);
    const first = await cache.current.fetchBody('big.pdf');
    expect(await reread()).not.toBe(first);
  });

  it('replaces a file a read found changed, even when the re-read finds the same version', async () => {
    const versions = [ranged('"v1"'), ranged('"v1"'), ranged('"v1"')];
    const [, second, third] = versions;
    const { cache, cached, reread } = setup([...versions]);
    await cache.current.fetchBody('big.pdf');
    markStale(cached().ranged!);
    expect((await reread()).ranged).toBe(second);
    // The cache holds the reader itself, not a copy, so the object its own
    // reads mark is the one the next re-read finds stale.
    markStale(second);
    expect((await reread()).ranged).toBe(third);
  });
});
