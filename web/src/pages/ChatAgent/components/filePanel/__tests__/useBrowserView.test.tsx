import type { ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { Computer } from '@/types/api';
import { BrowserViewSession } from '../browserStream';
import { useBrowserView } from '../useBrowserView';

const MAC_KEY = 'SHA256:helperHostKey';

vi.mock('@/lib/desktop', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/desktop')>()),
  canRemoteControl: () => true,
  getRemoteControl: vi.fn(async () => ({ supported: true, service: 'on', fingerprint: MAC_KEY })),
}));

const computer = (id: string, kind: Computer['kind'] = 'daytona', fingerprint: string | null = null) =>
  ({ computer_id: id, kind, status: 'running', ssh: fingerprint ? { fingerprint } : null }) as unknown as Computer;

function spiedView() {
  const view = new BrowserViewSession();
  const stops: ReturnType<typeof vi.fn>[] = [];
  const watch = vi.spyOn(view, 'watch').mockImplementation(() => {
    const stop = vi.fn();
    stops.push(stop);
    return stop;
  });
  return { view, watch, stops };
}

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

describe('useBrowserView', () => {
  it('watches again on the computer a moved workspace now lives on', () => {
    const { view, watch, stops } = spiedView();
    const { rerender } = renderHook(
      ({ on }: { on: Computer }) => useBrowserView(view, 'ws-1', on.computer_id, on, true),
      { initialProps: { on: computer('computer-a') }, wrapper },
    );
    expect(watch).toHaveBeenCalledTimes(1);
    rerender({ on: computer('computer-a') });
    expect(watch).toHaveBeenCalledTimes(1);
    rerender({ on: computer('computer-b') });
    expect(stops[0]).toHaveBeenCalledTimes(1);
    expect(watch).toHaveBeenCalledTimes(2);
  });

  it('follows a move for a shared workspace member, who is sent no computer row', () => {
    const { view, watch, stops } = spiedView();
    const { rerender } = renderHook(
      ({ id }: { id: string }) => useBrowserView(view, 'ws-1', id, null, true),
      { initialProps: { id: 'computer-a' }, wrapper },
    );
    rerender({ id: 'computer-b' });
    expect(stops[0]).toHaveBeenCalledTimes(1);
    expect(watch).toHaveBeenCalledTimes(2);
  });

  it('watches past the server only on the computer that is this Mac', async () => {
    const { view, watch } = spiedView();
    const { rerender } = renderHook(
      ({ on }: { on: Computer }) => useBrowserView(view, 'ws-1', on.computer_id, on, true),
      { initialProps: { on: computer('mac', 'ssh', MAC_KEY) }, wrapper },
    );
    await waitFor(() => expect(watch.mock.lastCall?.[0].local).toBe(true));
    rerender({ on: computer('other', 'ssh', 'SHA256:other') });
    expect(watch.mock.lastCall?.[0].local).toBe(false);
  });
});
