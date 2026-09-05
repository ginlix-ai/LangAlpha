/**
 * How long a builtin's tool list may be trusted.
 *
 * The backend runs several workers and each connects the builtin servers
 * itself, so one that failed to connect is dropped for the life of that
 * process while its siblings keep serving it. The endpoint says which answer
 * you got via `connected`. Freezing a `connected: false` reply turns one
 * worker's gap into "tools unavailable" for the rest of the session, since
 * reopening the panel, refocusing the window and reconnecting all read the
 * cache; a connected reply really is frozen and should never be refetched.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import React, { type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('../../pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  getBuiltinMcpServerTools: vi.fn(),
}));

import { getBuiltinMcpServerTools } from '../../pages/ChatAgent/utils/api';
import { useBuiltinMcpServerTools } from '../useMcpServers';

const fetchTools = getBuiltinMcpServerTools as ReturnType<typeof vi.fn>;

function harness() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return wrapper;
}

async function mountTwice(wrapper: ReturnType<typeof harness>) {
  const first = renderHook(() => useBuiltinMcpServerTools('price_data'), { wrapper });
  await waitFor(() => expect(first.result.current.isSuccess).toBe(true));
  first.unmount();
  const second = renderHook(() => useBuiltinMcpServerTools('price_data'), { wrapper });
  await waitFor(() => expect(second.result.current.isSuccess).toBe(true));
}

describe('useBuiltinMcpServerTools', () => {
  beforeEach(() => fetchTools.mockReset());

  it('asks again after a worker said it had no connection', async () => {
    fetchTools.mockResolvedValue({ connected: false, tools: [] });
    await mountTwice(harness());
    expect(fetchTools.mock.calls.length).toBeGreaterThan(1);
  });

  it('never asks twice once a worker answered with the tools', async () => {
    fetchTools.mockResolvedValue({
      connected: true,
      tools: [{ name: 'quote', description: '', input_schema: {} }],
    });
    await mountTwice(harness());
    expect(fetchTools).toHaveBeenCalledTimes(1);
  });
});
