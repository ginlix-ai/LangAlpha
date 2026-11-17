/**
 * The default a connection made in setup saves: only once the credential's
 * own list was read, fresh, in full and not fallen back to the manifest,
 * since the catalog may offer a model that credential does not serve until
 * then.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { ReactNode } from 'react';
import { render, renderHook, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';

const h = vi.hoisted(() => ({ loading: false }));
vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: { openai: { models: ['gpt-x'] } },
    metadata: { 'gpt-x': { provider: 'openai', intelligence: 5 } },
    customModels: [],
    isLoading: h.loading,
    listedProviders: ['openai'],
  }),
}));
vi.mock('@/hooks/useConfiguredProviders', () => ({ useConfiguredProviders: () => ({ configuredSet: new Set(['openai', 'my-gateway']) }) }));
vi.mock('@/hooks/useProviderCatalog', () => ({ useProviderCatalog: () => ({ connectionOf: (p: string) => p }) }));
vi.mock('@/hooks/useSlotDefaultModels', () => ({ useDefaultModel: () => null }));
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync: vi.fn(), isPending: false }) }));

import { useSaveConnectedDefault } from '../connectedDefault';

const LIST = queryKeys.providerModels.available('openai', '');

function connected(client: QueryClient) {
  const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  return renderHook(() => useSaveConnectedDefault(), { wrapper }).result.current;
}
const modelFor = (client: QueryClient) => connected(client).modelFor('openai');

describe('useSaveConnectedDefault', () => {
  beforeEach(() => { h.loading = false; });

  it('names the model once the credential\'s own list was read in full', () => {
    const client = new QueryClient();
    client.setQueryData(LIST, { provider: 'openai', status: 'ok', fetched_at: null, missing: [] });
    expect(modelFor(client)).toBe('gpt-x');
  });

  it('names none while the models load, or before the list is read', () => {
    const client = new QueryClient();
    expect(modelFor(client)).toBeNull();
    client.setQueryData(LIST, { provider: 'openai', status: 'ok', fetched_at: null, missing: [] });
    h.loading = true;
    expect(modelFor(client)).toBeNull();
  });

  it('names none from a list that fell back to the manifest, or one being read again', async () => {
    const client = new QueryClient();
    client.setQueryData(LIST, { provider: 'openai', status: 'fallback', fetched_at: null, missing: [] });
    expect(modelFor(client)).toBeNull();
    client.setQueryData(LIST, { provider: 'openai', status: 'ok', fetched_at: null, missing: [] });
    await client.invalidateQueries({ queryKey: LIST });
    expect(modelFor(client)).toBeNull();
  });

  it('names none where the list could not be read, and says so is decided', () => {
    const client = new QueryClient();
    client.getQueryCache().build(client, { queryKey: LIST }).setState({ status: 'error', error: new Error('down') });
    expect(modelFor(client)).toBeNull();
    expect(connected(client).settled('openai')).toBe(true);
  });

  it('decides once a list read again ends, even with the same answer', async () => {
    const client = new QueryClient();
    const ok = { provider: 'openai', status: 'ok', fetched_at: null, missing: [] };
    client.setQueryData(LIST, ok);
    let answer!: (v: unknown) => void;
    void client.fetchQuery({ queryKey: LIST, queryFn: () => new Promise((resolve) => { answer = resolve; }) });
    function Probe() {
      return <p data-testid="settled">{String(useSaveConnectedDefault().settled('openai'))}</p>;
    }
    render(<QueryClientProvider client={client}><Probe /></QueryClientProvider>);
    expect(screen.getByTestId('settled')).toHaveTextContent('false');
    answer(ok);
    expect(await screen.findByText('true')).toBeInTheDocument();
  });

  it('is undecided until the list is read, or the connection is known', () => {
    const client = new QueryClient();
    expect(connected(client).settled('openai')).toBe(false);
    expect(connected(client).settled('not-yet-listed')).toBe(false);
    expect(connected(client).settled('my-gateway')).toBe(true);
    expect(connected(client).modelFor('my-gateway')).toBeNull();
  });
});
