import React, { type ReactElement, type ReactNode } from 'react';
import { render, renderHook, type RenderOptions } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';

function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
}

function createWrapper(queryClient: QueryClient, route = '/') {
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={[route]}>
          {children}
        </MemoryRouter>
      </QueryClientProvider>
    );
  };
}

interface RenderWithProvidersOptions extends Omit<RenderOptions, 'wrapper'> {
  route?: string;
  queryClient?: QueryClient;
}

export function renderWithProviders(ui: ReactElement, { route = '/', queryClient, ...options }: RenderWithProvidersOptions = {}) {
  const client = queryClient || createTestQueryClient();
  const Wrapper = createWrapper(client, route);
  return { ...render(ui, { wrapper: Wrapper, ...options }), queryClient: client };
}

export function renderHookWithProviders<TResult>(hook: () => TResult, { route = '/', queryClient, ...options }: RenderWithProvidersOptions = {}) {
  const client = queryClient || createTestQueryClient();
  const wrapper = createWrapper(client, route);
  return { ...renderHook(hook, { wrapper, ...options }), queryClient: client };
}

export { createTestQueryClient };
