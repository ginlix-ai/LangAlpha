/**
 * Once the portal has listed its pages, Settings' search finds them in place
 * of the Plan and Integrations sections they replace.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/components/AccountPortal/PortalHost', () => ({
  usePortalHost: () => ({
    nav: [
      { label: null, items: [{ path: '/billing', label: 'Plan & billing', icon: 'credit-card' }] },
      { label: 'Acme Capital', items: [{ path: '/organization/members', label: 'Members', icon: 'users' }] },
    ],
  }),
}));

const { SettingsRail } = await import('../components/SettingsRail');

function search(query: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(queryKeys.features.list(), []);
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <SettingsRail view={{ kind: 'section', section: 'general' }} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  fireEvent.change(screen.getByRole('combobox'), { target: { value: query } });
  return within(screen.getByRole('listbox'));
}

describe('searching Settings with the portal listed', () => {
  it('finds a portal page of the account', () => {
    const results = search('billing');
    expect(results.getByText('Plan & billing')).toBeTruthy();
    expect(results.getByText('Account')).toBeTruthy();
  });

  it('finds an organization page under its name', () => {
    expect(search('acme').getByText('Members')).toBeTruthy();
  });

  it('leaves out the sections the portal pages replace', () => {
    expect(search('chat apps').queryByText('Integrations')).toBeNull();
  });
});
