/**
 * A model switch's notice is named when shown, from the catalog the composer
 * loads. A thread opened cold replays the switch before that catalog arrives,
 * so the notice has to pick up the names and the plan once it does.
 */
import { describe, it, expect } from 'vitest';
import { act, screen } from '@testing-library/react';
import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { NotificationDivider } from '../NotificationDivider';

const notice = { from: 'model-alpha', to: 'model-beta-oauth', paidBy: 'oauth' };

describe('NotificationDivider, model switch', () => {
  it('names the models and the plan once the catalog arrives after the event', async () => {
    const queryClient = createTestQueryClient();
    renderWithProviders(<NotificationDivider fallback={notice} />, { queryClient });
    // Before the catalog, the generic word stands in for the plan.
    expect(screen.getByText(/\(Subscription\)$/)).toBeInTheDocument();

    act(() => {
      queryClient.setQueryData(queryKeys.models.all, {
        model_metadata: {
          'model-alpha': { display_name: 'Alpha' },
          'model-beta-oauth': { display_name: 'Beta', provider: 'claude-oauth', access_type: 'oauth' },
        },
      });
    });
    expect(await screen.findByText('Alpha failed — switched to Beta (Claude plan)')).toBeInTheDocument();
  });

  // A model the user added on a sign-in may share its name with another
  // provider's model; the plan is the one the call went through.
  it("names the plan of the provider the call went through, not the catalog's", () => {
    const queryClient = createTestQueryClient();
    queryClient.setQueryData(queryKeys.models.all, {
      model_metadata: { 'model-alpha': { display_name: 'Alpha' }, 'model-gamma': { display_name: 'Gamma', provider: 'openai' } },
    });
    renderWithProviders(
      <NotificationDivider fallback={{ from: 'model-alpha', to: 'model-gamma', paidBy: 'oauth', provider: 'codex-oauth' }} />,
      { queryClient },
    );
    expect(screen.getByText('Alpha failed — switched to Gamma (ChatGPT plan)')).toBeInTheDocument();
  });

  // The server stamps a keyless local call as the platform's; nobody pays.
  it('leaves the payer out for a call that went through a local server', () => {
    const queryClient = createTestQueryClient();
    queryClient.setQueryData(queryKeys.models.all, {
      model_metadata: { 'model-alpha': { display_name: 'Alpha' } },
      provider_catalog: [{ provider: 'ollama', access_type: 'local' }],
    });
    renderWithProviders(
      <NotificationDivider fallback={{ from: 'model-alpha', to: 'my-llama', paidBy: 'platform', provider: 'ollama' }} />,
      { queryClient },
    );
    expect(screen.getByText(/^Alpha failed — switched to .+$/).textContent).not.toMatch(/\(/);
  });

  it('leaves the payer out when the server did not say', () => {
    const queryClient = createTestQueryClient();
    queryClient.setQueryData(queryKeys.models.all, {
      model_metadata: { 'model-alpha': { display_name: 'Alpha' }, 'model-beta': { display_name: 'Beta' } },
    });
    renderWithProviders(<NotificationDivider fallback={{ from: 'model-alpha', to: 'model-beta' }} />, { queryClient });
    expect(screen.getByText('Alpha failed — switched to Beta')).toBeInTheDocument();
  });
});
