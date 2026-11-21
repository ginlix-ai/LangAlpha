/**
 * The setup's search provider is the user's own preference, the one the Agent
 * section sets: it shows what is stored once preferences arrive, and a pick
 * clears a depth the new provider does not declare.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { SetupStatus } from '@/api/setup';

const h = vi.hoisted(() => ({
  preferences: null as null | object,
  mutateAsync: vi.fn(async (_patch: unknown) => ({})),
}));
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: h.preferences === null }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync: h.mutateAsync }) }));
vi.mock('@/hooks/useModels', () => ({
  useModels: () => ({
    models: {
      search_providers: {
        tavily: {
          display_name: 'Tavily',
          min_tier: 0,
          depths: [
            { name: 'fast', display_name: 'Fast', min_tier: 0 },
            { name: 'deep', display_name: 'Deep', min_tier: 0 },
          ],
        },
        exa: {
          display_name: 'Exa',
          min_tier: 0,
          depths: [
            { name: 'fast', display_name: 'Fast', min_tier: 0 },
            { name: 'deep', display_name: 'Deep', min_tier: 0 },
          ],
        },
      },
    },
  }),
}));

import { WebSearchKeys } from '../ServiceKeys';
import type { ServiceKeysDraft } from '../../hooks/useSetupDrafts';

const status = {
  keys: [
    { name: 'TAVILY_API_KEY', group: 'search', provider: 'tavily', state: 'saved', masked: 'tvly...a1b2' },
    { name: 'EXA_API_KEY', group: 'search', provider: 'exa', state: 'unset' },
  ],
} as unknown as SetupStatus;

const draft = { drafts: {}, setDraft: vi.fn() } as unknown as ServiceKeysDraft;

beforeEach(() => {
  h.preferences = { other_preference: { search_provider: 'tavily', search_depth: 'deep' } };
  h.mutateAsync.mockClear();
});

describe("the setup's web search provider", () => {
  it('clears the old provider\'s depth on a switch', () => {
    renderWithProviders(<WebSearchKeys status={status} draft={draft} />);
    fireEvent.change(screen.getByLabelText('Provider'), { target: { value: 'exa' } });
    expect(h.mutateAsync).toHaveBeenCalledWith({ other_preference: { search_provider: 'exa', search_depth: null } });
  });

  it('shows the stored provider once preferences arrive after the form', () => {
    h.preferences = null;
    const { rerender } = renderWithProviders(<WebSearchKeys status={status} draft={draft} />);
    expect(screen.getByLabelText('Provider')).toHaveValue('');
    h.preferences = { other_preference: { search_provider: 'tavily' } };
    rerender(<WebSearchKeys status={status} draft={draft} />);
    expect(screen.getByLabelText('Provider')).toHaveValue('tavily');
    expect(screen.getByText('tvly...a1b2')).toBeInTheDocument();
  });

  it("names each provider as the catalog does, and says which hold a key", () => {
    renderWithProviders(<WebSearchKeys status={status} draft={draft} />);
    expect(screen.getByRole('option', { name: 'Tavily (key added)' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'Exa' })).toBeInTheDocument();
  });
});
