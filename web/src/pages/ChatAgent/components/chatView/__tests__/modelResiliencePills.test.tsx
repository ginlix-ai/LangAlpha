/**
 * Tests the real model-resilience pill components (extracted from ChatView in
 * 5.9b; formerly pinned via local copies in ChatViewModelStatusPill.test.tsx):
 *   - ModelStatusPill: rendered only when `modelStatus && isLoading`
 *     - retrying → "<model> error — retrying ({attempt+1}/{maxRetries+1})…"
 *     - fallback → "Falling back to <toModel>…"
 *
 * The suggestion to switch once a fallback answered is a notice on the model
 * pill now (useComposerNotices.test.tsx).
 *
 * Copy is asserted through the real i18n instance (global test setup) against
 * the chat.modelRetrying / chat.modelFallingBack keys in en-US.json.
 */
import { describe, it, expect } from 'vitest';
import '@testing-library/jest-dom';
import { screen } from '@testing-library/react';
import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import React from 'react';
import { ModelStatusPill } from '../ModelStatusPill';

// The pills print a model's authored name from the cached catalog the composer
// loaded; seed it the way that load would.
function seededClient() {
  const queryClient = createTestQueryClient();
  queryClient.setQueryData(queryKeys.models.all, {
    model_metadata: {
      'model-alpha': { display_name: 'Alpha' },
      'model-beta': { display_name: 'Beta' },
    },
  });
  return queryClient;
}

const render = (ui: React.ReactElement) => renderWithProviders(ui, { queryClient: seededClient() });

describe('ModelStatusPill', () => {
  it('renders the retrying pill with 1-based counts when streaming', () => {
    render(
      <ModelStatusPill
        modelStatus={{ kind: 'retrying', model: 'model-alpha', attempt: 1, maxRetries: 3 }}
        isLoading={true}
      />,
    );
    const pill = screen.getByRole('status');
    // attempt+1 = 2, maxRetries+1 = 4
    expect(pill).toHaveTextContent('Alpha error — retrying (2/4)…');
    expect(pill).toHaveAttribute('aria-live', 'polite');
  });

  it('renders the fallback pill naming the target model when streaming', () => {
    render(
      <ModelStatusPill
        modelStatus={{ kind: 'fallback', fromModel: 'model-alpha', toModel: 'model-beta' }}
        isLoading={true}
      />,
    );
    expect(screen.getByRole('status')).toHaveTextContent('Falling back to Beta…');
  });

  it('says who pays for the model it falls back to, where the server said', () => {
    render(
      <ModelStatusPill
        modelStatus={{ kind: 'fallback', fromModel: 'model-alpha', toModel: 'model-beta', paidBy: 'byok' }}
        isLoading={true}
      />,
    );
    expect(screen.getByRole('status')).toHaveTextContent('Falling back to Beta (Your key)…');
  });

  it('renders nothing when not loading, even with a modelStatus set', () => {
    const { container } = render(
      <ModelStatusPill
        modelStatus={{ kind: 'retrying', model: 'model-alpha', attempt: 0, maxRetries: 2 }}
        isLoading={false}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing when modelStatus is null', () => {
    const { container } = render(<ModelStatusPill modelStatus={null} isLoading={true} />);
    expect(container.firstChild).toBeNull();
  });
});
