/**
 * Tests the real model-resilience pill components (extracted from ChatView in
 * 5.9b; formerly pinned via local copies in ChatViewModelStatusPill.test.tsx):
 *   - ModelStatusPill: rendered only when `modelStatus && isLoading`
 *     - retrying → "<model> error — retrying ({attempt+1}/{maxRetries+1})…"
 *     - fallback → "Falling back to <toModel>…"
 *   - FallbackSuggestionPill: rendered only when `fallbackSuggestion &&
 *     !isLoading && toModel !== composerModel`, with a switch action and a
 *     dismiss. `composerModel` is the model the composer shows, which is what
 *     the next send names.
 *
 * Copy is asserted through the real i18n instance (global test setup) against
 * the chat.modelRetrying / chat.modelFallingBack / chat.modelTroubleSuggestion
 * / chat.switchToModel keys in en-US.json.
 */
import { describe, it, expect, vi } from 'vitest';
import '@testing-library/jest-dom';
import { screen, fireEvent } from '@testing-library/react';
import { createTestQueryClient, renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import React from 'react';
import type { FallbackSuggestion } from '../../../session/types';
import { ModelStatusPill } from '../ModelStatusPill';
import { FallbackSuggestionPill } from '../FallbackSuggestionPill';

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

describe('FallbackSuggestionPill', () => {
  const suggestion: FallbackSuggestion = { fromModel: 'model-alpha', toModel: 'model-beta' };
  const noop = () => {};

  const renderPill = (overrides: Partial<React.ComponentProps<typeof FallbackSuggestionPill>> = {}) =>
    render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        composerModel="model-alpha"
        onSwitchModel={noop}
        onDismiss={noop}
        {...overrides}
      />,
    );

  it('names the troubled model and the working model, and switches to the working one', () => {
    const onSwitchModel = vi.fn();
    renderPill({ onSwitchModel });
    const pill = screen.getByRole('status');
    expect(pill).toHaveTextContent('Alpha is having trouble — this answer came from Beta');

    fireEvent.click(screen.getByRole('button', { name: 'Switch to Beta' }));
    expect(onSwitchModel).toHaveBeenCalledWith('model-beta');
  });

  // Two models can share a name, one on a plan and one on credits, so the
  // switch says which payer the thread moves onto.
  it('says who pays for the model it would switch to', () => {
    const onSwitchModel = vi.fn();
    renderPill({ onSwitchModel, fallbackSuggestion: { ...suggestion, paidBy: 'byok' } });
    fireEvent.click(screen.getByRole('button', { name: 'Switch to Beta (Your key)' }));
    expect(onSwitchModel).toHaveBeenCalledWith('model-beta');
  });

  it('dismisses via the close button', () => {
    const onDismiss = vi.fn();
    renderPill({ onDismiss });
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onDismiss).toHaveBeenCalled();
  });

  it('renders nothing while a turn is streaming', () => {
    const { container } = renderPill({ isLoading: true });
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing once the composer already shows the working model', () => {
    const { container } = renderPill({ composerModel: 'model-beta' });
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing without a suggestion', () => {
    const { container } = renderPill({ fallbackSuggestion: null });
    expect(container.firstChild).toBeNull();
  });
});
